"""
Statistics monitor of a running scheduler, readable over Pyro.

The scheduler updates the statistics in-process (its job threads call :meth:`SchedulerMonitor.enqueued`
and the other update methods). Over Pyro the monitor is registered in the nameserver as
``mupif.scheduler`` with metadata ``type:scheduler``; it exposes only :meth:`~SchedulerMonitor.getStatistics`
(read by the monitoring pages, e.g. ``mupif.monitor.schedulerInfo``), and :meth:`~SchedulerMonitor.stop`
and :meth:`~SchedulerMonitor.setDrain` (callers on the scheduler's host only).

Counters (see :class:`~mupifDB.scheduler.stats.SchedulerStat`): ``scheduled`` counts executions taken
by the scheduler and not started yet, ``running`` those whose workflow process runs, ``processed`` =
``finished`` + ``failed`` those that reached a final status; an execution returned to ``Pending`` is
not processed. Totals and history survive restarts (statistics file), the current counters do not.
"""

import datetime
import functools
import ipaddress
import logging
import socket
import threading
from pathlib import Path
from typing import Callable

import Pyro5.api
import Pyro5.errors

from .stats import SchedulerStat

log = logging.getLogger('scheduler')

#: name of the monitor in the Pyro nameserver
NS_NAME = 'mupif.scheduler'


def localCallerOnly(fn):
    """
    Restrict a Pyro method to callers on this host (loopback or one of the host's own addresses).

    :raises PermissionError: (in the decorated method) when called over Pyro from another host
    """
    @functools.wraps(fn)
    def inner(*args, **kwargs):
        import Pyro5.callcontext
        ctx = Pyro5.callcontext.current_context
        if ctx.client is not None:
            import psutil
            addr = ipaddress.ip_address(ctx.client_sock_addr[0])  # type: ignore
            if not addr.is_loopback:
                own = {ipaddress.ip_address(rec.address.split('%')[0]) for recs in psutil.net_if_addrs().values() for rec in recs
                       if rec.family in (socket.AF_INET, socket.AF_INET6)}
                if addr not in own:
                    raise PermissionError(f'Must only be called locally (not from {addr})')
        return fn(*args, **kwargs)
    return inner


class SchedulerMonitor:
    """
    Holds the statistics of the scheduler; thread-safe.

    Lifecycle: created at startup (reads the statistics file), served over Pyro by :meth:`runServer`,
    updated by the scheduler while it runs, persisted by :meth:`persist`.
    """

    def __init__(self, statFile: str | None, capacity: int, onStop: Callable[[], None] = lambda: None):
        """
        Load the statistics of the previous run, if any.

        :param statFile: file keeping the statistics across restarts (None: do not persist)
        :param capacity: number of executions that can run at once (for the load percentage)
        :param onStop: called by :meth:`stop` to stop the scheduler
        """
        self.statFile = statFile
        self.onStop = onStop
        self.lock = threading.RLock()
        #: serializes the copies to the database (see :meth:`syncDb`)
        self._syncLock = threading.Lock()
        #: number of changes of the statistics so far, and the number already copied to the database
        self._version = 0
        self._syncedVersion = -1
        self.stat = SchedulerStat()
        SchedulerStat.capacity = capacity
        if statFile and Path(statFile).is_file():
            try:
                self.stat = SchedulerStat.load_from_file(statFile)
            except Exception:
                log.exception(f'Failure reading persistent schedule statistics from {statFile}, starting from scratch.')
        # the totals and the history carry over from the previous run, but nothing is queued or
        # running in this new process yet; the database queue is sampled again by the first
        # scheduler iteration (sample())
        self.stat.tasks.running = 0
        self.stat.tasks.scheduled = 0
        self.stat.tasks.pending = 0
        self.stat.tasks.waiting = 0
        self.stat.tasks.dbRunning = None
        # size the history lists (empty in a new SchedulerStat) before anything is stored in them
        self.stat.advanceTime()
        #: nameserver proxy and URI of the monitor, once registered by :meth:`runServer`
        self.ns = None
        self.uri: str | None = None
        #: drain mode of the scheduler (reported in the statistics)
        self.draining = False
        #: called by :meth:`setDrain` with the new state (set by the scheduler)
        self.onDrain: Callable[[bool], None] = lambda on: None

    # --- updates by the scheduler (in-process, not exposed over Pyro) -------------------------

    def enqueued(self, weid: str) -> None:
        """Count an execution taken by the scheduler (not started yet)."""
        with self.lock:
            self.stat.advanceTime()
            self.stat.tasks.scheduled += 1
            # the execution has left the database queue (Pending -> Scheduled): without this, it
            # would be counted twice as waiting (pending and taken) until the next sample()
            self.stat.tasks.pending = max(0, self.stat.tasks.pending - 1)
            self.stat.countEvent('pooled')
            self._changed()
        self.syncDb()

    def dropped(self, weid: str) -> None:
        """Count a taken execution that ended before its workflow process was started."""
        with self.lock:
            self.stat.advanceTime()
            self.stat.tasks.scheduled = max(0, self.stat.tasks.scheduled - 1)
            self._changed()
        self.syncDb()

    def started(self, weid: str, wid: str) -> None:
        """Count a workflow process being started for a taken execution."""
        with self.lock:
            self.stat.advanceTime()
            self.stat.tasks.scheduled = max(0, self.stat.tasks.scheduled - 1)
            self.stat.tasks.running += 1
            self.stat.lastJobNew(SchedulerStat.JobInfo(we_id=weid, wid=wid, status='Running', started=datetime.datetime.now(), finished=None))
            self._changed()
        self.syncDb()

    def finished(self, weid: str, status: str) -> None:
        """
        Count the end of a workflow process.

        :param weid: execution ID
        :param status: outcome: ``Finished``, ``Failed``, or ``Pending`` (returned to the queue, not
                       counted as processed)
        """
        with self.lock:
            self.stat.advanceTime()
            self.stat.tasks.running = max(0, self.stat.tasks.running - 1)
            if status != 'Pending':
                self.stat.tasks.processed += 1
                self.stat.countEvent('processed')
                if status == 'Finished':
                    self.stat.tasks.finished += 1
                    self.stat.countEvent('finished')
                else:
                    self.stat.tasks.failed += 1
                    self.stat.countEvent('failed')
            self.stat.lastJobDone(we_id=weid, status=status, finished=datetime.datetime.now())
            self._changed()
        self.syncDb()

    def sample(self, counts: dict[str, int]) -> None:
        """
        Store the numbers of executions in the database by status (once per scheduler iteration).

        Gives the queue length (``Pending``), the waiting executions (queue + taken by this
        scheduler) and the ``Running`` executions according to the database. They are copied to the
        database by the following :meth:`persist`.

        :param counts: number of executions by status, see :meth:`ExecutionBackend.statusCounts`
        """
        with self.lock:
            self.stat.advanceTime()
            self.stat.tasks.pending = counts.get('Pending', 0)
            self.stat.tasks.dbRunning = counts.get('Running', 0)
            self._changed()

    def persist(self) -> None:
        """
        Write the statistics to the statistics file (if any) and copy them to the database.

        Called every scheduler iteration: the copy in the database is refreshed even without
        changes, which also repairs it after a failed copy.
        """
        with self.lock:
            self.stat.advanceTime()
            self._changed()
            if self.statFile:
                try:
                    self.stat.save_to_file(self.statFile)
                except Exception:
                    log.exception(f'Could not write the statistics file {self.statFile}')
        self.syncDb()

    def _changed(self) -> None:
        """
        Note a change of the statistics (call with :attr:`lock` held): update the derived levels
        and the peaks in the history, and mark the statistics for copying by :meth:`syncDb`.
        """
        self.stat.updateGauges()
        self._version += 1

    def syncDb(self) -> None:
        """
        Copy the latest statistics to the database, unless they are there already; never raises.

        Called after each change *without* holding :attr:`lock`: the REST request may take
        seconds (timeouts), and while it was made under the lock it blocked the other job threads
        and :meth:`getStatistics`, which then timed out in the monitoring pages. Copies are made
        one at a time (:attr:`_syncLock`) and each takes the newest statistics, so an older
        snapshot can never overwrite a newer one; threads which find their change already copied
        by another one return at once.
        """
        with self._syncLock:
            with self.lock:
                if self._version == self._syncedVersion:
                    return
                version, values = self._version, self.stat.dbValues()
            if SchedulerStat.storeInDb(values):
                self._syncedVersion = version

    # --- Pyro API --------------------------------------------------------------------------------

    @Pyro5.api.expose
    def getStatistics(self, raw=False):
        """
        Return the statistics.

        :param raw: return the complete :class:`SchedulerStat` as JSON-compatible data (to rebuild
                    the model on the other side) instead of the flat dictionary of the older format
        :return: dictionary of statistics
        """
        with self.lock:
            self.stat.advanceTime()
            s = self.stat.model_copy(deep=True)
        if raw:
            return s.model_dump(mode='json')
        h48, h2 = s.hist48h, s.hist2h
        return dict(
            # --- keys of the original format (read by mupif.monitor.schedulerInfo of all versions)
            runningTasks=s.tasks.running,
            scheduledTasks=s.tasks.scheduled,
            processedTasks=s.tasks.processed,
            finishedTasks=s.tasks.finished,
            failedTasks=s.tasks.failed,
            lastJobs=[list(j.model_dump(mode='json').values()) for j in s.lastJobs],
            currentLoad=s.load,
            processedTasks48=h48.processed,
            pooledTasks48=h48.pooled,
            finishedTasks48=h48.finished,
            failedTasks48=h48.failed,
            load48=h48.load,
            draining=self.draining,
            # --- load of the scheduler: added keys, ignored by older readers
            #: executions that can run at once (to sum the capacity of several schedulers)
            capacity=s.capacity,
            #: executions waiting to run = pendingTasks + scheduledTasks
            waitingTasks=s.tasks.waiting,
            #: executions in the database queue (Pending), shared by the deployment
            pendingTasks=s.tasks.pending,
            #: executions Running according to the database (None: not sampled yet)
            dbRunningTasks=s.tasks.dbRunning,
            #: peak running / waiting executions in each hour of the last 48 h (oldest first)
            running48=h48.running,
            waiting48=h48.waiting,
            #: peak running / waiting executions in each minute of the last 2 h (oldest first)
            running120m=h2.running,
            waiting120m=h2.waiting,
            #: start of the last (current) period of each history in seconds since the epoch, and
            #: the length of the periods: the time axis is periodStart - (count-1-i)*periodLength
            periodStart48=h48.periodStart(),
            periodLength48=h48.interval,
            periodStart120m=h2.periodStart(),
            periodLength120m=h2.interval,
        )

    @Pyro5.api.expose
    @localCallerOnly
    def stop(self):
        """Stop the scheduler (callers on the scheduler's host only) and unregister the monitor."""
        log.warning('Stop requested through the monitor')
        self.onStop()
        self.unregister()

    @Pyro5.api.expose
    @localCallerOnly
    def setDrain(self, on: bool) -> None:
        """
        Switch the scheduler's drain mode (callers on the scheduler's host only).

        While draining, the scheduler takes no new executions and lets running ones finish.

        :param on: True to drain, False to resume
        """
        self.onDrain(bool(on))

    def unregister(self) -> None:
        """
        Remove the monitor from the nameserver, so that the monitoring pages do not list a scheduler which is gone.

        Called when the scheduler shuts down (and by :meth:`stop`); safe to call more than once,
        never raises. The name is the same for every scheduler run: if the nameserver entry does
        not point to this monitor any more (a new scheduler has registered meanwhile), it is left
        alone.
        """
        ns, self.ns = self.ns, None
        if ns is None:
            return
        try:
            # a Pyro proxy belongs to the thread which created it; stop() runs in a Pyro thread
            claim = getattr(ns, '_pyroClaimOwnership', None)
            if claim is not None:
                claim()
            if self.uri is not None and str(ns.lookup(NS_NAME)) != str(self.uri):
                log.info(f'{NS_NAME} in the nameserver belongs to another scheduler now, not removing it')
                return
            ns.remove(NS_NAME)
            log.info(f'Removed {NS_NAME} from the nameserver')
        except Pyro5.errors.NamingError:
            pass  # not registered (any more)
        except Exception:
            log.exception(f'Could not remove {NS_NAME} from the nameserver')

    def registerPyro(self, *, daemon, ns, uri, appName, exclusiveDaemon):
        """Accept the registration callback of :func:`mupif.pyroutil.runServer` (nothing to do)."""

    def runServer(self, ns) -> str:
        """
        Serve the monitor over Pyro and register it in the nameserver.

        :param ns: nameserver proxy
        :return: URI of the monitor
        """
        import mupif as mp
        self.ns = ns
        # the shared daemon of mupif runs in a daemon thread; without passing it, runServer creates
        # an exclusive daemon in a non-daemon thread, which keeps the process alive after the
        # scheduler has shut down
        daemon = mp.pyroutil.getDaemon(proxy=ns)
        self.uri = mp.pyroutil.runServer(ns=ns, appName=NS_NAME, app=self, daemon=daemon, metadata={'type:scheduler'})
        return self.uri
