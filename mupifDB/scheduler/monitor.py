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
        self.stat = SchedulerStat()
        SchedulerStat.capacity = capacity
        if statFile and Path(statFile).is_file():
            try:
                self.stat = SchedulerStat.load_from_file(statFile)
            except Exception:
                log.exception(f'Failure reading persistent schedule statistics from {statFile}, starting from scratch.')
        # the totals and the history carry over from the previous run, but nothing is queued or
        # running in this new process yet
        self.stat.tasks.running = 0
        self.stat.tasks.scheduled = 0
        # size the history lists (empty in a new SchedulerStat) before anything is stored in them
        self.stat.advanceTime()
        self.ns = None
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
            self.stat.hist48h.pooled[-1] += 1
            self.stat.sync()

    def dropped(self, weid: str) -> None:
        """Count a taken execution that ended before its workflow process was started."""
        with self.lock:
            self.stat.advanceTime()
            self.stat.tasks.scheduled = max(0, self.stat.tasks.scheduled - 1)
            self.stat.sync()

    def started(self, weid: str, wid: str) -> None:
        """Count a workflow process being started for a taken execution."""
        with self.lock:
            self.stat.advanceTime()
            self.stat.tasks.scheduled = max(0, self.stat.tasks.scheduled - 1)
            self.stat.tasks.running += 1
            self.stat.lastJobNew(SchedulerStat.JobInfo(we_id=weid, wid=wid, status='Running', started=datetime.datetime.now(), finished=None))
            self.stat.sync()

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
                self.stat.hist48h.processed[-1] += 1
                if status == 'Finished':
                    self.stat.tasks.finished += 1
                    self.stat.hist48h.finished[-1] += 1
                else:
                    self.stat.tasks.failed += 1
                    self.stat.hist48h.failed[-1] += 1
            self.stat.lastJobDone(we_id=weid, status=status, finished=datetime.datetime.now())
            self.stat.sync()

    def persist(self) -> None:
        """Write the statistics to the statistics file (if any) and copy them to the database."""
        with self.lock:
            self.stat.advanceTime()
            self.stat.sync()
            if self.statFile:
                try:
                    self.stat.save_to_file(self.statFile)
                except Exception:
                    log.exception(f'Could not write the statistics file {self.statFile}')

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
        return dict(
            runningTasks=s.tasks.running,
            scheduledTasks=s.tasks.scheduled,
            processedTasks=s.tasks.processed,
            finishedTasks=s.tasks.finished,
            failedTasks=s.tasks.failed,
            lastJobs=[list(j.model_dump(mode='json').values()) for j in s.lastJobs],
            currentLoad=s.load,
            processedTasks48=s.hist48h.processed,
            pooledTasks48=s.hist48h.pooled,
            finishedTasks48=s.hist48h.finished,
            failedTasks48=s.hist48h.failed,
            load48=s.hist48h.load,
            draining=self.draining,
        )

    @Pyro5.api.expose
    @localCallerOnly
    def stop(self):
        """Stop the scheduler (callers on the scheduler's host only) and unregister the monitor."""
        log.warning('Stop requested through the monitor')
        self.onStop()
        if self.ns is not None:
            self.ns.remove(NS_NAME)

    @Pyro5.api.expose
    @localCallerOnly
    def setDrain(self, on: bool) -> None:
        """
        Switch the scheduler's drain mode (callers on the scheduler's host only).

        While draining, the scheduler takes no new executions and lets running ones finish.

        :param on: True to drain, False to resume
        """
        self.onDrain(bool(on))

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
        return mp.pyroutil.runServer(ns=ns, appName=NS_NAME, app=self, daemon=daemon, metadata={'type:scheduler'})
