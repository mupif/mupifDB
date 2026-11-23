"""
The scheduler: main loop, job threads and the status changes of executions.

:class:`Scheduler` runs in the scheduler process. Its main loop (:meth:`Scheduler.run`) wakes up every
``poll_interval`` seconds and takes pending executions, at most as many as it has free job slots
(``max_jobs``). Each execution taken is run by a job thread (:meth:`Scheduler.runJob`) through the
:class:`~mupifDB.scheduler.runner.JobRunner`, which starts the workflow subprocess and waits for it.

Several schedulers may run at once (normally on several hosts). An execution is taken under a
**lease** (:mod:`mupifDB.api.leases`): claiming it is atomic between schedulers, and every later
change made for it names the lease (:class:`~mupifDB.scheduler.backend.Lease`). If another
scheduler took the execution over meanwhile, the change is refused
(:class:`~mupifDB.scheduler.backend.LeaseLost`) and the job is dropped without writing anything.

Status changes made here (database access through :class:`~mupifDB.scheduler.backend.ExecutionBackend`):

* ``Pending -> Scheduled``: the execution is claimed (fails if another scheduler claimed it);
* ``Pending -> Created``: the resources of the execution stayed unavailable for ``max_attempts``
  attempts (claimed first, so that only one scheduler does it; the owner is notified);
* ``Pending -> Failed``: the execution asks for an execution profile its workflow does not have
  (claimed first; a short log tells the owner why);
* ``Pending`` with a retry delay: its resources are unavailable (an attempt is counted, by one
  scheduler only, see :meth:`ExecutionBackend.defer`);
* ``Scheduled -> Running``: the job thread starts the workflow subprocess (attempt count + 1);
* ``Running -> Finished | Failed``: outcome of the run (the owner is notified), see
  :func:`~mupifDB.scheduler.runner.statusForExitCode`;
* ``Running -> Pending``: no free model-server slot when the workflow started (the attempt is not
  counted; retried after a delay), or the scheduler is shutting down;
* ``Scheduled -> Pending``: the job thread failed before starting the workflow (e.g. REST API
  unreachable; retried after a delay); ``Scheduled -> Failed``: the workflow package cannot be
  used, or compulsory inputs are not set (a short log tells the owner why);
* ``Scheduled | Running -> Pending`` after a takeover: executions whose lease expired, held by a
  scheduler which is not alive (see :meth:`Scheduler.recoverExpired`).

Retry delays are stored in the database (``NotBefore``, growing with consecutive delays), so all
schedulers respect them and they survive restarts.

Model servers: the scheduler checks that the models of an execution can find free slots on
compatible model servers before it takes it (:class:`~mupifDB.scheduler.resources.ResourceBudget`,
under the execution's execution profile); the workflow itself chooses the server for each model
when it allocates it, and the execution script stores the choice in the execution record
(``ModelServers``), from where the scheduler takes it for its statistics and the clean-up of
model-server jobs (:meth:`Scheduler.refreshPlacements`).

Threads: the main loop runs in the thread calling :meth:`Scheduler.run`; one job thread per running
execution (at most ``max_jobs``); the Pyro daemon of the monitor. :attr:`Scheduler.jobs` (executions
taken and not finished) is shared and protected by :attr:`Scheduler.lock`; the statistics are
protected by the monitor's own lock.
"""

import concurrent.futures
import contextvars
import logging
import random
import threading
import time
from typing import Any, Callable

from mupif import modelplacement

from .backend import ExecutionBackend, Lease, LeaseLost
from .config import SchedulerConfig
from .monitor import SchedulerMonitor
from .resources import InvalidProfileError, ResourceBudget
from .runner import Job, JobRunner
from .staging import StagingError

log = logging.getLogger('scheduler')

#: execution handled by the current job thread (None elsewhere), added to log records by :class:`ExecutionLogFilter`
currentExecution: contextvars.ContextVar[str | None] = contextvars.ContextVar('currentExecution', default=None)


class ExecutionLogFilter(logging.Filter):
    """
    Add the attribute ``weid`` (execution ID, or None) to every log record [S12].

    Log formats can show it (``%(weid)s``), and the REST log handler sends it with the record, so the
    log of a scheduler can be filtered by execution. Installed on the ``scheduler`` logger.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Set ``record.weid``; never drops a record."""
        if not hasattr(record, 'weid'):
            record.weid = currentExecution.get()
        return True


log.addFilter(ExecutionLogFilter())

def _formatServers(modelServers: dict[str, str]) -> str:
    """Format ``model -> model server`` for logs: ``m1→CVUT.demo01~node2, m2→…``."""
    return ', '.join(f'{m}→{s}' for m, s in sorted(modelServers.items()))


class InputsIncompleteError(Exception):
    """Compulsory inputs of the execution are not set; running it cannot succeed."""


def runningExecutionProcesses() -> set[str]:
    """
    Find execution subprocesses alive on this host.

    :return: execution IDs of all running ``workflow_execution_script.py -eid <ID>`` processes
    """
    import psutil
    weids = set()
    for proc in psutil.process_iter(['cmdline']):
        cmd = proc.info['cmdline'] or []
        if any(c.endswith('workflow_execution_script.py') for c in cmd) and '-eid' in cmd[:-1]:
            weids.add(cmd[cmd.index('-eid') + 1])
    return weids


class Scheduler:
    """
    Take pending executions and run them, at most ``config.max_jobs`` at a time.

    Invariant: :attr:`jobs` contains exactly the executions this scheduler has taken (set to
    ``Scheduled``) and whose outcome is not stored yet.
    """

    def __init__(self, config: SchedulerConfig, backend: ExecutionBackend, runner: JobRunner,
                 findModelServers: Callable[[Any], list], serverLoad: Callable[[Any], Any], monitor: SchedulerMonitor,
                 terminateModelJobs: Callable[[list[str], str], list[str]] = lambda jobmanagers, loggerURI: [],
                 removeModelServer: Callable[[str, str], bool] = lambda name, uri: False):
        """
        Set up the scheduler; nothing runs until :meth:`run`.

        :param config: settings
        :param backend: database access
        :param runner: runs one execution (see :class:`JobRunner`)
        :param findModelServers: ``requirement -> compatible model servers`` (see
                                 :func:`mupif.modelplacement.findModelServers`)
        :param serverLoad: ``model server -> load`` (None if unreachable; see
                           :func:`mupif.modelplacement.serverLoad`)
        :param removeModelServer: ``(registered name, URI) -> removed``: removes the nameserver entry
                                  of a model server, if it still points to that URI (see
                                  :meth:`checkedServerLoad`)
        :param monitor: statistics
        :param terminateModelJobs: ``(jobmanagers, loggerURI) -> keys of terminated jobs``: terminates
                                   the model-server jobs of an execution (see
                                   :func:`~mupifDB.scheduler.resources.terminateExecutionModelJobs`)
        """
        self.config = config
        self.backend = backend
        self.runner = runner
        self.findModelServers = findModelServers
        self.serverLoad = serverLoad
        self.removeModelServer = removeModelServer
        #: (registered name, URI) of model servers which did not answer -> monotonic time of the first failure
        self.unreachableSince: dict[tuple[str, str], float] = {}
        self.monitor = monitor
        self.terminateModelJobs = terminateModelJobs
        #: protects :attr:`jobs`
        self.lock = threading.Lock()
        #: execution ID -> job, for every execution taken and not finished
        self.jobs: dict[str, Job] = {}
        #: set to end the main loop; also interrupts its sleep
        self.stopEvent = threading.Event()
        #: set while draining: no new executions are taken, running ones continue (see :meth:`setDrain`)
        self.draining = threading.Event()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=config.max_jobs, thread_name_prefix='job')
        #: sends notifications to owners (e-mail) without holding up the job threads
        self.notifier = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix='notify')

    # --- main loop ------------------------------------------------------------------------------

    def run(self) -> None:
        """
        Run until :meth:`stop` is called: recover interrupted executions, then take pending ones every ``poll_interval``.

        Returns when the main loop has ended and all job threads have finished.
        """
        self.recoverExpired()
        log.info('Entering main loop to check for Pending executions')
        while not self.stopEvent.is_set():
            try:
                # placements first: jobs whose models are allocated stop being counted as in flight
                self.refreshPlacements()
                self.schedulePass()
                self.sampleQueue()
                self.monitor.persist()
            except Exception:
                # e.g. REST API or nameserver temporarily unreachable: an error in one iteration must
                # not end the scheduler (running executions continue); try again in the next one
                log.exception('Error in scheduler iteration; continuing with the next one')
            self.stopEvent.wait(self.config.poll_interval)
        self.shutdown()

    def sampleQueue(self) -> None:
        """
        Give the statistics the numbers of executions in the database by status (queue length, Running).

        Best effort: if the database cannot be asked, the statistics keep the previous numbers and
        the scheduling goes on.
        """
        try:
            counts = self.backend.statusCounts()
        except Exception:
            log.exception('Could not count the executions by status for the statistics')
            return
        self.monitor.sample(counts)

    def stop(self) -> None:
        """
        Ask the scheduler to shut down: the main loop ends after its current pass, then :meth:`shutdown` runs.

        Safe to call from a signal handler or another thread.
        """
        self.stopEvent.set()

    def shutdown(self) -> None:
        """
        Finish the running executions: wait up to ``drain_timeout``, then stop the remaining ones.

        A stopped workflow gets SIGTERM (its execution script terminates its models) and SIGKILL after
        ``kill_grace``; its execution returns to ``Pending`` with its attempt count and runs again
        after the restart. Returns when every job thread has stored its outcome.
        """
        with self.lock:
            running = len(self.jobs)
        if running:
            log.warning(f'Shutting down with {running} running execution(s): waiting up to {self.config.drain_timeout} s')
            deadline = time.monotonic() + self.config.drain_timeout
            while time.monotonic() < deadline:
                with self.lock:
                    if not self.jobs:
                        break
                time.sleep(0.2)
            with self.lock:
                remaining = list(self.jobs.values())
            for job in remaining:
                job.stop('shutdown')
        self.executor.shutdown(wait=True)
        self.notifier.shutdown(wait=True)
        self.monitor.persist()
        log.info('Scheduler shut down')

    def setDrain(self, on: bool) -> None:
        """
        Switch drain mode, e.g. before maintenance [S12].

        While draining, no new executions are taken; running ones continue to their end. Once
        :attr:`jobs` is empty the scheduler can be stopped without stopping any workflow.

        :param on: drain (True) or resume taking executions (False)
        """
        (self.draining.set if on else self.draining.clear)()
        self.monitor.draining = on
        log.warning(f'Drain mode {"on: no new executions are taken" if on else "off: taking executions again"}')

    def freeSlots(self) -> int:
        """Return how many more executions can be taken now."""
        with self.lock:
            return self.config.max_jobs - len(self.jobs)

    @property
    def backoff(self) -> tuple[float, float, float]:
        """
        Retry delay after a transient failure [S4]: ``(retry_backoff, retry_backoff_max, retry_jitter)``.

        The delay is ``retry_backoff`` doubled for every consecutive delay of the execution (stored
        with it, ``Deferrals``), at most ``retry_backoff_max``, varied by ``retry_jitter``; the
        database computes it and stores the time of the next attempt (``NotBefore``), which every
        scheduler respects (pending executions not due are not listed).
        """
        return (self.config.retry_backoff, self.config.retry_backoff_max, self.config.retry_jitter)

    def schedulePass(self) -> int:
        """
        Take pending executions, oldest submitted first, while job slots and model-server slots allow.

        Model-server slots are budgeted for the pass (:class:`ResourceBudget`), over the model servers
        compatible with each model under the execution's execution profile: an execution whose
        model servers are busy waits for a later pass without using up an attempt; one whose model
        servers (or workflow record) are unreachable uses up an attempt and is retried after a
        growing delay (stored with the execution), and after ``max_attempts`` attempts it returns
        to ``Created``. An execution asking for an execution profile its workflow does not have
        fails. Only executions whose retry delay has passed are listed. An execution claimed by
        another scheduler first is skipped.

        :return: number of executions taken
        """
        if self.draining.is_set():
            return 0
        free = self.freeSlots()
        if free <= 0:
            return 0
        pending = self.backend.pendingExecutions(limit=self.config.fetch_limit)
        budget = ResourceBudget(self.backend.getWorkflow, self.findModelServers, self.checkedServerLoad)
        self.reserveInFlight(budget)
        taken = 0
        for rec in pending:
            if taken >= free:
                break
            weid = rec.dbID
            with self.lock:
                if weid in self.jobs:
                    continue
            if rec.Attempts > self.config.max_attempts:
                # claimed first: of several schedulers only one returns it and notifies the owner
                self.giveUp(weid, 'Created', f'resources unavailable for {rec.Attempts} attempts, returning it to Created')
                continue
            try:
                resources = budget.reserve(rec.WorkflowID, rec.WorkflowVersion, getattr(rec, 'ExecutionProfileIndex', -1))
            except InvalidProfileError as e:
                # will not change by waiting: fail it and tell the owner why
                self.giveUp(weid, 'Failed', f'cannot run: {e}', message=f'The execution was not started: {e}')
                continue
            if resources.status == ResourceBudget.BUSY:
                # the model servers are working on other executions: wait without using up an attempt
                log.debug(f'{weid} waits for free model server slots')
                continue
            if resources.status == ResourceBudget.UNAVAILABLE:
                # an attempt is counted and the retry delayed; by one scheduler only per window
                try:
                    if self.backend.defer(weid, self.backoff):
                        log.info(f'{weid} cannot be scheduled due to unavailable resources; retried later (attempt {rec.Attempts + 1})', extra={'weid': weid})
                except Exception:
                    log.exception(f'{weid}: could not postpone it')
                continue
            # a job thread only runs executions this scheduler claimed: one taken by another
            # scheduler meanwhile is skipped, one whose claim failed otherwise (REST API error)
            # stays Pending and is considered again in the next pass
            try:
                claimed, lease = self.backend.claim(weid)
            except LeaseLost:
                log.debug(f'{weid} was taken by another scheduler')
                continue
            except Exception:
                log.exception(f'Could not claim {weid}; not taking it')
                continue
            job = Job(weid=weid, record=claimed, lease=lease)
            with self.lock:
                self.jobs[weid] = job
            self.monitor.enqueued(weid)
            self.executor.submit(self.runJob, job)
            taken += 1
            log.info(f'{weid} taken (attempts so far: {rec.Attempts}; {resources}; lease epoch {lease.epoch})', extra={'weid': weid})
        return taken

    def giveUp(self, weid: str, status: str, reason: str, message: str | None = None) -> None:
        """
        Claim a pending execution and end it without running it (``Created`` or ``Failed``), notifying its owner.

        Claimed first, so that of several schedulers finding the same reason only one does it (and
        the owner gets one notification). Errors are logged.

        :param weid: execution ID
        :param status: ``Created`` (back to the user) or ``Failed``
        :param reason: for the scheduler log
        :param message: stored as the execution's log, so that its owner sees why (optional)
        """
        try:
            _, lease = self.backend.claim(weid)
        except LeaseLost:
            return
        except Exception:
            log.exception(f'{weid}: could not claim it to set it {status}')
            return
        log.warning(f'{weid}: {reason}', extra={'weid': weid})
        if message:
            self.storeMessageLog(weid, message, lease)
        try:
            self.backend.transition(lease, status, resetAttempts=(status == 'Created'))
            self.notify(weid)
        except LeaseLost:
            log.warning(f'{weid}: lease lost before it could be set {status}', extra={'weid': weid})
        except Exception:
            log.exception(f'{weid}: could not set it {status}')

    # --- one execution (job thread) -------------------------------------------------------------

    def runJob(self, job: Job) -> None:
        """
        Run one claimed execution to its end and store the outcome; never raises.

        Errors are handled here (see the module documentation for the resulting statuses): before
        the workflow subprocess is launched the execution returns to ``Pending`` (``Failed`` if its
        workflow package cannot be used), after the launch it fails. Every change is made under the
        job's lease; if the lease was lost (another scheduler took the execution over), the job ends
        without writing anything, notifying anybody or cleaning up model-server jobs (the new
        holder did that).

        :param job: the execution, claimed by :meth:`schedulePass` (``Scheduled``, ``job.lease``)
        """
        weid, lease = job.weid, job.lease
        launched = False
        token = currentExecution.set(weid)
        thread = threading.current_thread()
        threadName, thread.name = thread.name, f'job:{weid}'
        try:
            rec = self.backend.getExecution(weid)
            if rec.Status != 'Scheduled' or rec.SchedulerID != lease.schedulerId or int(rec.LeaseEpoch) != lease.epoch:
                raise LeaseLost(f'{weid} is {rec.Status} under {rec.SchedulerID}:{rec.LeaseEpoch}, not Scheduled under {lease.header()}')
            if job.stopRequested.is_set():
                # shutting down before the workflow started: no point in starting it
                self.backend.transition(lease, 'Pending')
                self.monitor.dropped(weid)
                return
            workflow = self.backend.getWorkflow(rec.WorkflowID, rec.WorkflowVersion)
            self.checkInputs(weid)

            def onLaunch():
                nonlocal launched
                self.backend.transition(lease, 'Running', incAttempts=1, renew=True)
                launched = True
                job.launched = True
                self.monitor.started(weid, rec.WorkflowID)

            result = self.runner.run(job, workflow, onLaunch=onLaunch, storeLog=lambda path: self.backend.storeLog(weid, path, lease),
                                     timeout=self.jobTimeout(workflow))
            if result.stopReason == 'lease-lost':
                raise LeaseLost(f'{weid}: lease lost while it ran')
            final = self.terminateLeftoverModelJobs(weid, workflow, lease)
            # the model servers the workflow chose, as stored by the execution script (for the statistics)
            modelServers = dict(getattr(final, 'ModelServers', None) or {}) or job.modelServers
            log.warning(f'Workflow execution {weid} {result.status}' + (f' ({result.stopReason})' if result.stopReason else '')
                        + (f'; model servers: {_formatServers(modelServers)}' if modelServers else ''))
            if result.status == 'Pending' and result.stopReason is None:
                # no free model-server slot when the workflow started: retry later, not at once; the
                # attempt is not counted (it would send executions back to Created when several
                # schedulers compete for the model servers)
                self.backend.transition(lease, 'Pending', incAttempts=-1, backoff=self.backoff)
            else:
                self.backend.transition(lease, result.status)
            if result.status != 'Pending':
                self.notify(weid)
            self.monitor.finished(weid, result.status, modelServers=modelServers)
        except LeaseLost as e:
            # another scheduler holds the execution now (it took it over after our lease expired):
            # nothing more is written for it from here
            log.warning(f'Execution {weid}: lease lost, dropping it without writing anything ({e})')
            (self.monitor.finished(weid, 'Pending') if launched else self.monitor.dropped(weid))
        except InputsIncompleteError as e:
            log.warning(f'Execution {weid} failed: {e}')
            self.storeMessageLog(weid, f'The execution was not started: {e}', lease)
            self._recover(weid, lease, launched, permanent=True)
        except Exception as e:
            log.exception(f'Execution of workflow {weid} failed.')
            self._recover(weid, lease, launched, permanent=isinstance(e, StagingError))
        finally:
            with self.lock:
                self.jobs.pop(weid, None)
            thread.name = threadName
            currentExecution.reset(token)

    def notify(self, weid: str) -> None:
        """
        Notify the owner of an execution about its new status, in the background [S12].

        Notifications (an e-mail, through a REST call and an SMTP server) are sent one at a time by
        :attr:`notifier`, so that a slow mail server does not keep a job slot busy. Errors are logged.

        :param weid: execution ID
        """
        def send():
            try:
                self.backend.notifyOwner(weid)
            except Exception:
                log.exception(f'{weid}: could not notify the owner')
        self.notifier.submit(send)

    def checkInputs(self, weid: str) -> None:
        """
        Make sure the compulsory inputs of an execution are set before its workflow is started [S3].

        A failure of the check itself (e.g. REST API briefly unreachable) is only logged: the run
        proceeds, and the execution script reports missing inputs as a workflow error.

        :param weid: execution ID
        :raises InputsIncompleteError: if some compulsory input is not set
        """
        try:
            complete = self.backend.inputsComplete(weid)
        except Exception:
            log.exception(f'{weid}: could not check its inputs; running it anyway')
            return
        if not complete:
            raise InputsIncompleteError('some compulsory inputs are not set')

    def storeMessageLog(self, weid: str, message: str, lease: Lease | None = None) -> None:
        """
        Store a short message as the log of an execution that did not run, so that its owner sees why.

        :param weid: execution ID
        :param message: text of the log
        :param lease: the lease the execution is held under
        """
        import tempfile
        from pathlib import Path
        try:
            with tempfile.TemporaryDirectory(dir=self.config.work_dir, prefix='mupifDB') as tmp:
                path = Path(tmp) / 'scheduler.log'
                path.write_text(message + '\n')
                self.backend.storeLog(weid, path, lease)
        except Exception:
            log.exception(f'{weid}: could not store the message log')

    def jobTimeout(self, workflow: Any) -> float:
        """
        Return the maximum run time of an execution of *workflow* in seconds (0: no limit).

        The workflow's own ``Timeout`` (from its metadata) if it declares one, else the
        ``default_job_timeout`` setting.
        """
        own = getattr(workflow, 'Timeout', 0) or 0
        return float(own) if own > 0 else self.config.default_job_timeout

    def _recover(self, weid: str, lease: Lease, launched: bool, permanent: bool) -> None:
        """
        Set the status of an execution whose job failed with an exception.

        :param weid: execution ID
        :param lease: the lease it is held under
        :param launched: whether its workflow subprocess was started
        :param permanent: the error will not go away by retrying (unusable workflow package)
        """
        try:
            if launched or permanent:
                self.backend.transition(lease, 'Failed')
                self.notify(weid)
            else:
                # transient (e.g. REST API unreachable): back to the queue, retried after a delay
                self.backend.transition(lease, 'Pending', backoff=self.backoff)
        except LeaseLost:
            log.warning(f'{weid}: lease lost, its status is not changed from here')
        except Exception:
            # REST API unreachable: nothing more can be done from here; the lease is not renewed
            # any more, so after it expires a scheduler takes the execution over and requeues it
            log.exception(f'Could not update status of execution {weid} after the failure.')
        if launched:
            self.monitor.finished(weid, 'Failed')
        else:
            self.monitor.dropped(weid)

    def compatibleServers(self, workflow: Any, profileIndex: int, exclude: set[str] = frozenset()) -> list[str]:
        """
        Return the registered names of all model servers compatible with the models of *workflow*.

        Used for the models whose servers are not known (e.g. the script died before storing them,
        or templates which the workflow allocates later). An unknown execution profile falls back
        to the default one, which is a superset of the servers of any profile, as far as identities go.

        :param workflow: workflow record
        :param profileIndex: execution profile of the execution
        :param exclude: models left out (their servers are known)
        """
        try:
            reqs = modelplacement.modelRequirements(workflow, profileIndex)
        except ValueError:
            reqs = modelplacement.modelRequirements(workflow, -1)
        names = set()
        for model, req in reqs.items():
            if model not in exclude:
                names.update(c.nsName for c in self.findModelServers(req))
        return sorted(names)

    def terminateLeftoverModelJobs(self, weid: str, workflow: Any = None, lease: Lease | None = None) -> Any:
        """
        Terminate model-server jobs still allocated for an execution whose workflow process has ended.

        Normally there are none: the execution script terminates its models. A script that was
        killed (or stopped while blocked in a call to a model) leaves them running, occupying slots
        on the model servers (finding #17). The jobs are looked for on the model servers the
        execution used (``ModelServers`` of the record, stored by the execution script once its
        models were allocated at initialization), and on every server compatible with the models
        not stored there: all models if nothing was stored, else the templates (``Instantiate:
        false``), whose copies the workflow allocated later, on servers not recorded anywhere.
        Best effort: errors are logged.

        :param weid: execution ID
        :param workflow: workflow record of the execution (read if not given)
        :param lease: the lease the execution is held under: if it is not current any more, nothing
                      is done (the record's ``loggerURI`` may be the new holder's)
        :return: the execution record as read here (None if it could not be read)
        """
        rec = None
        try:
            rec = self.backend.getExecution(weid)
            if lease is not None and (rec.SchedulerID != lease.schedulerId or int(rec.LeaseEpoch) != lease.epoch):
                log.warning(f'{weid}: lease lost, not cleaning up model-server jobs (the new holder does)')
                return rec
            loggerURI = getattr(rec, 'loggerURI', None)
            if not loggerURI:
                return rec
            stored = dict(getattr(rec, 'ModelServers', None) or {})
            if workflow is None:
                workflow = self.backend.getWorkflow(rec.WorkflowID, rec.WorkflowVersion)
            # the servers used at initialization, plus every server the other models (templates,
            # or all if nothing was stored) may have been allocated on
            servers = sorted(set(stored.values()) | set(self.compatibleServers(workflow, getattr(rec, 'ExecutionProfileIndex', -1), exclude=set(stored))))
            if servers and (terminated := self.terminateModelJobs(servers, loggerURI)):
                log.warning(f'{weid}: terminated model-server jobs left by its workflow: {terminated}')
        except Exception:
            log.exception(f'{weid}: could not clean up its model-server jobs')
        return rec

    def checkedServerLoad(self, server: Any) -> Any:
        """
        Return the load of a model server (None if unreachable); remove it from the nameserver once it stayed unreachable too long.

        A model server killed or crashed leaves its nameserver entry behind; the scheduler, which
        asks the compatible servers every pass, would try it for ever (a warning per pass). Now an
        unreachable server is reported once; if it is still unreachable after
        ``stale_server_timeout`` seconds, its entry is removed (only if it still points to the same
        server) and reported. A live server removed by mistake (e.g. a network problem between it
        and the scheduler) registers again by itself (mupif ``watchRegistration``). A server which
        answers again is reported too.

        :param server: compatible model server (``nsName``, ``uri``; see mupif.modelplacement.Candidate)
        :return: its load, None if it cannot be reached
        """
        key = (server.nsName, str(server.uri))
        load = self.serverLoad(server)
        now = time.monotonic()
        if load is not None:
            if self.unreachableSince.pop(key, None) is not None:
                log.info(f'Model server {server.nsName} answers again')
            return load
        since = self.unreachableSince.setdefault(key, now)
        if since == now:
            log.warning(f'Model server {server.nsName} ({server.uri}) does not answer; skipped while unreachable'
                        + (f', its nameserver entry is removed after {self.config.stale_server_timeout:.0f} s' if self.config.stale_server_timeout > 0 else ''))
        elif 0 < self.config.stale_server_timeout <= now - since:
            del self.unreachableSince[key]
            try:
                removed = self.removeModelServer(server.nsName, str(server.uri))
            except Exception:
                log.exception(f'Could not remove model server {server.nsName} from the nameserver')
                removed = False
            if removed:
                log.warning(f'Removed model server {server.nsName} from the nameserver: unreachable for {now - since:.0f} s')
        return None

    def reserveInFlight(self, budget: ResourceBudget) -> None:
        """
        Count the slots of executions taken earlier whose workflows have not allocated their models yet.

        The model servers report a slot as used only once the workflow has allocated its model,
        which takes a few seconds after the start (staging, starting Python, importing the
        workflow). A pass in between saw those slots as free and took more executions than the
        servers could serve; the extra ones found no free slot, exited with code 2 and were
        requeued (seen with two instances and a short poll interval). Such in-flight jobs are now
        counted in the budget first, as if they were new executions. A job stops being in flight
        when :meth:`refreshPlacements` finds its models allocated. Errors are ignored: the count is
        only a better estimate.

        :param budget: the budget of the current pass
        """
        with self.lock:
            inFlight = [j for j in self.jobs.values() if not j.allocated]
        for job in inFlight:
            try:
                budget.reserve(job.record.WorkflowID, job.record.WorkflowVersion, getattr(job.record, 'ExecutionProfileIndex', -1))
            except Exception:
                pass

    def refreshPlacements(self) -> None:
        """
        Take the model servers of running executions from their records, for the statistics.

        The workflow chooses a model server for each model when it allocates it, and the execution
        script stores the choice in the execution record (``ModelServers``) right after. For every
        running job whose models are not known to be allocated yet, the record is read (at most one
        REST call per such job and pass, only until allocated), the servers are logged and given to
        the statistics (:meth:`SchedulerMonitor.placed`). A job is allocated once its record has
        ``ModelServers``, or ``workflowURI`` (set by the execution script after ``initialize()``
        also with workflows of older mupif versions, which do not report their servers); until
        then :meth:`reserveInFlight` counts its slots. Both fields are cleared by the REST API when
        the execution becomes ``Running``, so values of an earlier attempt do not count. Best
        effort: errors are logged at debug level, and the next pass tries again.
        """
        with self.lock:
            jobs = [j for j in self.jobs.values() if j.launched and not j.allocated]
        for job in jobs:
            try:
                rec = self.backend.getExecution(job.weid)
            except Exception as e:
                log.debug(f'{job.weid}: could not read its model servers: {e}')
                continue
            modelServers = dict(getattr(rec, 'ModelServers', None) or {})
            if not modelServers:
                if getattr(rec, 'workflowURI', None):
                    # initialized, but its workflow does not report model servers (older mupif)
                    job.allocated = True
                continue
            job.modelServers = modelServers
            job.allocated = True
            log.info(f'{job.weid}: models allocated on model servers {_formatServers(modelServers)}', extra={'weid': job.weid})
            self.monitor.placed(job.weid, modelServers)

    # --- startup --------------------------------------------------------------------------------

    def recoverExpired(self, limit: int = 50) -> int:
        """
        Take over executions whose lease expired, held by schedulers which are not alive, and requeue them.

        A scheduler which crashed or was killed (its workflows die with it) leaves its executions
        ``Scheduled``/``Running``; once their leases expired and its heartbeat ended, any scheduler
        takes them over (atomically, :meth:`ExecutionBackend.takeOver`), terminates the model-server
        jobs their workflows left behind and returns them to ``Pending`` (attempt count kept).
        Records from before leases count as expired. Errors are logged.

        :param limit: maximum number of executions taken over at once
        :return: number of executions taken over
        """
        try:
            taken = self.backend.takeOver(limit)
        except Exception:
            log.exception('Could not take over executions with expired leases')
            return 0
        for rec, lease in taken:
            log.warning(f'{rec.dbID} was left {rec.Status} by a scheduler which is gone ({rec.SchedulerID if lease.epoch > 1 else "before leases"}); '
                        f'taken over (epoch {lease.epoch}) and returned to Pending', extra={'weid': rec.dbID})
            if rec.Status == 'Running':
                self.terminateLeftoverModelJobs(rec.dbID, lease=lease)
            try:
                self.backend.transition(lease, 'Pending')
            except Exception:
                log.exception(f'Could not return {rec.dbID} to Pending')
        return len(taken)
