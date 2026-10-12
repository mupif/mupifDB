"""
The scheduler: main loop, job threads and the status changes of executions.

:class:`Scheduler` runs in the scheduler process. Its main loop (:meth:`Scheduler.run`) wakes up every
``poll_interval`` seconds and takes pending executions, at most as many as it has free job slots
(``max_jobs``). Each execution taken is run by a job thread (:meth:`Scheduler.runJob`) through the
:class:`~mupifDB.scheduler.runner.JobRunner`, which starts the workflow subprocess and waits for it.

Status changes made here (database access through :class:`~mupifDB.scheduler.backend.ExecutionBackend`):

* ``Pending -> Scheduled``: the execution is taken (only if this change succeeded);
* ``Pending -> Created``: the resources of the execution stayed unavailable for ``max_attempts``
  attempts (the owner is notified);
* ``Scheduled -> Running``: the job thread starts the workflow subprocess (attempt count + 1);
* ``Running -> Finished | Failed``: outcome of the run (the owner is notified), see
  :func:`~mupifDB.scheduler.runner.statusForExitCode`;
* ``Running -> Pending``: no free model-server slot when the workflow started, or the scheduler is
  shutting down; the execution is run again later (attempt count kept);
* ``Scheduled -> Pending``: the job thread failed before starting the workflow (e.g. REST API
  unreachable); ``Scheduled -> Failed``: the workflow package cannot be used, or compulsory inputs
  are not set (a short log tells the owner why);
* at startup ``Scheduled | Running -> Pending``: executions interrupted by a previous run of the
  scheduler (see :meth:`Scheduler.recoverInterruptedExecutions`).

Threads: the main loop runs in the thread calling :meth:`Scheduler.run`; one job thread per running
execution (at most ``max_jobs``); the Pyro daemon of the monitor. :attr:`Scheduler.jobs` (executions
taken and not finished) is shared and protected by :attr:`Scheduler.lock`; the statistics are
protected by the monitor's own lock.
"""

import concurrent.futures
import logging
import random
import threading
import time
from typing import Any, Callable

from .backend import ExecutionBackend
from .config import SchedulerConfig
from .monitor import SchedulerMonitor
from .resources import ResourceBudget
from .runner import Job, JobRunner
from .staging import StagingError

log = logging.getLogger('scheduler')

#: upper bound on the number of interrupted executions recovered at startup (the REST API pages its results)
RECOVERY_LIMIT = 100000


class NotScheduledError(Exception):
    """The execution handed to a job thread is no longer in the ``Scheduled`` state; it is left alone."""


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
                 freeJobSlots: Callable[[str], int | None], monitor: SchedulerMonitor,
                 terminateModelJobs: Callable[[list[str], str], list[str]] = lambda jobmanagers, loggerURI: []):
        """
        Set up the scheduler; nothing runs until :meth:`run`.

        :param config: settings
        :param backend: database access
        :param runner: runs one execution (see :class:`JobRunner`)
        :param freeJobSlots: ``jobmanager name -> free job slots`` (None if unreachable)
        :param monitor: statistics
        :param terminateModelJobs: ``(jobmanagers, loggerURI) -> keys of terminated jobs``: terminates
                                   the model-server jobs of an execution (see
                                   :func:`~mupifDB.scheduler.resources.terminateExecutionModelJobs`)
        """
        self.config = config
        self.backend = backend
        self.runner = runner
        self.freeJobSlots = freeJobSlots
        self.monitor = monitor
        self.terminateModelJobs = terminateModelJobs
        #: protects :attr:`jobs`
        self.lock = threading.Lock()
        #: execution ID -> job, for every execution taken and not finished
        self.jobs: dict[str, Job] = {}
        #: set to end the main loop; also interrupts its sleep
        self.stopEvent = threading.Event()
        #: execution ID -> (monotonic time before which it is not tried again, consecutive transient failures)
        self.backoff: dict[str, tuple[float, int]] = {}
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=config.max_jobs, thread_name_prefix='job')

    # --- main loop ------------------------------------------------------------------------------

    def run(self) -> None:
        """
        Run until :meth:`stop` is called: recover interrupted executions, then take pending ones every ``poll_interval``.

        Returns when the main loop has ended and all job threads have finished.
        """
        self.recoverInterruptedExecutions()
        log.info('Entering main loop to check for Pending executions')
        while not self.stopEvent.is_set():
            try:
                self.schedulePass()
                self.monitor.persist()
            except Exception:
                # e.g. REST API or nameserver temporarily unreachable: an error in one iteration must
                # not end the scheduler (running executions continue); try again in the next one
                log.exception('Error in scheduler iteration; continuing with the next one')
            self.stopEvent.wait(self.config.poll_interval)
        self.shutdown()

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
        self.monitor.persist()
        log.info('Scheduler shut down')

    def freeSlots(self) -> int:
        """Return how many more executions can be taken now."""
        with self.lock:
            return self.config.max_jobs - len(self.jobs)

    def deferRetry(self, weid: str) -> float:
        """
        Delay the next attempt of an execution after a transient failure [S4].

        The delay is ``retry_backoff`` doubled for every consecutive transient failure, at most
        ``retry_backoff_max``, varied by ``retry_jitter``. Kept in memory: a restart retries at once.

        :param weid: execution ID
        :return: the delay in seconds
        """
        with self.lock:
            failures = self.backoff.get(weid, (0., 0))[1] + 1
            delay = min(self.config.retry_backoff * 2 ** (failures - 1), self.config.retry_backoff_max)
            delay *= 1 + random.uniform(-self.config.retry_jitter, self.config.retry_jitter)
            self.backoff[weid] = (time.monotonic() + delay, failures)
        log.info(f'{weid}: next attempt in {delay:.0f} s (transient failure #{failures})')
        return delay

    def deferred(self, weid: str) -> bool:
        """Tell whether an execution must not be tried yet (see :meth:`deferRetry`)."""
        with self.lock:
            until = self.backoff.get(weid, (0., 0))[0]
        return time.monotonic() < until

    def _workflowJobmanagers(self, wid: str, version: int) -> list[str]:
        """Return the jobmanagers of the models of a workflow, one per model with a remote jobmanager."""
        workflow = self.backend.getWorkflow(wid, version)
        return [m.Jobmanager for m in workflow.Models if m.Jobmanager]

    def schedulePass(self) -> int:
        """
        Take pending executions, oldest submitted first, while job slots and model-server slots allow.

        Model-server slots are budgeted for the pass (:class:`ResourceBudget`): an execution whose
        model servers are busy waits for a later pass without using up an attempt; one whose model
        servers (or workflow record) are unreachable uses up an attempt and is retried after a
        growing delay (:meth:`deferRetry`), and after ``max_attempts`` attempts it returns to
        ``Created``. Executions deferred after a transient failure are skipped until their delay
        has passed.

        :return: number of executions taken
        """
        free = self.freeSlots()
        if free <= 0:
            return 0
        pending = self.backend.pendingExecutions(limit=self.config.fetch_limit)
        budget = ResourceBudget(self._workflowJobmanagers, self.freeJobSlots)
        taken = 0
        for rec in pending:
            if taken >= free:
                break
            weid = rec.dbID
            with self.lock:
                if weid in self.jobs:
                    continue
            if self.deferred(weid):
                continue
            if rec.Attempts > self.config.max_attempts:
                log.warning(f'{weid}: resources unavailable for {rec.Attempts} attempts, returning it to Created')
                with self.lock:
                    self.backoff.pop(weid, None)
                try:
                    self.backend.setStatus(weid, 'Created')
                    self.backend.notifyOwner(weid)
                except Exception:
                    log.exception(f'{weid}: could not return it to Created')
                continue
            resources = budget.reserve(rec.WorkflowID, rec.WorkflowVersion)
            if resources == ResourceBudget.BUSY:
                # the model servers are working on other executions: wait without using up an attempt
                log.debug(f'{weid} waits for free model server slots')
                continue
            if resources == ResourceBudget.UNAVAILABLE:
                log.info(f'{weid} cannot be scheduled due to unavailable resources')
                try:
                    self.backend.setAttempts(weid, rec.Attempts + 1)
                except Exception:
                    log.exception(f'{weid}: could not increase the attempt count')
                self.deferRetry(weid)
                continue
            # a job thread only runs Scheduled executions: one whose status could not be changed
            # stays Pending and is considered again in the next pass
            try:
                self.backend.setStatus(weid, 'Scheduled')
            except Exception:
                log.exception(f'Could not set {weid} to Scheduled; not taking it')
                continue
            job = Job(weid=weid, record=rec)
            with self.lock:
                self.jobs[weid] = job
            self.monitor.enqueued(weid)
            self.executor.submit(self.runJob, job)
            taken += 1
            log.info(f'{weid} taken (attempts so far: {rec.Attempts})')
        return taken

    # --- one execution (job thread) -------------------------------------------------------------

    def runJob(self, job: Job) -> None:
        """
        Run one taken execution to its end and store the outcome; never raises.

        Errors are handled here (see the module documentation for the resulting statuses): before
        the workflow subprocess is launched the execution returns to ``Pending`` (``Failed`` if its
        workflow package cannot be used), after the launch it fails.

        :param job: the execution, set to ``Scheduled`` by :meth:`schedulePass`
        """
        weid = job.weid
        launched = False
        try:
            rec = self.backend.getExecution(weid)
            if rec.Status != 'Scheduled':
                raise NotScheduledError(f'{weid} is {rec.Status}, not Scheduled')
            if job.stopRequested.is_set():
                # shutting down before the workflow started: no point in starting it
                self.backend.setStatus(weid, 'Pending', keepAttempts=True)
                self.monitor.dropped(weid)
                return
            workflow = self.backend.getWorkflow(rec.WorkflowID, rec.WorkflowVersion)
            self.checkInputs(weid)

            def onLaunch():
                nonlocal launched
                self.backend.setStatus(weid, 'Running')
                self.backend.setAttempts(weid, rec.Attempts + 1)
                launched = True
                self.monitor.started(weid, rec.WorkflowID)

            result = self.runner.run(job, workflow, onLaunch=onLaunch, storeLog=lambda path: self.backend.storeLog(weid, path),
                                     timeout=self.jobTimeout(workflow))
            self.terminateLeftoverModelJobs(weid, workflow)
            log.warning(f'Workflow execution {weid} {result.status}' + (f' ({result.stopReason})' if result.stopReason else ''))
            if result.status == 'Pending' and result.stopReason is None:
                # no free model-server slot when the workflow started: retry later, not at once
                self.deferRetry(weid)
            elif result.status != 'Pending':
                with self.lock:
                    self.backoff.pop(weid, None)
            self.backend.setStatus(weid, result.status, keepAttempts=(result.status == 'Pending'))
            if result.status != 'Pending':
                self.backend.notifyOwner(weid)
            self.monitor.finished(weid, result.status)
        except NotScheduledError:
            log.exception(f'Execution {weid} is not Scheduled any more, not running it.')
            self.monitor.dropped(weid)
        except InputsIncompleteError as e:
            log.warning(f'Execution {weid} failed: {e}')
            self.storeMessageLog(weid, f'The execution was not started: {e}')
            self._recover(weid, launched, permanent=True)
        except Exception as e:
            log.exception(f'Execution of workflow {weid} failed.')
            self._recover(weid, launched, permanent=isinstance(e, StagingError))
        finally:
            with self.lock:
                self.jobs.pop(weid, None)

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

    def storeMessageLog(self, weid: str, message: str) -> None:
        """
        Store a short message as the log of an execution that did not run, so that its owner sees why.

        :param weid: execution ID
        :param message: text of the log
        """
        import tempfile
        from pathlib import Path
        try:
            with tempfile.TemporaryDirectory(dir=self.config.work_dir, prefix='mupifDB') as tmp:
                path = Path(tmp) / 'scheduler.log'
                path.write_text(message + '\n')
                self.backend.storeLog(weid, path)
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

    def _recover(self, weid: str, launched: bool, permanent: bool) -> None:
        """
        Set the status of an execution whose job failed with an exception.

        :param weid: execution ID
        :param launched: whether its workflow subprocess was started
        :param permanent: the error will not go away by retrying (unusable workflow package)
        """
        try:
            if launched or permanent:
                self.backend.setStatus(weid, 'Failed')
                self.backend.notifyOwner(weid)
            else:
                self.deferRetry(weid)
                self.backend.setStatus(weid, 'Pending', keepAttempts=True)
        except Exception:
            # REST API unreachable: nothing more can be done from here, the record is recovered
            # when the scheduler restarts (interrupted Scheduled/Running executions)
            log.exception(f'Could not update status of execution {weid} after the failure.')
        if launched:
            self.monitor.finished(weid, 'Failed')
        else:
            self.monitor.dropped(weid)

    def terminateLeftoverModelJobs(self, weid: str, workflow: Any = None) -> None:
        """
        Terminate model-server jobs still allocated for an execution whose workflow process has ended.

        Normally there are none: the execution script terminates its models. A script that was
        killed (or stopped while blocked in a call to a model) leaves them running, occupying slots
        on the model servers (finding #17). Best effort: errors are logged.

        :param weid: execution ID
        :param workflow: workflow record of the execution (read if not given)
        """
        try:
            rec = self.backend.getExecution(weid)
            loggerURI = getattr(rec, 'loggerURI', None)
            if not loggerURI:
                return
            if workflow is None:
                workflow = self.backend.getWorkflow(rec.WorkflowID, rec.WorkflowVersion)
            jobmanagers = [m.Jobmanager for m in workflow.Models if m.Jobmanager]
            if jobmanagers and (terminated := self.terminateModelJobs(jobmanagers, loggerURI)):
                log.warning(f'{weid}: terminated model-server jobs left by its workflow: {terminated}')
        except Exception:
            log.exception(f'{weid}: could not clean up its model-server jobs')

    # --- startup --------------------------------------------------------------------------------

    def recoverInterruptedExecutions(self) -> None:
        """
        Return executions left ``Scheduled`` or ``Running`` by a previous scheduler run to ``Pending``.

        Executions in these states belong to the scheduler (there is a single scheduler per
        deployment), so at startup they can only be leftovers of a previous run that crashed or was
        killed. They are returned to ``Pending`` (keeping their attempt count) and go through the
        normal scheduling; the model-server jobs of ``Running`` ones are terminated first. An
        execution whose workflow process is still alive on this host is left alone: it outlived the
        previous scheduler and finishes on its own.
        """
        log.info('Recovering executions interrupted by a previous scheduler run…')
        alive = runningExecutionProcesses()
        for status in ('Scheduled', 'Running'):
            try:
                records: list[Any] = self.backend.executionsWithStatus(status, RECOVERY_LIMIT)
            except Exception:
                log.exception(f'Could not list {status} executions to recover')
                continue
            for rec in records:
                if rec.dbID in alive:
                    log.info(f'{rec.dbID} is {status}, its workflow process is still running; left alone')
                    continue
                log.warning(f'{rec.dbID} was left {status} by a previous scheduler run; returning it to Pending')
                if status == 'Running':
                    self.terminateLeftoverModelJobs(rec.dbID)
                try:
                    self.backend.setStatus(rec.dbID, 'Pending', keepAttempts=True)
                except Exception:
                    log.exception(f'Could not return {rec.dbID} to Pending')
