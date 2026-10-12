"""
Running one execution: working directory, workflow subprocess, log, outcome.

The scheduler runs each execution in a job thread, which calls :meth:`JobRunner.run`. The runner
creates a temporary working directory, stages the workflow package and the execution script into
it, starts ``workflow_execution_script.py -eid <ID>`` as a subprocess and waits for it, writing the
subprocess output to the execution log. The exit code of the script (its contract with the
scheduler, see :func:`statusForExitCode`) decides the outcome.

Each subprocess runs in its own session (``start_new_session``), i.e. its own process group: the
scheduler can stop a workflow together with every process it started, and a signal sent to the
scheduler's terminal or process group (e.g. Ctrl-C) does not reach the workflows directly.
"""

import datetime
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import textwrap
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal

from .config import SchedulerConfig
from .staging import stageWorkflow

log = logging.getLogger('scheduler')

#: directory of the mupifDB package and its parent (added to PYTHONPATH of the subprocess)
_MUPIFDB_DIR = Path(__file__).resolve().parent.parent
_MUPIFDB_SRC_DIR = _MUPIFDB_DIR.parent

#: the script run for every execution
EXECUTION_SCRIPT = _MUPIFDB_DIR / 'workflow_execution_script.py'

#: final statuses an execution can get from a run (Pending: returned to the queue)
Outcome = Literal['Finished', 'Failed', 'Pending']


def statusForExitCode(exitCode: int) -> Outcome:
    """
    Map the exit code of ``workflow_execution_script.py`` to the execution status to set.

    The script exits with 0 when the workflow finished, 1 when it failed and 2 when a model
    server had no free resources (see its module documentation). Every other code -- notably the
    negative codes of a process killed by a signal (OOM killer, manual kill), or an interpreter
    crash -- also means the workflow did not finish and is reported as ``Failed``, so that no
    execution is ever left in ``Running``.

    :param exitCode: return code of the execution subprocess (negative: killed by that signal)
    :return: ``'Finished'``, ``'Failed'`` or ``'Pending'`` (retry later, the attempt count is kept)
    """
    if exitCode == 0:
        return 'Finished'
    if exitCode == 2:
        return 'Pending'
    return 'Failed'


@dataclass
class Job:
    """
    One execution taken by the scheduler, from its claim until its outcome is stored.

    Shared between the job thread running it and the main thread (which may stop it); the
    attributes set by the job thread are only read by others.
    """

    #: execution ID
    weid: str
    #: execution record as read when the execution was taken
    record: Any
    #: workflow subprocess, once started
    process: subprocess.Popen | None = None
    #: reason why the scheduler stops the job (``'shutdown'``, ...), None while it may run
    stopReason: str | None = None
    #: set when the job has to be stopped; wakes the waiting job thread
    stopRequested: threading.Event = field(default_factory=threading.Event)

    def stop(self, reason: str) -> None:
        """
        Ask the job to stop; its runner terminates the workflow subprocess.

        :param reason: recorded in the log and deciding the outcome (see :meth:`JobRunner.run`)
        """
        if self.stopReason is None:
            self.stopReason = reason
        self.stopRequested.set()


@dataclass
class JobResult:
    """Outcome of :meth:`JobRunner.run`."""

    #: status to set for the execution
    status: Outcome
    #: exit code of the workflow subprocess (negative: killed by that signal)
    exitCode: int
    #: why the job was stopped by the scheduler, None if the subprocess ended by itself
    stopReason: str | None = None


def killProcessGroup(process: subprocess.Popen, grace: float) -> None:
    """
    Terminate a subprocess started in its own session, together with all processes it started.

    The process group gets SIGTERM, and SIGKILL if the subprocess is still running after *grace*
    seconds.

    :param process: subprocess started with ``start_new_session=True``
    :param grace: seconds between SIGTERM and SIGKILL
    """
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            return
        try:
            process.wait(timeout=grace)
            # the subprocess has ended; make sure nothing it started survives it
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            return
        except subprocess.TimeoutExpired:
            continue


class JobRunner:
    """Run executions as ``workflow_execution_script.py`` subprocesses (see the module documentation)."""

    def __init__(self, config: SchedulerConfig, nsUri: str, stage: Callable[[Any, Path], Path] = stageWorkflow,
                 executionScript: Path = EXECUTION_SCRIPT, python: str = sys.executable):
        """
        Configure the runner.

        :param config: scheduler settings (``work_dir``, ``kill_grace``)
        :param nsUri: URI of the Pyro nameserver, passed to the subprocess as ``MUPIF_NS``
        :param stage: ``(workflow record, directory) -> module path``, see :func:`stageWorkflow`
        :param executionScript: script to run (replaced by tests)
        :param python: interpreter running the script; by default the scheduler's own
        """
        self.config = config
        self.nsUri = nsUri
        self.stage = stage
        self.executionScript = Path(executionScript)
        self.python = python

    def environment(self) -> dict[str, str]:
        """
        Build the environment of the workflow subprocess.

        :return: the scheduler's environment, with mupifDB on ``PYTHONPATH``, the nameserver in
                 ``MUPIF_NS`` and the REST API account in ``API_CREDENTIALS_FILE``
        """
        env = os.environ.copy()
        env['PYTHONPATH'] = os.pathsep.join(filter(None, [env.get('PYTHONPATH'), str(_MUPIFDB_SRC_DIR)]))
        env['MUPIF_NS'] = self.nsUri
        env['API_CREDENTIALS_FILE'] = os.environ.get('API_CREDENTIALS_FILE', 'None')
        return env

    def run(self, job: Job, workflowRecord: Any, onLaunch: Callable[[], None], storeLog: Callable[[Path], None]) -> JobResult:
        """
        Run an execution to its end.

        Exceptions raised before *onLaunch* is called (staging, ``onLaunch`` itself) propagate; the
        caller knows the workflow has not run. Once launched, the run always produces a
        :class:`JobResult`: if the job is stopped (:meth:`Job.stop`) the subprocess is terminated
        with its process group, and the outcome is ``Pending`` for reason ``'shutdown'`` (the
        execution is run again later) and ``Failed`` otherwise.

        :param job: the job; its ``process`` is set while the subprocess runs
        :param workflowRecord: record of the workflow to run
        :param onLaunch: called just before the subprocess starts (sets the execution Running)
        :param storeLog: called with the log file when the subprocess has ended (log of the
                         subprocess with a header and a footer); errors are logged and ignored
        :return: outcome of the run
        :raises mupifDB.scheduler.staging.StagingError: if the workflow package cannot be used
        """
        os.makedirs(self.config.work_dir, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=self.config.work_dir, prefix='mupifDB') as tempDir:
            workDir = Path(tempDir)
            self.stage(workflowRecord, workDir)
            script = workDir / self.executionScript.name
            shutil.copy(self.executionScript, script)
            logFile = workDir / 'workflow.log'
            cmd = [self.python, str(script), '-eid', str(job.weid)]
            onLaunch()
            ll = 10 * '='
            with open(logFile, 'w') as out:
                t0 = datetime.datetime.now()
                out.write(textwrap.dedent(f'''
                    {ll} WORKFLOW STARTING at {t0.isoformat(timespec='seconds')} {ll}
                    {ll} command is {cmd} {ll}'''))
                out.flush()
                log.info(f'Executing {job.weid} in {workDir}')
                job.process = subprocess.Popen(cmd, cwd=workDir, stdout=out, stderr=subprocess.STDOUT, env=self.environment(), start_new_session=True)
                exitCode = self._wait(job)
                t1 = datetime.datetime.now()
                dt = t1 - t0
                stopped = f' (stopped by the scheduler: {job.stopReason})' if job.stopReason else ''
                out.write(textwrap.dedent(f'''
                    {ll} WORKFLOW FINISHED at {t1.isoformat(timespec='seconds')} {ll}
                    {ll} duration: {dt - datetime.timedelta(microseconds=dt.microseconds)} {ll}
                    {ll} exit status of {cmd}: {exitCode} ({'ERROR' if exitCode != 0 else 'SUCCESS'}){stopped} {ll}'''))
            log.info(f'{job.weid}: exit code {exitCode}{stopped}')
            try:
                storeLog(logFile)
            except Exception:
                log.exception(f'{job.weid}: could not store the execution log')
        if job.stopReason is not None:
            return JobResult(status='Pending' if job.stopReason == 'shutdown' else 'Failed', exitCode=exitCode, stopReason=job.stopReason)
        return JobResult(status=statusForExitCode(exitCode), exitCode=exitCode)

    def _wait(self, job: Job) -> int:
        """
        Wait for the subprocess of *job*, terminating it when the job is asked to stop.

        :param job: job whose subprocess is running
        :return: exit code of the subprocess
        """
        process = job.process
        assert process is not None
        while process.poll() is None:
            if job.stopRequested.wait(timeout=0.5):
                log.warning(f'{job.weid}: stopping the workflow ({job.stopReason})')
                killProcessGroup(process, self.config.kill_grace)
                break
        rc = process.wait()
        # the workflow may have left processes of its own (e.g. a model run locally): they are
        # part of its process group and must not outlive the execution
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        return rc

