"""
Tests of :class:`mupifDB.scheduler.core.Scheduler` with an in-memory database and a fake runner.

No REST API, nameserver, model server or subprocess is involved: :class:`FakeRunner` decides the
outcome of each execution, and can hold all jobs running (``gate``) to observe capacity limits.
"""

import subprocess
import sys
import threading
import time
import types

import pytest

from mupifDB.scheduler import core
from mupifDB.scheduler.config import SchedulerConfig
from mupifDB.scheduler.core import Scheduler
from mupifDB.scheduler.monitor import SchedulerMonitor
from mupifDB.scheduler.runner import JobResult
from mupifDB.scheduler.staging import StagingError
from mupifDB.scheduler.tests.fakes import FakeBackend


class FakeRunner:
    """
    Stand-in for :class:`~mupifDB.scheduler.runner.JobRunner`.

    ``outcomes`` maps an execution ID to ``'Finished'``/``'Failed'``/``'Pending'`` (default
    ``'Finished'``), to an exception raised before the launch, to ``('after', exception)`` raised
    after it, or to ``'untilStopped'`` (runs until the job is stopped). While ``gate`` is an unset
    Event, launched jobs wait for it.
    """

    def __init__(self, logFile):
        """Use *logFile* as the log of every run."""
        self.outcomes = {}
        self.gate = None
        self.launched = []
        self.logFile = logFile
        self.lock = threading.Lock()

    def run(self, job, workflow, onLaunch, storeLog):
        """Behave like JobRunner.run for the configured outcome."""
        outcome = self.outcomes.get(job.weid, 'Finished')
        if isinstance(outcome, Exception):
            raise outcome
        onLaunch()
        with self.lock:
            self.launched.append(job.weid)
        if outcome == 'untilStopped':
            # like a long workflow: runs until the scheduler stops it (JobRunner returns Pending on shutdown)
            assert job.stopRequested.wait(10)
            storeLog(self.logFile)
            return JobResult(status='Pending' if job.stopReason == 'shutdown' else 'Failed', exitCode=143, stopReason=job.stopReason)
        if self.gate is not None:
            assert self.gate.wait(10)
        if isinstance(outcome, tuple):
            raise outcome[1]
        storeLog(self.logFile)
        return JobResult(status=outcome, exitCode={'Finished': 0, 'Failed': 1, 'Pending': 2}[outcome])


@pytest.fixture
def env(tmp_path, monkeypatch):
    """
    Build a scheduler over an in-memory database.

    The namespace returned has ``backend`` (FakeBackend with workflow ``wf`` on jobmanager
    ``CVUT.demo01``), ``runner``, ``freeSlots`` (jobmanager -> slots, 100 by default), ``monitor`` and
    ``scheduler`` (``max_jobs=3``), plus ``drain()`` waiting for all job threads.
    """
    from mupifDB import restApiControl
    monkeypatch.setattr(restApiControl, 'setStatScheduler', lambda **kw: None)
    log = tmp_path / 'workflow.log'
    log.write_bytes(b'workflow output')
    e = types.SimpleNamespace(backend=FakeBackend(), runner=FakeRunner(log), freeSlots={})
    e.backend.addWorkflow('wf', 1, jobmanagers=('CVUT.demo01',))
    config = SchedulerConfig(max_jobs=3, poll_interval=0.05, work_dir=str(tmp_path), max_attempts=12)
    e.monitor = SchedulerMonitor(None, capacity=config.max_jobs)
    e.scheduler = Scheduler(config, e.backend, e.runner, freeJobSlots=lambda jm: e.freeSlots.get(jm, 100), monitor=e.monitor)

    def drain():
        if e.runner.gate is not None:
            e.runner.gate.set()
        e.scheduler.executor.shutdown(wait=True)
    e.drain = drain
    yield e
    drain()


def test_takes_oldest_first_within_capacity(env):
    """
    A pass takes at most ``max_jobs`` executions, oldest submitted first; the rest stay Pending.

    The forked pool used to accept up to 300 executions per pass and queue them behind its
    workers, already marked Scheduled.
    """
    for i in range(10):
        env.backend.addExecution(f'E{i}')
    env.runner.gate = threading.Event()
    assert env.scheduler.schedulePass() == 3
    assert env.scheduler.schedulePass() == 0            # no free job slot
    assert sorted(env.scheduler.jobs) == ['E0', 'E1', 'E2']
    assert [env.backend.status(f'E{i}') for i in range(3, 10)] == ['Pending'] * 7
    env.drain()
    assert [env.backend.statuses(f'E{i}') for i in range(3)] == [['Scheduled', 'Running', 'Finished']] * 3
    assert env.scheduler.freeSlots() == 3


def test_model_server_slots_limit_the_pass(env):
    """Only as many executions as the model server has free slots are taken; waiting costs no attempt."""
    for i in range(5):
        env.backend.addExecution(f'E{i}', attempts=1)
    env.freeSlots['CVUT.demo01'] = 2
    assert env.scheduler.schedulePass() == 2
    env.drain()
    assert [env.backend.executions[f'E{i}'].Attempts for i in range(2, 5)] == [1, 1, 1]


def test_unavailable_resources_use_attempts(env):
    """An unreachable model server costs an attempt; beyond max_attempts the execution returns to Created."""
    env.backend.addExecution('E0', attempts=3)
    env.backend.addExecution('E1', attempts=13)
    env.freeSlots['CVUT.demo01'] = None
    assert env.scheduler.schedulePass() == 0
    assert env.backend.executions['E0'].Attempts == 4
    assert env.backend.status('E1') == 'Created' and env.backend.notified == ['E1']


def test_failed_status_change_is_not_taken(env):
    """An execution whose change to Scheduled fails stays Pending and is not run (finding #5)."""
    env.backend.addExecution('E0')
    env.backend.addExecution('E1')
    env.backend.failures['setStatus'] = lambda weid, status: RuntimeError('REST API error') if (weid, status) == ('E0', 'Scheduled') else None
    assert env.scheduler.schedulePass() == 1
    env.drain()
    assert env.backend.status('E0') == 'Pending' and env.runner.launched == ['E1']


@pytest.mark.parametrize('outcome,statuses,attempts,notified', [
    ('Finished', ['Scheduled', 'Running', 'Finished'], 1, True),
    ('Failed', ['Scheduled', 'Running', 'Failed'], 1, True),
    ('Pending', ['Scheduled', 'Running', 'Pending'], 1, False),      # no resources: retried, attempt kept
])
def test_job_outcomes(env, outcome, statuses, attempts, notified):
    """The outcome of the run is stored with the log; the owner is notified of final statuses only."""
    env.backend.addExecution('E0')
    env.runner.outcomes['E0'] = outcome
    env.scheduler.schedulePass()
    env.drain()
    assert env.backend.statuses('E0') == statuses
    assert env.backend.executions['E0'].Attempts == attempts
    assert env.backend.logs['E0'] == b'workflow output'
    assert (env.backend.notified == ['E0']) == notified
    t = env.monitor.stat.tasks
    assert (t.scheduled, t.running, t.processed) == (0, 0, 0 if outcome == 'Pending' else 1)


def test_error_before_launch_requeues(env):
    """An error before the workflow starts returns the execution to Pending with its attempt count (finding #4)."""
    env.backend.addExecution('E0', attempts=2)
    env.runner.outcomes['E0'] = RuntimeError('REST API unreachable')
    env.scheduler.schedulePass()
    env.drain()
    assert env.backend.statuses('E0') == ['Scheduled', 'Pending']
    assert env.backend.executions['E0'].Attempts == 2
    assert env.monitor.stat.tasks.scheduled == 0


def test_unusable_workflow_package_fails(env):
    """A workflow package that cannot be staged fails the execution instead of retrying it forever."""
    env.backend.addExecution('E0')
    env.runner.outcomes['E0'] = StagingError('module missing')
    env.scheduler.schedulePass()
    env.drain()
    assert env.backend.statuses('E0') == ['Scheduled', 'Failed'] and env.backend.notified == ['E0']


def test_error_after_launch_fails(env):
    """An error once the workflow subprocess was launched fails the execution."""
    env.backend.addExecution('E0')
    env.runner.outcomes['E0'] = ('after', OSError('cannot execute'))
    env.scheduler.schedulePass()
    env.drain()
    assert env.backend.statuses('E0') == ['Scheduled', 'Running', 'Failed']
    t = env.monitor.stat.tasks
    assert (t.running, t.failed) == (0, 1)


def test_unscheduled_execution_left_alone(env):
    """An execution changed by someone else after it was taken is neither run nor modified."""
    rec = env.backend.addExecution('E0', status='Created')
    env.scheduler.jobs['E0'] = core.Job(weid='E0', record=rec)
    env.scheduler.runJob(env.scheduler.jobs['E0'])
    assert env.backend.statuses('E0') == [] and env.runner.launched == [] and env.scheduler.jobs == {}


def test_recover_interrupted_executions(env, monkeypatch):
    """At startup Scheduled/Running leftovers return to Pending (attempts kept), unless their process still runs."""
    env.backend.addExecution('S1', status='Scheduled', attempts=1)
    env.backend.addExecution('R1', status='Running', attempts=2)
    env.backend.addExecution('R2', status='Running', attempts=1)
    monkeypatch.setattr(core, 'runningExecutionProcesses', lambda: {'R2'})
    env.scheduler.recoverInterruptedExecutions()
    assert [env.backend.status(w) for w in ('S1', 'R1', 'R2')] == ['Pending', 'Pending', 'Running']
    assert env.backend.executions['R1'].Attempts == 2


def test_run_survives_errors_and_stops(env, monkeypatch):
    """The main loop continues after a failing pass (finding #16) and ends promptly on stop() (finding #1)."""
    monkeypatch.setattr(core, 'runningExecutionProcesses', lambda: set())
    env.backend.addExecution('E0')
    calls = []

    def firstPassFails(limit):
        calls.append(1)
        return RuntimeError('401 Unauthorized') if len(calls) == 1 else None
    env.backend.failures['pendingExecutions'] = firstPassFails
    t = threading.Thread(target=env.scheduler.run)
    t.start()
    for _ in range(100):
        if env.backend.status('E0') == 'Finished':
            break
        time.sleep(0.05)
    env.scheduler.stop()
    t.join(timeout=5)
    assert not t.is_alive()
    assert len(calls) >= 2 and env.backend.status('E0') == 'Finished'


def test_running_execution_processes(tmp_path):
    """The process scan finds an execution subprocess by its command line (``... workflow_execution_script.py -eid ID``)."""
    script = tmp_path / 'workflow_execution_script.py'
    script.write_text('import time\ntime.sleep(60)\n')
    proc = subprocess.Popen([sys.executable, str(script), '-eid', 'unit-test-weid'])
    try:
        for _ in range(50):
            if 'unit-test-weid' in core.runningExecutionProcesses():
                break
            time.sleep(0.1)
        assert 'unit-test-weid' in core.runningExecutionProcesses()
    finally:
        proc.kill()
        proc.wait()
    assert 'unit-test-weid' not in core.runningExecutionProcesses()


def test_shutdown_requeues_running_executions(env):
    """
    On shutdown, workflows still running after drain_timeout are stopped and their executions return to Pending.

    They keep their attempt count and run again after the restart (finding #11: the old scheduler
    waited for all workflows, possibly hours, unless killed, which left them running unattended).
    """
    env.scheduler.config.drain_timeout = 0.2
    env.backend.addExecution('E0')
    env.runner.outcomes['E0'] = 'untilStopped'
    env.scheduler.schedulePass()
    for _ in range(100):
        if env.runner.launched:
            break
        time.sleep(0.02)
    t0 = time.time()
    env.scheduler.shutdown()
    assert time.time() - t0 < 5
    assert env.backend.statuses('E0') == ['Scheduled', 'Running', 'Pending']
    assert env.backend.executions['E0'].Attempts == 1
    assert env.backend.notified == [] and env.scheduler.jobs == {}


def test_shutdown_lets_short_workflows_finish(env):
    """A workflow ending within drain_timeout finishes normally."""
    env.scheduler.config.drain_timeout = 5
    env.backend.addExecution('E0')
    env.runner.gate = threading.Event()
    env.scheduler.schedulePass()
    threading.Timer(0.3, env.runner.gate.set).start()
    env.scheduler.shutdown()
    assert env.backend.status('E0') == 'Finished'


def test_job_stopped_before_launch_is_not_started(env):
    """A taken execution whose job is stopped before its workflow starts returns to Pending without running."""
    rec = env.backend.addExecution('E0', status='Scheduled', attempts=2)
    job = core.Job(weid='E0', record=rec)
    job.stop('shutdown')
    env.scheduler.jobs['E0'] = job
    env.scheduler.runJob(job)
    assert env.runner.launched == [] and env.backend.statuses('E0') == ['Pending']
    assert env.backend.executions['E0'].Attempts == 2
