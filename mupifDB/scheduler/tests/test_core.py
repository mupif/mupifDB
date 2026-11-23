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
from mupifDB.scheduler.tests.fakes import FakeBackend, FakeModelServers


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
        self.timeouts = {}
        self.onRun = None
        self.gate = None
        self.launched = []
        self.logFile = logFile
        self.lock = threading.Lock()

    def run(self, job, workflow, onLaunch, storeLog, timeout=0):
        """Behave like JobRunner.run for the configured outcome."""
        self.timeouts[job.weid] = timeout
        if self.onRun is not None:
            self.onRun(job)
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
    ``CVUT.demo01``), ``runner``, ``servers`` (FakeModelServers with ``CVUT.demo01`` registered),
    ``freeSlots`` (``servers.free``: server -> slots, 100 by default, None: unreachable), ``monitor``,
    ``cleanups`` (calls to terminate model-server jobs) and ``scheduler`` (``max_jobs=3``), plus
    ``drain()`` waiting for all job threads.
    """
    from mupifDB import restApiControl
    monkeypatch.setattr(restApiControl, 'setStatScheduler', lambda **kw: None)
    log = tmp_path / 'workflow.log'
    log.write_bytes(b'workflow output')
    e = types.SimpleNamespace(backend=FakeBackend(), runner=FakeRunner(log), servers=FakeModelServers())
    e.servers.add('CVUT.demo01')
    e.freeSlots = e.servers.free
    e.backend.addWorkflow('wf', 1, jobmanagers=('CVUT.demo01',))
    config = SchedulerConfig(max_jobs=3, poll_interval=0.05, work_dir=str(tmp_path), max_attempts=12)
    e.monitor = SchedulerMonitor(None, capacity=config.max_jobs)
    e.cleanups = []
    e.removed = []
    e.scheduler = Scheduler(config, e.backend, e.runner, findModelServers=e.servers.find, serverLoad=e.servers.load, monitor=e.monitor,
                            removeModelServer=lambda name, uri: e.removed.append((name, uri)) or True,
                            terminateModelJobs=lambda jobmanagers, uri: e.cleanups.append((jobmanagers, uri)) or ['1@x'])

    def drain():
        if e.runner.gate is not None:
            e.runner.gate.set()
        e.scheduler.executor.shutdown(wait=True)
        e.scheduler.notifier.shutdown(wait=True)
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


def test_leftover_model_jobs_are_terminated(env):
    """
    After the workflow process ends, model-server jobs logging to the execution's loggerURI are terminated.

    Finding #17: a killed workflow left its model jobs running, occupying model-server slots
    indefinitely. Without a loggerURI (the script died before storing it) there is nothing to match.
    """
    env.backend.addExecution('E0', loggerURI='PYRO:log-E0')
    env.backend.addExecution('E1')
    env.scheduler.schedulePass()
    env.drain()
    assert env.cleanups == [(['CVUT.demo01'], 'PYRO:log-E0')]


def test_recovery_terminates_model_jobs_of_running_executions(env, monkeypatch):
    """Executions left Running by a killed scheduler get their model-server jobs terminated before being requeued."""
    monkeypatch.setattr(core, 'runningExecutionProcesses', lambda: set())
    env.backend.addExecution('R1', status='Running', loggerURI='PYRO:log-R1')
    env.backend.addExecution('S1', status='Scheduled', loggerURI='PYRO:log-S1')
    env.scheduler.recoverInterruptedExecutions()
    assert env.cleanups == [(['CVUT.demo01'], 'PYRO:log-R1')]


@pytest.mark.parametrize('workflowTimeout,default,expected', [(30, 0, 30.), (0, 600, 600.), (0, 0, 0.), (30, 600, 30.)])
def test_job_timeout(env, workflowTimeout, default, expected):
    """
    An execution may run for the workflow's own Timeout, else for default_job_timeout (0: no limit) [S2].

    The Timeout of the workflow metadata used to be ignored.
    """
    env.scheduler.config.default_job_timeout = default
    env.backend.workflows[('wf', 1)].Timeout = workflowTimeout
    env.backend.addExecution('E0')
    env.scheduler.schedulePass()
    env.drain()
    assert env.runner.timeouts['E0'] == expected


def test_incomplete_inputs_fail_before_launch(env):
    """
    An execution whose compulsory inputs are not set fails without running, with a log saying why [S3].

    It used to be started and failed inside the workflow, with a traceback in the log.
    """
    env.backend.addExecution('E0')
    env.backend.incomplete.add('E0')
    env.scheduler.schedulePass()
    env.drain()
    assert env.backend.statuses('E0') == ['Scheduled', 'Failed'] and env.runner.launched == []
    assert b'compulsory inputs are not set' in env.backend.logs['E0'] and env.backend.notified == ['E0']


def test_input_check_failure_does_not_block(env):
    """If the input check itself fails (e.g. REST API briefly unreachable), the execution runs anyway."""
    env.backend.addExecution('E0')
    env.backend.failures['inputsComplete'] = lambda weid: RuntimeError('timeout')
    env.scheduler.schedulePass()
    env.drain()
    assert env.backend.status('E0') == 'Finished'


@pytest.fixture
def clock(monkeypatch):
    """Control the scheduler's monotonic clock: ``clock[0]`` is the current time in seconds."""
    now = [1000.]
    monkeypatch.setattr(core.time, 'monotonic', lambda: now[0])
    return now


def test_backoff_grows_and_is_capped(env, clock):
    """Delays after consecutive transient failures double from retry_backoff up to retry_backoff_max [S4]."""
    env.scheduler.config.retry_jitter = 0
    delays = [env.scheduler.deferRetry('E0') for _ in range(9)]
    assert delays == [10, 20, 40, 80, 160, 320, 600, 600, 600]


def test_unreachable_model_server_is_retried_later(env, clock):
    """
    An execution whose model server is unreachable is not tried again before its delay has passed [S4].

    It used to be tried at every pass (every 20 s), so a model server down for a few minutes
    exhausted the attempt limit of its executions.
    """
    env.scheduler.config.retry_jitter = 0
    env.backend.addExecution('E0')
    env.freeSlots['CVUT.demo01'] = None
    env.scheduler.schedulePass()
    env.scheduler.schedulePass()
    assert env.backend.executions['E0'].Attempts == 1            # second pass skipped it
    clock[0] += 11
    env.scheduler.schedulePass()
    assert env.backend.executions['E0'].Attempts == 2


def test_no_slot_at_start_is_retried_later(env, clock):
    """An execution returned to Pending because no model-server slot was free at its start is deferred."""
    env.backend.addExecution('E0')
    env.runner.outcomes['E0'] = 'Pending'
    env.scheduler.schedulePass()
    env.drain()
    assert env.scheduler.deferred('E0')
    env.scheduler.executor = core.concurrent.futures.ThreadPoolExecutor(max_workers=3)
    assert env.scheduler.schedulePass() == 0


def test_busy_model_server_is_not_deferred(env, clock):
    """Waiting for a busy model server is normal queueing: no delay, the execution keeps its place."""
    env.backend.addExecution('E0')
    env.freeSlots['CVUT.demo01'] = 0
    env.scheduler.schedulePass()
    assert not env.scheduler.deferred('E0') and env.backend.executions['E0'].Attempts == 0


def test_final_outcome_clears_the_delay(env, clock):
    """Once an execution reaches a final status, its retry delay is forgotten."""
    env.backend.addExecution('E0')
    env.scheduler.backoff['E0'] = (0., 3)
    env.scheduler.schedulePass()
    env.drain()
    assert env.backend.status('E0') == 'Finished' and 'E0' not in env.scheduler.backoff


def test_drain_mode(env):
    """While draining, no executions are taken; running ones finish; resuming takes executions again [S12]."""
    env.backend.addExecution('E0')
    env.backend.addExecution('E1')
    env.runner.gate = threading.Event()
    env.scheduler.config.max_jobs = 3
    assert env.scheduler.schedulePass() == 2
    env.backend.addExecution('E2')
    env.scheduler.setDrain(True)
    assert env.scheduler.schedulePass() == 0 and env.monitor.getStatistics()['draining'] is True
    env.runner.gate.set()
    for _ in range(100):
        if not env.scheduler.jobs:
            break
        time.sleep(0.02)
    assert env.backend.status('E0') == 'Finished' and env.backend.status('E2') == 'Pending'
    env.scheduler.setDrain(False)
    assert env.scheduler.schedulePass() == 1


def test_log_records_carry_the_execution(env, caplog):
    """Log records written while a job runs carry its execution ID (``record.weid``) and the job thread is named after it [S12]."""
    import logging
    env.backend.addExecution('E0')
    names = []
    env.runner.onRun = lambda job: names.append(threading.current_thread().name)
    with caplog.at_level(logging.INFO, logger='scheduler'):
        env.scheduler.schedulePass()
        env.drain()
    tagged = [r for r in caplog.records if r.name == 'scheduler' and getattr(r, 'weid', None) == 'E0']
    assert tagged and any('Finished' in r.getMessage() for r in tagged)
    assert names == ['job:E0']
    # the main loop tags its line about the execution explicitly (with its profile and model servers) ...
    assert [r.weid for r in caplog.records if 'taken' in r.getMessage()] == ['E0']
    # ... but the job thread's execution does not leak into the main loop's context
    assert core.currentExecution.get() is None


def test_notifications_do_not_hold_job_slots(env):
    """
    A slow notification (e-mail) does not keep the job slot busy; shutdown waits for it [S12].

    The owner was notified from the job thread, through an SMTP connection without a timeout.
    """
    env.backend.addExecution('E0')
    sent = threading.Event()
    original = env.backend.notifyOwner

    def slowNotify(weid):
        time.sleep(1)
        original(weid)
        sent.set()
    env.backend.notifyOwner = slowNotify
    env.scheduler.schedulePass()
    for _ in range(100):
        if not env.scheduler.jobs:
            break
        time.sleep(0.02)
    assert not env.scheduler.jobs and env.backend.status('E0') == 'Finished' and not sent.is_set()
    env.scheduler.shutdown()
    assert sent.is_set() and env.backend.notified == ['E0']


def test_notification_errors_are_logged(env, caplog):
    """A failing notification is logged; the execution's outcome is not affected."""
    env.backend.addExecution('E0')

    def fail(weid):
        raise OSError('SMTP server unreachable')
    env.backend.notifyOwner = fail
    env.scheduler.schedulePass()
    env.drain()
    assert env.backend.status('E0') == 'Finished' and 'could not notify the owner' in caplog.text


def test_statistics_report_running_and_waiting(env):
    """
    The statistics tell how many executions run and how many wait (database queue + taken, not started).

    The database count of Running executions is reported next to the scheduler's own count; here
    one Running record is not a process of this scheduler (e.g. it outlived a previous run).
    """
    for i in range(5):
        env.backend.addExecution(f'E{i}')
    env.backend.addExecution('X', status='Running')
    env.runner.gate = threading.Event()
    env.scheduler.schedulePass()                        # takes 3 (max_jobs), 2 stay Pending
    for _ in range(100):
        if env.monitor.stat.tasks.running == 3:
            break
        time.sleep(0.02)
    env.scheduler.sampleQueue()
    t = env.monitor.stat.tasks
    assert (t.running, t.pending, t.waiting, t.dbRunning) == (3, 2, 2, 4)
    assert env.monitor.stat.hist2h.running[-1] == 3 and env.monitor.stat.hist48h.waiting[-1] >= 2
    env.drain()


def test_queue_sampling_failure_is_not_fatal(env, caplog):
    """If the executions cannot be counted, the statistics keep the previous numbers and nothing raises."""
    env.backend.failures['statusCounts'] = lambda: RuntimeError('API down')
    env.scheduler.sampleQueue()
    assert env.monitor.stat.tasks.dbRunning is None
    assert 'Could not count the executions' in caplog.text


# --- several model servers, execution profiles -----------------------------------------------------

def _waitLaunched(env, weid, timeout=5):
    """Wait until the job thread of *weid* has launched its (fake) workflow."""
    t0 = time.time()
    while weid not in env.runner.launched:
        assert time.time() - t0 < timeout, f'{weid} not launched'
        time.sleep(0.01)


def test_instances_add_capacity(env):
    """Executions are admitted up to the free slots of all instances of the model server together."""
    env.freeSlots['CVUT.demo01'] = 1
    env.servers.add('CVUT.demo01~node2', base='CVUT.demo01', free=1)
    for i in range(3):
        env.backend.addExecution(f'E{i}')
    assert env.scheduler.schedulePass() == 2
    env.drain()
    assert env.backend.status('E2') == 'Pending'


def test_unknown_profile_fails(env):
    """An execution asking for an execution profile its workflow does not have fails at once, with a log telling why."""
    env.backend.addExecution('E0', profile=2)
    assert env.scheduler.schedulePass() == 0
    env.drain()
    assert env.backend.statuses('E0') == ['Failed']
    assert b'Execution profile #2' in env.backend.logs['E0']
    assert env.backend.notified == ['E0']


def test_profile_selects_servers(env, caplog):
    """The execution's profile decides the compatible servers; the scheduler log names the profile."""
    import logging
    env.backend.addWorkflow('wfp', 1, jobmanagers=('CVUT.demo01',),
                            profiles=[{'Name': 'fast', 'Models': [{'Name': 'm0', 'RequiredModelMetadata': ['Runtime_seconds']}]}])
    env.backend.addExecution('E0', wid='wfp', profile=0)
    env.backend.addExecution('E1', wid='wfp')
    with caplog.at_level(logging.INFO, logger='scheduler'):
        # no server is tagged Runtime_seconds: E0 cannot be placed (costs an attempt), E1 (default profile) can
        assert env.scheduler.schedulePass() == 1
    assert env.backend.executions['E0'].Attempts == 1
    assert 'E1 taken (attempts so far: 0; profile default; m0: 1 compatible model server(s))' in caplog.text
    env.servers.add('CVUT.demo01~fast', base='CVUT.demo01', tags=('Runtime_seconds',))
    env.scheduler.backoff.clear()
    with caplog.at_level(logging.INFO, logger='scheduler'):
        assert env.scheduler.schedulePass() == 1
    env.drain()
    assert 'E0 taken (attempts so far: 1; profile #0 "fast"; m0: 1 compatible model server(s))' in caplog.text


def test_model_servers_in_statistics(env, caplog):
    """
    The model servers the workflow chose (stored in the record by the execution script) appear in
    the log, with the execution in the statistics, and in the running executions per model server.
    """
    import logging
    env.servers.add('CVUT.demo01~node2', base='CVUT.demo01')
    env.backend.addExecution('E0')
    env.backend.addExecution('E1')
    env.runner.gate = threading.Event()
    env.scheduler.schedulePass()
    _waitLaunched(env, 'E0')
    _waitLaunched(env, 'E1')
    # as the execution script does once the models are allocated
    env.backend.setModelServers('E0', {'m0': 'CVUT.demo01~node2'})
    with caplog.at_level(logging.INFO, logger='scheduler'):
        env.scheduler.refreshPlacements()
        env.scheduler.refreshPlacements()   # known already: not counted twice
    assert 'E0: models allocated on model servers m0→CVUT.demo01~node2' in caplog.text
    st = env.monitor.getStatistics()
    assert st['modelServerJobs'] == {'CVUT.demo01~node2': 1}
    lastJobs = {j[0]: j for j in st['lastJobs']}
    assert lastJobs['E0'][5] == {'m0': 'CVUT.demo01~node2'} and lastJobs['E1'][5] == {}
    # E1 stores its servers just before it ends: they are shown with it, nothing stays counted
    env.backend.setModelServers('E1', {'m0': 'CVUT.demo01'})
    env.drain()
    st = env.monitor.getStatistics()
    assert st['modelServerJobs'] == {}
    assert {j[0]: j[5] for j in st['lastJobs']} == {'E0': {'m0': 'CVUT.demo01~node2'}, 'E1': {'m0': 'CVUT.demo01'}}


def test_leftover_jobs_on_servers_used(env):
    """Leftover model jobs are looked for on the servers the execution used, if known, else on every compatible one."""
    env.servers.add('CVUT.demo01~node2', base='CVUT.demo01')
    env.backend.addExecution('E0', loggerURI='PYRO:log-E0')
    env.backend.addExecution('E1', loggerURI='PYRO:log-E1')
    env.runner.gate = threading.Event()
    env.scheduler.schedulePass()
    _waitLaunched(env, 'E0')
    # as the execution script does once the models are allocated (E1's script died before)
    env.backend.setModelServers('E0', {'m0': 'CVUT.demo01~node2'})
    env.drain()
    assert sorted(env.cleanups) == [(['CVUT.demo01', 'CVUT.demo01~node2'], 'PYRO:log-E1'), (['CVUT.demo01~node2'], 'PYRO:log-E0')]


def test_leftover_jobs_of_templates(env):
    """
    Copies of a template (Instantiate false) are allocated by the workflow after the model servers
    were stored: leftover jobs are also looked for on every server compatible with the template.
    """
    env.servers.add('pre')
    env.servers.add('post')
    env.servers.add('post~2', base='post')
    env.backend.addWorkflow('wft', 1, jobmanagers=('pre', 'post'), templates=('m1',))
    env.backend.addExecution('E0', wid='wft', loggerURI='PYRO:log-E0')
    env.runner.gate = threading.Event()
    assert env.scheduler.schedulePass() == 1
    _waitLaunched(env, 'E0')
    # the script stores the servers of the models allocated at initialization: m0 only
    env.backend.setModelServers('E0', {'m0': 'pre'})
    env.drain()
    assert env.cleanups == [(['post', 'post~2', 'pre'], 'PYRO:log-E0')]


def test_in_flight_executions_hold_their_slots(env):
    """
    Executions started but whose workflows have not allocated their models yet keep their slots in the next pass.

    The model servers count a slot only once the model is allocated; the scheduler took more
    executions in between, which then found no free slot and were requeued (exit code 2).
    """
    env.freeSlots['CVUT.demo01'] = 2
    for i in range(4):
        env.backend.addExecution(f'E{i}')
    env.runner.gate = threading.Event()
    assert env.scheduler.schedulePass() == 2
    _waitLaunched(env, 'E0')
    _waitLaunched(env, 'E1')
    # the model server still reports 2 free slots: the workflows have not allocated their models
    assert env.scheduler.schedulePass() == 0
    # E0 allocated its model (the server counts it now: 1 free); E1 is still in flight
    env.backend.setModelServers('E0', {'m0': 'CVUT.demo01'})
    env.freeSlots['CVUT.demo01'] = 1
    env.scheduler.refreshPlacements()
    assert env.scheduler.schedulePass() == 0
    # E1's workflow is initialized but does not report model servers (older mupif): no longer in flight
    env.backend.executions['E1'].workflowURI = 'PYRO:wf-E1'
    env.freeSlots['CVUT.demo01'] = 0
    env.scheduler.refreshPlacements()
    assert [j.allocated for j in env.scheduler.jobs.values()] == [True, True]
    env.freeSlots['CVUT.demo01'] = 1
    assert env.scheduler.schedulePass() == 1


def test_restarted_execution_listed_once(env):
    """An execution returned to the queue and started again appears once among the last executions, with its final status."""
    env.backend.addExecution('E0')
    env.runner.outcomes['E0'] = 'Pending'
    env.scheduler.schedulePass()
    while env.scheduler.jobs:
        time.sleep(0.01)
    env.runner.outcomes['E0'] = 'Finished'
    env.scheduler.backoff.clear()
    env.scheduler.schedulePass()
    env.drain()
    assert [(j[0], j[2]) for j in env.monitor.getStatistics()['lastJobs']] == [('E0', 'Finished')]


def test_unreachable_server_removed_after_timeout(env, clock, caplog):
    """
    A model server unreachable for stale_server_timeout is removed from the nameserver, reported
    once while waiting; one answering again is forgotten.

    A crashed instance used to stay in the nameserver for ever, with a warning every pass.
    """
    import logging
    env.scheduler.config.stale_server_timeout = 300
    env.servers.add('CVUT.demo01~gone', base='CVUT.demo01', free=None)
    env.servers.add('CVUT.demo01~flaky', base='CVUT.demo01', free=None)
    found = {c.nsName: c for c in env.servers.find(core.modelplacement.ModelRequirement(model='m', identity=frozenset({'CVUT.demo01'})))}
    gone, flaky = found['CVUT.demo01~gone'], found['CVUT.demo01~flaky']
    with caplog.at_level(logging.INFO, logger='scheduler'):
        for t in (0, 100, 200):
            clock[0] = 1000 + t
            assert env.scheduler.checkedServerLoad(gone) is None
            assert env.scheduler.checkedServerLoad(flaky) is None
        # reported once, not every pass
        assert caplog.text.count('Model server CVUT.demo01~gone') == 1
        env.servers.free['CVUT.demo01~flaky'] = 2
        assert env.scheduler.checkedServerLoad(flaky).free == 2
        assert 'Model server CVUT.demo01~flaky answers again' in caplog.text
        clock[0] = 1000 + 300
        env.scheduler.checkedServerLoad(gone)
        env.servers.free['CVUT.demo01~flaky'] = None
        env.scheduler.checkedServerLoad(flaky)
    # removed after 300 s unreachable; the flaky one's time starts again when it failed again
    assert env.removed == [('CVUT.demo01~gone', 'PYRO:CVUT.demo01~gone@host:1')]
    assert 'Removed model server CVUT.demo01~gone from the nameserver: unreachable for 300 s' in caplog.text
    # stale_server_timeout 0: never removed
    env.scheduler.config.stale_server_timeout = 0
    clock[0] = 1000 + 10000
    env.scheduler.checkedServerLoad(flaky)
    assert len(env.removed) == 1


def test_passes_use_the_checked_load(env, clock):
    """The budget of a pass asks for the loads through checkedServerLoad, so a dead server in the nameserver is eventually removed."""
    env.scheduler.config.stale_server_timeout = 60
    env.servers.add('CVUT.demo01~gone', base='CVUT.demo01', free=None)
    env.backend.addExecution('E0')
    env.scheduler.schedulePass()
    clock[0] += 61
    env.backend.addExecution('E1')
    env.scheduler.schedulePass()
    env.drain()
    assert [n for n, _ in env.removed] == ['CVUT.demo01~gone']
    assert env.backend.status('E0') == env.backend.status('E1') == 'Finished'
