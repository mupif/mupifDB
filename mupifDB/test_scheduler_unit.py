"""
Unit tests of the workflow scheduler (:mod:`mupifDB.workflowscheduler`).

These tests need no running services: no MongoDB, REST API, Pyro nameserver or model server.
Calls to the REST API are replaced by fakes through ``monkeypatch``. End-to-end behaviour is
covered by ``test_scheduler.py`` (pytest + xprocess) and ``tools/scheduler_smoketest.py``
(real local deployment).

Each test names the finding of the refactoring plan it guards against, where applicable.
"""
import subprocess
import sys
import time
import types

import pytest


class _FakeNameserver:
    """Stands in for the Pyro nameserver proxy; records removed names."""

    def __init__(self):
        self.removed = []

    def remove(self, name):
        self.removed.append(name)


def test_stop_ends_main_loop(monkeypatch, tmp_path):
    """
    ``SchedulerMonitor.stop()`` must end the main loop promptly.

    Regression for finding #1: ``stop()`` assigned a local variable instead of the global flag, so
    the loop never ended. The stop must also interrupt the sleep between iterations, so the loop has
    to return well within one ``LOOP_SLEEP_SEC`` interval, without starting another iteration.
    """
    from mupifDB import workflowscheduler as ws
    monkeypatch.setattr(ws, 'LOOP_SLEEP_SEC', 3)
    monkeypatch.setattr(ws, 'schedulerStatFile', str(tmp_path / 'stat.json'))
    monkeypatch.setattr(ws, 'stopEvent', ws.threading.Event())
    fakeNs = _FakeNameserver()
    monitor = ws.SchedulerMonitor(fakeNs)
    iterations = []

    def schedulePending(pool):
        iterations.append(time.time())
        assert len(iterations) == 1, 'loop continued after stop()'
        if len(iterations) == 1:
            # request the stop from another thread while the loop is about to sleep
            ws.threading.Timer(0.2, monitor.stop).start()

    class _Monitor:
        def persistStat(self):
            pass

    monkeypatch.setattr(ws, 'scheduler_schedule_pending', schedulePending)
    t0 = time.time()
    ws.runLoop(pool=None, monitor=_Monitor())
    assert time.time() - t0 < 2
    assert fakeNs.removed == ['mupif.scheduler']


@pytest.mark.parametrize('exitCode,status', [(0, 'Finished'), (1, 'Failed'), (2, 'Pending'), (-9, 'Failed'), (-15, 'Failed'), (3, 'Failed'), (137, 'Failed')])
def test_exit_code_mapping(exitCode, status):
    """
    Every exit code of the execution subprocess maps to a status other than ``Running``.

    Regression for finding #3: codes other than 0, 1 and 2 (e.g. -9 when the workflow process is
    killed) were ignored, leaving the execution in ``Running`` forever.
    """
    from mupifDB import workflowscheduler as ws
    assert ws.statusForExitCode(exitCode) == status


# ---------------------------------------------------------------------------------------------
# scheduler pool worker (executeWorkflow): run in-process against fakes of the REST API, the
# statistics monitor, workflow staging and the workflow subprocess
# ---------------------------------------------------------------------------------------------

class _FakeMonitor:
    """Stands in for the Pyro proxy of SchedulerMonitor; statistics are not checked here."""

    def __getattr__(self, name):
        return lambda *args, **kw: None


@pytest.fixture
def worker(monkeypatch):
    """
    Return a namespace to drive :func:`mupifDB.workflowscheduler.executeWorkflow` with fakes.

    Attributes: ``record`` (execution record returned by the fake REST API, or an exception to
    raise), ``subprocessResult`` (exit code of the fake workflow subprocess, or an exception to
    raise) and ``statuses`` (list of ``(status, kwargs)`` set through the REST API).
    """
    from mupifDB import workflowscheduler as ws
    env = types.SimpleNamespace(statuses=[], subprocessResult=0,
                                record=types.SimpleNamespace(Status='Scheduled', WorkflowID='wf', WorkflowVersion=1, dbID='E1', Attempts=0))

    def getExecutionRecord(weid):
        if isinstance(env.record, Exception):
            raise env.record
        return env.record

    def call(*args, **kw):
        if isinstance(env.subprocessResult, Exception):
            raise env.subprocessResult
        return env.subprocessResult

    rc = ws.restApiControl
    monkeypatch.setattr(rc, 'getExecutionRecord', getExecutionRecord)
    monkeypatch.setattr(rc, 'getWorkflowRecord', lambda wid, version: types.SimpleNamespace(modulename='fakewf'))
    monkeypatch.setattr(rc, 'setExecutionStatus', lambda weid, status, **kw: env.statuses.append((status, kw)))
    monkeypatch.setattr(rc, 'setExecutionAttemptsCount', lambda weid, n: None)
    monkeypatch.setattr(ws.my_email, 'sendEmailAboutExecutionStatus', lambda weid: None)
    monkeypatch.setattr(ws, 'executeWorkflow_copyInputs', lambda *args: None)
    monkeypatch.setattr(ws, 'copyLogToDB', lambda *args: None)
    monkeypatch.setattr(ws.Pyro5.api, 'Proxy', lambda uri: _FakeMonitor())
    monkeypatch.setattr(ws.subprocess, 'call', call)
    monkeypatch.setattr(ws, 'ns_uri', 'PYRO:Pyro.NameServer@127.0.0.1:1')
    env.run = lambda: ws.executeWorkflow('E1')
    return env


def test_worker_success(worker):
    """A workflow subprocess exiting with 0 takes the execution Running -> Finished."""
    worker.run()
    assert [s for s, kw in worker.statuses] == ['Running', 'Finished']


def test_worker_error_before_launch_requeues(worker):
    """
    An error before the workflow subprocess starts returns the execution to Pending.

    Regression for finding #4: the worker logged the error and left the execution Scheduled, where
    the main loop (which only looks at Pending executions) never picked it up again.
    """
    worker.record = RuntimeError('REST API unreachable')
    worker.run()
    assert worker.statuses == [('Pending', {'revertPending': True})]


def test_worker_error_after_launch_fails(worker):
    """An error once the workflow subprocess was launched marks the execution Failed."""
    worker.subprocessResult = OSError('cannot execute python')
    worker.run()
    assert [s for s, kw in worker.statuses] == ['Running', 'Failed']


def test_worker_leaves_unscheduled_execution_alone(worker):
    """An execution that is not Scheduled any more (changed meanwhile) is neither run nor modified."""
    worker.record.Status = 'Created'
    worker.run()
    assert worker.statuses == []


def test_worker_unreachable_monitor_does_not_change_outcome(worker, monkeypatch):
    """
    A failing statistics call must not turn a finished execution into a failed one.

    This happens when the scheduler process is killed while a worker still runs its workflow: the
    worker survives, but the statistics monitor (in the killed process) is gone.
    """
    import Pyro5.errors
    from mupifDB import workflowscheduler as ws

    class _DeadMonitor:
        def __getattr__(self, name):
            def call(*args):
                raise Pyro5.errors.CommunicationError('cannot connect')
            return call
    monkeypatch.setattr(ws.Pyro5.api, 'Proxy', lambda uri: _DeadMonitor())
    worker.run()
    assert [s for s, kw in worker.statuses] == ['Running', 'Finished']


def test_recover_interrupted_executions(monkeypatch):
    """
    At startup, Scheduled and Running leftovers return to Pending unless their process still runs.

    Regression for finding #4 (startup part): Scheduled executions found at startup were only
    re-enqueued when resources happened to be free, otherwise they stayed Scheduled forever;
    Running ones were not looked at at all.
    """
    from mupifDB import workflowscheduler as ws
    records = {'Scheduled': ['S1'], 'Running': ['R1', 'R2']}
    statuses = []
    monkeypatch.setattr(ws.restApiControl, 'getExecutionRecords',
                        lambda status, num_limit: [types.SimpleNamespace(dbID=weid) for weid in records[status]])
    monkeypatch.setattr(ws.restApiControl, 'setExecutionStatus', lambda weid, status, **kw: statuses.append((weid, status, kw)))
    monkeypatch.setattr(ws, 'runningExecutionProcesses', lambda: {'R2'})
    ws.recoverInterruptedExecutions()
    assert statuses == [('S1', 'Pending', {'revertPending': True}), ('R1', 'Pending', {'revertPending': True})]


def test_running_execution_processes(tmp_path):
    """The scan finds an execution subprocess by its command line (``... workflow_execution_script.py -eid ID``)."""
    from mupifDB import workflowscheduler as ws
    script = tmp_path / 'workflow_execution_script.py'
    script.write_text('import time\ntime.sleep(60)\n')
    proc = subprocess.Popen([sys.executable, str(script), '-eid', 'unit-test-weid'])
    try:
        for _ in range(50):
            if 'unit-test-weid' in ws.runningExecutionProcesses():
                break
            time.sleep(0.1)
        assert 'unit-test-weid' in ws.runningExecutionProcesses()
    finally:
        proc.kill()
        proc.wait()
    assert 'unit-test-weid' not in ws.runningExecutionProcesses()


# ---------------------------------------------------------------------------------------------
# one iteration of the main loop (scheduler_schedule_pending) against fakes of the REST API,
# the resource check, the statistics monitor and the worker pool
# ---------------------------------------------------------------------------------------------

class _FakePool:
    """Worker pool stand-in: records the execution IDs handed to it."""

    def __init__(self):
        self.enqueued = []

    def apply_async(self, fn, args, **kw):
        self.enqueued.append(args[0])


@pytest.fixture
def schedulingPass(monkeypatch):
    """
    Return a namespace to run one :func:`scheduler_schedule_pending` pass with fakes.

    Attributes: ``pending`` (records returned as Pending), ``jobmanagers`` (workflow ID -> list of
    jobmanagers its models use; ``['CVUT.demo01']`` by default), ``freeSlots`` (jobmanager -> free
    slots; 100 by default), ``failStatusFor`` (execution IDs whose status update raises),
    ``statuses`` (list of ``(weid, status)`` set), ``attempts`` (execution ID -> attempt count set),
    ``slotQueries`` (jobmanagers asked for free slots), ``sleeps``, ``pool`` (the fake pool) and
    ``run()``.
    """
    from mupifDB import workflowscheduler as ws
    env = types.SimpleNamespace(pending=[], jobmanagers={}, freeSlots={}, failStatusFor=set(), statuses=[], attempts={},
                                slotQueries=[], sleeps=[], pool=_FakePool(), pendingQueries=[])

    def setExecutionStatus(weid, status, **kw):
        if weid in env.failStatusFor:
            raise RuntimeError('REST API error')
        env.statuses.append((weid, status))

    def getPendingExecutions(**kw):
        env.pendingQueries.append(kw)
        return list(env.pending)

    monkeypatch.setattr(ws.restApiControl, 'getPendingExecutions', getPendingExecutions)
    monkeypatch.setattr(ws.restApiControl, 'setExecutionStatus', setExecutionStatus)
    monkeypatch.setattr(ws.restApiControl, 'setExecutionAttemptsCount', lambda weid, n: env.attempts.__setitem__(weid, n))

    def freeJobSlots(jobmanager):
        env.slotQueries.append(jobmanager)
        return env.freeSlots.get(jobmanager, 100)

    monkeypatch.setattr(ws, 'workflowJobmanagers', lambda wid, version: env.jobmanagers.get(wid, ['CVUT.demo01']))
    monkeypatch.setattr(ws, 'freeJobSlots', freeJobSlots)
    monkeypatch.setattr(ws.Pyro5.api, 'Proxy', lambda uri: _FakeMonitor())
    monkeypatch.setattr(ws.time, 'sleep', lambda sec: env.sleeps.append(sec))
    env.run = lambda: ws.scheduler_schedule_pending(env.pool)
    return env


def _pendingRecord(weid, attempts=0):
    """Pending execution record as returned by the REST API (only the fields the scheduler reads)."""
    return types.SimpleNamespace(dbID=weid, Attempts=attempts, WorkflowID='Workflow01', WorkflowVersion=1)


def test_failed_status_update_is_not_enqueued(schedulingPass):
    """
    An execution whose change to Scheduled fails stays Pending and is not handed to the pool.

    Regression for finding #5 (non-atomic status change): the execution was enqueued even when
    setting it Scheduled failed; the worker then found it not Scheduled and dropped it.
    """
    schedulingPass.pending = [_pendingRecord('E1'), _pendingRecord('E2')]
    schedulingPass.failStatusFor = {'E1'}
    schedulingPass.run()
    assert schedulingPass.pool.enqueued == ['E2']
    assert schedulingPass.statuses == [('E2', 'Scheduled')]


def test_pass_respects_free_slots(schedulingPass):
    """
    One pass starts no more executions than the jobmanager has free slots, without sleeping.

    Regression for findings #6 and #8: each pending execution was checked against the same free
    slots (they are only taken when the workflow allocates its models), so all of them passed the
    check, while a 2 s sleep per execution made one pass over a long backlog take minutes. The
    free slots are now read once per pass and consumed by every execution enqueued in it.
    """
    schedulingPass.pending = [_pendingRecord(f'E{i}', attempts=1) for i in range(6)]
    schedulingPass.freeSlots = {'CVUT.demo01': 4}
    schedulingPass.run()
    assert schedulingPass.pool.enqueued == ['E0', 'E1', 'E2', 'E3']
    # the others stay Pending and wait; waiting for a busy model server is not a failed attempt,
    # otherwise a long queue alone would exhaust the attempt limit
    assert schedulingPass.attempts == {}
    assert schedulingPass.slotQueries == ['CVUT.demo01']
    assert schedulingPass.sleeps == []


def test_pass_counts_slots_per_model(schedulingPass):
    """A workflow with two models on the same jobmanager needs two of its slots; local models need none."""
    schedulingPass.pending = [_pendingRecord('E0'), _pendingRecord('E1'), _pendingRecord('L0')]
    schedulingPass.pending[2].WorkflowID = 'local'
    schedulingPass.jobmanagers = {'Workflow01': ['CVUT.demo01', 'CVUT.demo01'], 'local': []}
    schedulingPass.freeSlots = {'CVUT.demo01': 3}
    schedulingPass.run()
    assert schedulingPass.pool.enqueued == ['E0', 'L0']


def test_pass_unreachable_jobmanager(schedulingPass):
    """
    An unreachable jobmanager starts nothing and counts as a failed attempt.

    The attempt limit exists for resources that do not become available (after it the execution
    returns to Created and its owner is notified), unlike a busy jobmanager, which just means wait.
    """
    schedulingPass.pending = [_pendingRecord('E0', attempts=3)]
    schedulingPass.freeSlots = {'CVUT.demo01': None}
    schedulingPass.run()
    assert schedulingPass.pool.enqueued == []
    assert schedulingPass.attempts == {'E0': 4}


def test_free_job_slots_unreachable(monkeypatch):
    """freeJobSlots() reports None instead of raising when the jobmanager cannot be reached."""
    import Pyro5.errors
    from mupifDB import workflowscheduler as ws

    def connectModelServer(ns, name):
        raise Pyro5.errors.NamingError(f'unknown name: {name}')
    monkeypatch.setattr(ws.mp.pyroutil, 'connectModelServer', connectModelServer)
    assert ws.freeJobSlots('CVUT.missing') is None


def test_pass_takes_oldest_pending_first(schedulingPass):
    """
    The scheduler asks for pending executions oldest submitted first (finding #7).

    The REST API orders by creation date, newest first, unless told otherwise; the pending
    executions are enqueued in the order received.
    """
    schedulingPass.run()
    assert [q.get('sort') for q in schedulingPass.pendingQueries] == ['SubmittedDate']


# ---------------------------------------------------------------------------------------------
# statistics (SchedulerStat, SchedulerMonitor)
# ---------------------------------------------------------------------------------------------

@pytest.fixture
def statMonitor(monkeypatch, tmp_path):
    """
    Return ``(monitor, call)`` where ``call(method, *args)`` invokes a ``pyro_only`` method directly.

    The monitor persists to a temporary file, and the copy of the counters into the database is
    recorded in ``monitor.synced`` instead of being sent to the REST API.
    """
    from mupifDB import workflowscheduler as ws
    monkeypatch.setattr(ws, 'schedulerStatFile', str(tmp_path / 'stat.json'))
    synced = []
    monkeypatch.setattr(ws.restApiControl, 'setStatScheduler', lambda **kw: synced.append(kw))
    monitor = ws.SchedulerMonitor(_FakeNameserver())
    monitor.synced = synced

    def call(method, *args):
        # bypass the pyro_only check (which requires a Pyro call context)
        return getattr(ws.SchedulerMonitor, method).__wrapped__(monitor, *args)
    return monitor, call


def test_monitor_accounting(statMonitor):
    """
    Counters follow executions through the pool: enqueued -> running -> final or requeued.

    Regression for finding #9: "scheduled" was overwritten with the size of the pending backlog
    (and then decremented, going negative), "pooled" re-counted the same backlog every pass, and an
    execution returned to Pending for lack of resources was counted as failed.
    """
    monitor, call = statMonitor
    for weid in ('E1', 'E2', 'E3', 'E4'):
        call('updateEnqueued', weid)
    call('updateRunning', 'E1', 'wf')
    call('updateRunning', 'E2', 'wf')
    call('updateRunning', 'E3', 'wf')
    call('updateDropped', 'E4')                     # failed before its subprocess started
    call('updateFinished', 0, 'E1')
    call('updateFinished', -9, 'E2')                # killed by a signal: failed
    call('updateFinished', 2, 'E3')                 # no resources: back to Pending, not processed
    t = monitor.stat.tasks
    assert (t.scheduled, t.running, t.processed, t.finished, t.failed) == (0, 0, 2, 1, 1)
    h = monitor.stat.hist48h
    assert (h.pooled[-1], h.processed[-1], h.finished[-1], h.failed[-1]) == (4, 2, 1, 1)
    assert [j.status for j in monitor.stat.lastJobs] == ['Finished', 'Failed', 'Pending']
    call('updateDropped', 'E5')                     # never negative
    assert monitor.stat.tasks.scheduled == 0


def test_monitor_survives_database_errors(monkeypatch, statMonitor):
    """A failure to store the statistics in the database does not propagate (finding #16)."""
    from mupifDB import workflowscheduler as ws
    monitor, call = statMonitor

    def setStatScheduler(**kw):
        raise RuntimeError('401 Unauthorized')
    monkeypatch.setattr(ws.restApiControl, 'setStatScheduler', setStatScheduler)
    call('updateEnqueued', 'E1')
    assert monitor.stat.tasks.scheduled == 1


def test_monitor_restart_keeps_totals(statMonitor):
    """
    Totals and history survive a restart; per-process counters start from zero.

    The scheduler used to reset processedTasks to 0 in the database at every start and stop.
    """
    from mupifDB import workflowscheduler as ws
    monitor, call = statMonitor
    call('updateEnqueued', 'E1')
    call('updateRunning', 'E1', 'wf')
    call('updateFinished', 0, 'E1')
    call('updateEnqueued', 'E2')
    call('updateRunning', 'E2', 'wf')               # still running when the scheduler stops
    call('persistStat')
    restarted = ws.SchedulerMonitor(_FakeNameserver())
    t = restarted.stat.tasks
    assert (t.processed, t.finished, t.running, t.scheduled) == (1, 1, 0, 0)


def test_monitor_fresh_start(statMonitor):
    """
    A monitor started without a statistics file can store its statistics right away.

    The history lists are empty until the time is first advanced; storing the load into them
    failed with IndexError on a fresh installation.
    """
    monitor, call = statMonitor
    monitor.stat.sync()
    assert len(monitor.stat.hist48h.load) == monitor.stat.hist48h.count


def test_copy_log_to_db(monkeypatch, tmp_path, caplog):
    """
    The workflow log is uploaded and linked to the execution, without echoing it into the scheduler log.

    Regression for finding #14: every line of the workflow log was repeated in the scheduler log.
    """
    import logging
    from mupifDB import workflowscheduler as ws
    logFile = tmp_path / 'workflow.log'
    logFile.write_text('first line of the workflow\nsecond line\n')
    uploads, params = [], []
    monkeypatch.setattr(ws.restApiControl, 'uploadBinaryFile', lambda f: uploads.append(f.read()) or 'FILE1')
    monkeypatch.setattr(ws.restApiControl, 'setExecutionParameter', lambda weid, key, val: params.append((weid, key, val)))
    with caplog.at_level(logging.DEBUG):
        ws.copyLogToDB('E1', str(logFile))
    assert uploads == [b'first line of the workflow\nsecond line\n']
    assert params == [('E1', 'ExecutionLog', 'FILE1')]
    assert 'first line of the workflow' not in caplog.text


def test_loop_survives_iteration_error(monkeypatch, tmp_path):
    """
    An error in one iteration of the main loop is logged and the loop continues.

    Regression for finding #16: one failed REST call (e.g. 401 after the REST API restarted with
    a new JWT key) propagated out of the loop and ended the scheduler.
    """
    from mupifDB import workflowscheduler as ws
    monkeypatch.setattr(ws, 'LOOP_SLEEP_SEC', 0.05)
    monkeypatch.setattr(ws, 'stopEvent', ws.threading.Event())
    iterations = []

    def schedulePending(pool):
        iterations.append(1)
        if len(iterations) == 1:
            raise RuntimeError('401 Unauthorized')
        ws.stopEvent.set()

    class _Monitor:
        def persistStat(self):
            pass

    monkeypatch.setattr(ws, 'scheduler_schedule_pending', schedulePending)
    ws.runLoop(pool=None, monitor=_Monitor())
    assert len(iterations) == 2
