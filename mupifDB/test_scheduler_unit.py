"""
Unit tests of the workflow scheduler (:mod:`mupifDB.workflowscheduler`).

These tests need no running services: no MongoDB, REST API, Pyro nameserver or model server.
Calls to the REST API are replaced by fakes through ``monkeypatch``. End-to-end behaviour is
covered by ``test_scheduler.py`` (pytest + xprocess) and ``tools/scheduler_smoketest.py``
(real local deployment).

Each test names the finding of the refactoring plan it guards against, where applicable.
"""
import os
import subprocess
import sys
import time


def test_import_has_no_side_effects():
    """
    Importing the scheduler module must not sleep, connect to the nameserver or fail.

    Regression for finding #12: the import used to sleep 5 s and connect to the nameserver, so it
    failed (after 5 s) whenever no nameserver was reachable, e.g. in unit tests. The import runs in
    a fresh interpreter, with ``MUPIF_NS`` pointing to a port where nothing listens.
    """
    code = 'import mupifDB.workflowscheduler as ws; assert ws.ns is None and ws.ns_uri is None'
    env = dict(os.environ, MUPIF_NS='127.0.0.1:1')
    # mupifDB itself (mupif, pydantic, ...) takes about a second to import; the scheduler module adds
    # only milliseconds now, while the old import alone took 5 s
    t0 = time.time()
    subprocess.run([sys.executable, '-c', code], env=env, check=True, cwd=os.path.dirname(__file__) + '/..', timeout=60)
    assert time.time() - t0 < 4.5


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


# ---------------------------------------------------------------------------------------------
# workflow_execution_script: main() is run in-process against fakes of the nameserver, the Pyro
# daemon, the REST API and a workflow class, to check the exit code / status contract with the
# scheduler (0 = Finished, 1 = Failed, 2 = no resources)
# ---------------------------------------------------------------------------------------------

import types

import pytest


class _FakeDaemon:
    """Pyro daemon stand-in: registering an object just returns a made-up URI."""

    def register(self, obj):
        return 'PYRO:fake@127.0.0.1:1'


class _FakeWorkflow:
    """
    Workflow stand-in; class attribute ``fail`` selects where it raises.

    ``fail`` is ``None`` (succeed) or a tuple ``(stage, exception)`` with stage ``'initialize'`` or
    ``'solve'``.
    """

    fail = None
    terminated = 0

    def _maybeFail(self, stage):
        if self.fail and self.fail[0] == stage:
            raise self.fail[1]

    def initialize(self, metadata):
        self._maybeFail('initialize')

    def solve(self):
        self._maybeFail('solve')

    def getExecutionTargetTime(self):
        return 1.

    def terminate(self):
        _FakeWorkflow.terminated += 1


@pytest.fixture
def execScript(monkeypatch):
    """
    Return ``run(fail=None) -> (exit code, [statuses set through the REST API])``.

    Runs :func:`mupifDB.workflow_execution_script.main` for execution ``E1`` of a fake workflow
    (module ``fakewf`` injected into ``sys.modules``) with all external services faked.
    """
    from mupifDB import workflow_execution_script as es
    import mupifDB
    statuses = []
    monkeypatch.setattr(es.mp.pyroutil, 'connectNameserver', lambda: object())
    monkeypatch.setattr(es.mp.pyroutil, 'getDaemon', lambda proxy: _FakeDaemon())
    rc = mupifDB.restApiControl
    monkeypatch.setattr(rc, 'setExecutionStatus', lambda weid, status, **kw: statuses.append(status))
    monkeypatch.setattr(rc, 'getExecutionRecord', lambda weid: types.SimpleNamespace(WorkflowID='wf', WorkflowVersion=1, Task_ID=''))
    monkeypatch.setattr(rc, 'getWorkflowRecord', lambda wid, ver: types.SimpleNamespace(modulename='fakewf', classname='FakeWorkflow', UseCase='uc'))
    monkeypatch.setattr(rc, 'setExecutionParameter', lambda *a, **kw: None)
    monkeypatch.setattr(mupifDB.workflowmanager, 'mapInputs', lambda wf, weid: None)
    monkeypatch.setattr(mupifDB.workflowmanager, 'mapOutputs', lambda wf, weid, t: None)
    monkeypatch.setitem(sys.modules, 'fakewf', types.SimpleNamespace(FakeWorkflow=_FakeWorkflow))
    monkeypatch.setattr(sys, 'argv', ['workflow_execution_script.py', '-eid', 'E1'])

    def run(fail=None):
        monkeypatch.setattr(_FakeWorkflow, 'fail', fail)
        statuses.clear()
        with pytest.raises(SystemExit) as exc:
            es.main()
        return exc.value.code, list(statuses)
    return run


def test_exec_script_success(execScript):
    """A workflow that solves without error exits with 0 and is marked Finished."""
    assert execScript() == (0, ['Running', 'Finished'])


def test_exec_script_workflow_error(execScript):
    """
    A workflow raising during solve exits with 1 and is marked Failed.

    Also guards the ``mupifDB`` local-name pitfall of ``main()``: an ``import mupifDB.x`` statement
    inside the function would make every earlier use of ``mupifDB`` raise ``UnboundLocalError``.
    """
    assert execScript(fail=('solve', RuntimeError('boom'))) == (1, ['Running', 'Failed'])
