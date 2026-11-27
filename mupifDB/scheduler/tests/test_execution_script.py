"""
Tests of ``workflow_execution_script.py``: exit code and status contract with the scheduler.

``main()`` runs in-process against fakes of the nameserver, the Pyro daemon, the REST API and a
workflow class (exit code 0 = Finished, 1 = Failed, 2 = no resources, see the script's documentation).
Moved from ``mupifDB/test_scheduler_unit.py``.
"""

import os
import signal
import subprocess
import sys
import time
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
    ``'solve'``; instead of an exception, ``'SIGTERM'`` makes the workflow send SIGTERM to its own
    process (as the scheduler does when it stops an execution).
    """

    fail = None
    terminated = 0
    #: metadata given to the last initialize()
    initMetadata = None
    #: what getModelServers() returns
    modelServers: dict = {}
    #: what getModelPackages() returns
    modelPackages: dict = {}

    def _maybeFail(self, stage):
        if self.fail and self.fail[0] == stage:
            if self.fail[1] == 'SIGTERM':
                os.kill(os.getpid(), signal.SIGTERM)
                time.sleep(5)          # the signal handler raises before this ends
            raise self.fail[1]

    def initialize(self, metadata):
        _FakeWorkflow.initMetadata = metadata
        self._maybeFail('initialize')

    def getModelServers(self):
        return dict(self.modelServers)

    def getModelPackages(self):
        return dict(self.modelPackages)

    def solve(self):
        self._maybeFail('solve')

    def getExecutionTargetTime(self):
        return 1.

    def terminate(self):
        _FakeWorkflow.terminated += 1


@pytest.fixture
def execScript(monkeypatch):
    """
    Return ``run(fail=None, standalone=True) -> (exit code, [statuses set through the REST API])``.

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
    record = types.SimpleNamespace(WorkflowID='wf', WorkflowVersion=1, Task_ID='', ExecutionProfileIndex=-1)
    monkeypatch.setattr(rc, 'getExecutionRecord', lambda weid: record)
    monkeypatch.setattr(rc, 'getWorkflowRecord', lambda wid, ver: types.SimpleNamespace(modulename='fakewf', classname='FakeWorkflow', UseCase='uc'))
    monkeypatch.setattr(rc, 'setExecutionParameter', lambda *a, **kw: None)
    stored = {}
    monkeypatch.setattr(rc, 'setExecutionModelServers', lambda weid, servers: stored.update({weid: servers}))
    storedPackages = {}
    monkeypatch.setattr(rc, 'setExecutionModelPackages', lambda weid, packages: storedPackages.update({weid: packages}))
    monkeypatch.setattr(mupifDB.workflowmanager, 'mapInputs', lambda wf, weid: None)
    monkeypatch.setattr(mupifDB.workflowmanager, 'mapOutputs', lambda wf, weid, t: None)
    monkeypatch.setitem(sys.modules, 'fakewf', types.SimpleNamespace(FakeWorkflow=_FakeWorkflow))
    monkeypatch.setattr(sys, 'argv', ['workflow_execution_script.py', '-eid', 'E1'])
    # not started by a scheduler: no parent-death signal
    monkeypatch.delenv('MUPIFDB_SCHEDULER_PID', raising=False)
    previousSigterm = signal.getsignal(signal.SIGTERM)

    def run(fail=None, standalone=True):
        monkeypatch.setattr(_FakeWorkflow, 'fail', fail)
        monkeypatch.setattr(sys, 'argv', ['workflow_execution_script.py', '-eid', 'E1'] + (['--standalone'] if standalone else []))
        statuses.clear()
        try:
            with pytest.raises(SystemExit) as exc:
                es.main()
        finally:
            # main() installs its SIGTERM handler in this (pytest's) process
            signal.signal(signal.SIGTERM, previousSigterm)
        return exc.value.code, list(statuses)
    run.record, run.stored, run.storedPackages = record, stored, storedPackages
    return run


def test_exec_script_success(execScript):
    """A workflow that solves without error exits with 0 and is marked Finished."""
    assert execScript() == (0, ['Running', 'Finished'])


def test_exec_script_profile_and_model_servers(execScript, monkeypatch, caplog):
    """The execution's profile is passed to the workflow; the model servers it chose are logged and stored in the record."""
    import logging
    monkeypatch.setattr(_FakeWorkflow, 'modelServers', {'m': 'CVUT.demo01~node2'})
    with caplog.at_level(logging.INFO):
        assert execScript()[0] == 0
    # default profile: nothing added to the metadata
    assert 'ExecutionProfileIndex' not in _FakeWorkflow.initMetadata['Execution']
    assert execScript.stored == {'E1': {'m': 'CVUT.demo01~node2'}}
    assert 'Model servers used: m on CVUT.demo01~node2' in caplog.text
    execScript.record.ExecutionProfileIndex = 1
    assert execScript()[0] == 0
    assert _FakeWorkflow.initMetadata['Execution']['ExecutionProfileIndex'] == 1


def test_exec_script_model_packages(execScript, monkeypatch, caplog):
    """The model packages run by execution servers are logged and stored in the record (provenance)."""
    import logging
    pkg = {'ID': 'oofem-thermal', 'Version': '1.2', 'Digest': 'sha256:' + 'a' * 64}
    monkeypatch.setattr(_FakeWorkflow, 'modelServers', {'m': 'Exec.node1'})
    monkeypatch.setattr(_FakeWorkflow, 'modelPackages', {'m': pkg})
    with caplog.at_level(logging.INFO):
        assert execScript()[0] == 0
    assert execScript.storedPackages == {'E1': {'m': pkg}}
    assert 'Model packages run: m: oofem-thermal@1.2 (sha256:aaaa' in caplog.text
    # no models on execution servers: nothing stored
    execScript.storedPackages.clear()
    monkeypatch.setattr(_FakeWorkflow, 'modelPackages', {})
    assert execScript()[0] == 0
    assert execScript.storedPackages == {}


def test_exec_script_lease_lost(execScript):
    """A write refused because the run's lease was taken over (409) ends the script without setting a status."""
    from mupifDB.api.client_util import ConflictResponse
    assert execScript(fail=('solve', ConflictResponse('409 lease'))) == (1, ['Running'])


def test_exec_script_workflow_error(execScript):
    """
    A workflow raising during solve exits with 1 and is marked Failed.

    Also guards the ``mupifDB`` local-name pitfall of ``main()``: an ``import mupifDB.x`` statement
    inside the function would make every earlier use of ``mupifDB`` raise ``UnboundLocalError``.
    """
    assert execScript(fail=('solve', RuntimeError('boom'))) == (1, ['Running', 'Failed'])


#: what the workflow actually receives from a full model server (mupif, 2026-10): the server
#: cannot serialize ModelServerNoResourcesException and Pyro substitutes a generic PyroError
_PYRO_NO_RESOURCES_MESSAGE = ("Error serializing exception: 'dict' object is not callable. Original exception: "
                              "<class 'mupif.modelserverbase.ModelServerNoResourcesException'>: ModelServer: no more resources")


def _pyroErrorWithRemoteTraceback(text):
    """Build a PyroError carrying *text* only in its remote traceback, like Pyro does."""
    import Pyro5.errors
    err = Pyro5.errors.PyroError('remote call failed')
    err._pyroTraceback = ['Traceback (most recent call last):\n', text + '\n']
    return err


def _noResourcesErrors():
    """All forms in which a no-resources error may reach the execution script."""
    import mupif as mp
    import Pyro5.errors
    local = mp.ModelServerNoResourcesException('ModelServer: no more resources')
    wrapped = RuntimeError('allocating model m1 failed')
    wrapped.__cause__ = Pyro5.errors.PyroError(_PYRO_NO_RESOURCES_MESSAGE)
    return {
        'local': local,
        'pyro-message': Pyro5.errors.PyroError(_PYRO_NO_RESOURCES_MESSAGE),
        'pyro-traceback': _pyroErrorWithRemoteTraceback('mupif.modelserverbase.ModelServerNoResourcesException: ModelServer: no more resources'),
        'wrapped': wrapped,
    }


@pytest.mark.parametrize('form', ['local', 'pyro-message', 'pyro-traceback', 'wrapped'])
def test_exec_script_no_resources(execScript, form):
    """
    A model server without free slots ends the script with exit code 2 and no Failed status.

    Regression for finding #2: the script compared ``type(err) == mp.JobManNoResourcesException``
    (a deprecated subclass that is never raised, while the error actually arrives as a generic
    ``PyroError``) and set the status to Failed before exiting, so executions lacking resources
    were failed instead of being returned to Pending by the scheduler.
    """
    code, statuses = execScript(fail=('initialize', _noResourcesErrors()[form]))
    assert (code, statuses) == (2, ['Running'])


def test_exec_script_other_pyro_error_fails(execScript):
    """A Pyro error not caused by missing resources is an ordinary failure (exit 1, Failed)."""
    import Pyro5.errors
    err = Pyro5.errors.CommunicationError('connection to model server lost')
    assert execScript(fail=('initialize', err)) == (1, ['Running', 'Failed'])


@pytest.mark.parametrize('fail,code', [(None, 0), (('solve', RuntimeError('boom')), 1), (('initialize', __import__('mupif').ModelServerNoResourcesException('full')), 2)])
def test_exec_script_under_scheduler_sets_no_status(execScript, fail, code):
    """
    Started by the scheduler (without --standalone), the script reports only through its exit code.

    The scheduler sets the status after storing the log; the script used to set Finished itself,
    so for a moment executions were Finished without their log.
    """
    assert execScript(fail=fail, standalone=False) == (code, [])


def test_exec_script_sigterm_terminates_workflow(execScript, monkeypatch):
    """
    SIGTERM (the scheduler stopping the execution) terminates the workflow and its models; exit code 143.

    Terminating the workflow releases its model-server jobs (finding #17, for stopped executions).
    """
    monkeypatch.setattr(_FakeWorkflow, 'terminated', 0)
    assert execScript(fail=('solve', 'SIGTERM'), standalone=False) == (143, [])
    assert _FakeWorkflow.terminated == 1


#: started by the "scheduler" process of test_die_with_scheduler: arms the parent-death signal and sleeps
_CHILD = '''
import os, time
os.environ['MUPIFDB_SCHEDULER_PID'] = str(os.getppid())
from mupifDB.workflow_execution_script import dieWithScheduler
dieWithScheduler()
print('armed', flush=True)
time.sleep(60)
'''

#: the "scheduler": starts the child, prints its PID and exits
_PARENT = '''
import subprocess, sys
child = subprocess.Popen([sys.executable, sys.argv[1]], stdout=subprocess.PIPE, text=True)
assert child.stdout.readline().strip() == 'armed'
print(child.pid, flush=True)
'''


def test_die_with_scheduler(tmp_path):
    """
    The execution script ends when the scheduler that started it dies (e.g. killed with SIGKILL).

    The workflow runs in its own process group, so nothing else would stop it; the restarted
    scheduler must not find it still running when it runs the execution again. A real parent
    process starts a child which calls dieWithScheduler() and sleeps; the child must be gone
    shortly after the parent exits.
    """
    (tmp_path / 'child.py').write_text(_CHILD)
    (tmp_path / 'parent.py').write_text(_PARENT)
    repo = os.path.dirname(__file__) + '/../../..'
    out = subprocess.run([sys.executable, str(tmp_path / 'parent.py'), str(tmp_path / 'child.py')], capture_output=True, text=True,
                         timeout=60, env=dict(os.environ, MUPIF_NS='127.0.0.1:1', PYTHONPATH=os.path.abspath(repo)))
    childPid = int(out.stdout.strip().splitlines()[-1])
    for _ in range(50):
        try:
            with open(f'/proc/{childPid}/stat') as f:
                if f.read().split()[2] == 'Z':
                    break
        except FileNotFoundError:
            break
        time.sleep(0.1)
    else:
        os.kill(childPid, signal.SIGKILL)
        pytest.fail('the child survived its parent')
