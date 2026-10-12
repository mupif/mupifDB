"""Tests of :class:`mupifDB.scheduler.backend.RestBackend` (REST client replaced by a recorder)."""

import types

import pytest

from mupifDB.scheduler.backend import RestBackend


@pytest.fixture
def calls(monkeypatch):
    """Replace the REST client functions used by RestBackend; return the list of calls made."""
    from mupifDB import restApiControl, my_email
    made = []

    def recorder(name, result=None):
        def f(*args, **kw):
            made.append((name, args, kw))
            return result
        return f
    for name in ('getPendingExecutions', 'getExecutionRecords', 'getExecutionRecord', 'getWorkflowRecord',
                 'setExecutionStatus', 'setExecutionAttemptsCount', 'setExecutionParameter'):
        monkeypatch.setattr(restApiControl, name, recorder(name, []))
    monkeypatch.setattr(restApiControl, 'uploadBinaryFile', lambda f: made.append(('uploadBinaryFile', (f.read(),), {})) or 'LOG1')
    monkeypatch.setattr(my_email, 'sendEmailAboutExecutionStatus', recorder('sendEmailAboutExecutionStatus'))
    return made


def test_rest_backend_calls(calls, tmp_path):
    """Each backend operation maps to the REST client call (and arguments) used by the scheduler so far."""
    b = RestBackend()
    log = tmp_path / 'workflow.log'
    log.write_bytes(b'output')
    b.pendingExecutions(300)
    b.executionsWithStatus('Running', 10)
    b.getExecution('E1')
    b.getWorkflow('wf', '2')
    b.setStatus('E1', 'Pending', keepAttempts=True)
    b.setAttempts('E1', 3)
    b.storeLog('E1', log)
    b.notifyOwner('E1')
    assert calls == [
        ('getPendingExecutions', (), {'num_limit': 300, 'sort': 'SubmittedDate'}),
        ('getExecutionRecords', (), {'status': 'Running', 'num_limit': 10}),
        ('getExecutionRecord', ('E1',), {}),
        ('getWorkflowRecord', ('wf', 2), {}),
        ('setExecutionStatus', ('E1', 'Pending'), {'revertPending': True}),
        ('setExecutionAttemptsCount', ('E1', 3), {}),
        ('uploadBinaryFile', (b'output',), {}),
        ('setExecutionParameter', ('E1', 'ExecutionLog', 'LOG1'), {}),
        ('sendEmailAboutExecutionStatus', ('E1',), {}),
    ]


def test_fake_backend_mirrors_rest_semantics():
    """The test double behaves like the REST API where the scheduler depends on it."""
    from mupifDB.scheduler.tests.fakes import FakeBackend
    b = FakeBackend()
    b.addExecution('E2')
    b.addExecution('E1', attempts=4)
    assert [r.dbID for r in b.pendingExecutions(10)] == ['E2', 'E1']     # oldest submitted first
    b.setStatus('E1', 'Pending', keepAttempts=True)
    assert b.executions['E1'].Attempts == 4
    b.setStatus('E1', 'Pending')                                          # a new submission resets the count
    assert b.executions['E1'].Attempts == 0
    b.failures['setStatus'] = lambda weid, status: RuntimeError('REST API down') if weid == 'E2' else None
    with pytest.raises(RuntimeError):
        b.setStatus('E2', 'Scheduled')
    assert types.SimpleNamespace is type(b.getExecution('E2')) and b.status('E2') == 'Pending'
