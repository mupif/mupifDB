"""Tests of :class:`mupifDB.scheduler.backend.RestBackend` (REST client replaced by a recorder)."""

import types

import pytest

from mupifDB.scheduler.backend import RestBackend


EXEC = types.SimpleNamespace(dbID='E1', SchedulerID='s1', LeaseEpoch=2)


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
    for name in ('getPendingExecutions', 'getExecutionRecords', 'getExecutionRecord', 'getWorkflowRecord', 'setExecutionParameter', 'checkExecutionInputs'):
        monkeypatch.setattr(restApiControl, name, recorder(name, []))
    for name in ('claimExecution', 'transitionExecution', 'deferExecution'):
        monkeypatch.setattr(restApiControl, name, recorder(name, EXEC))
    monkeypatch.setattr(restApiControl, 'schedulerHeartbeat', recorder('schedulerHeartbeat', {'identity': True, 'held': ['E1']}))
    monkeypatch.setattr(restApiControl, 'uploadBinaryFile', lambda f: made.append(('uploadBinaryFile', (f.read(),), {})) or 'LOG1')
    monkeypatch.setattr(my_email, 'sendEmailAboutExecutionStatus', recorder('sendEmailAboutExecutionStatus'))
    return made


def test_rest_backend_calls(calls, tmp_path):
    """Each backend operation maps to the REST client call (and arguments) used by the scheduler."""
    from mupifDB.scheduler.backend import Lease
    b = RestBackend('s1', 600)
    log = tmp_path / 'workflow.log'
    log.write_bytes(b'output')
    b.pendingExecutions(300)
    b.executionsWithStatus('Running', 10)
    b.getExecution('E1')
    b.getWorkflow('wf', '2')
    b.inputsComplete('E1')
    assert b.claim('E1') == (EXEC, Lease('E1', 's1', 2))
    b.transition(Lease('E1', 's1', 2), 'Pending', incAttempts=-1, backoff=(10, 600, 0.2))
    b.transition(Lease('E1', 's1', 2), 'Running', renew=True)
    assert b.defer('E1', (10, 600, 0.2)) is True
    assert b.heartbeat([Lease('E1', 's1', 2)]) == (True, {'E1'})
    b.storeLog('E1', log, Lease('E1', 's1', 2))
    b.notifyOwner('E1')
    assert calls == [
        ('getPendingExecutions', (), {'num_limit': 300, 'sort': 'SubmittedDate', 'ready': True}),
        ('getExecutionRecords', (), {'status': 'Running', 'num_limit': 10}),
        ('getExecutionRecord', ('E1',), {}),
        ('getWorkflowRecord', ('wf', 2), {}),
        ('checkExecutionInputs', ('E1',), {}),
        ('claimExecution', ('E1', 's1', 600), {}),
        ('transitionExecution', ('E1', 's1', 2, 'Pending'), {'incAttempts': -1, 'resetAttempts': False, 'backoff': (10, 600, 0.2), 'leaseSeconds': None}),
        ('transitionExecution', ('E1', 's1', 2, 'Running'), {'incAttempts': 0, 'resetAttempts': False, 'backoff': None, 'leaseSeconds': 600}),
        ('deferExecution', ('E1', (10, 600, 0.2)), {}),
        ('schedulerHeartbeat', ('s1', b.instance, 600, 600, [('E1', 2)]), {'stats': None, 'draining': None}),
        ('uploadBinaryFile', (b'output',), {}),
        ('setExecutionParameter', ('E1', 'ExecutionLog', 'LOG1'), {}),
        ('sendEmailAboutExecutionStatus', ('E1',), {}),
    ]


def test_conflict_is_lease_lost(monkeypatch):
    """A 409 of the REST API (ConflictResponse) is LeaseLost for the scheduler; defer reports False."""
    from mupifDB import restApiControl
    from mupifDB.api.client_util import ConflictResponse
    from mupifDB.scheduler.backend import Lease, LeaseLost

    def conflict(*a, **kw):
        raise ConflictResponse('409')
    for name in ('claimExecution', 'transitionExecution', 'deferExecution'):
        monkeypatch.setattr(restApiControl, name, conflict)
    b = RestBackend('s1', 600)
    with pytest.raises(LeaseLost):
        b.claim('E1')
    with pytest.raises(LeaseLost):
        b.transition(Lease('E1', 's1', 1), 'Failed')
    assert b.defer('E1', (10, 600, 0)) is False


def test_pending_executions_query_keeps_order_and_limit(monkeypatch):
    """
    The request for pending executions asks the API for the oldest submitted first and for the limit.

    test_rest_backend_calls replaces the client functions, so it does not see the query they build:
    when 'sort' was dropped from it (merge of the num_limit -> pageSize change), the API silently
    returned the newest executions first, and old ones could wait indefinitely.
    """
    from mupifDB.api import client_mupif
    queries = []
    monkeypatch.setattr(client_mupif, 'rGet', lambda query, **kw: queries.append(query) or {'collection': []})
    assert RestBackend().pendingExecutions(5) == []
    params = queries[0].split('?', 1)[1].split('&')
    # only executions whose retry delay passed (stored with them, respected by every scheduler)
    assert {'status=Pending', 'pageSize=5', 'sort=SubmittedDate', 'ready=true'} <= set(params)


def test_fake_backend_basics():
    """The test double lists due pending executions oldest first, returns copies and can be told to fail."""
    from mupifDB.scheduler.tests.fakes import FakeBackend
    b = FakeBackend()
    b.addExecution('E2')
    b.addExecution('E1', attempts=4)
    b.addExecution('E3', NotBefore=b.store.clock() + 60)
    assert [r.dbID for r in b.pendingExecutions(10)] == ['E2', 'E1']     # oldest submitted first, only due
    b.failures['claim'] = lambda weid: RuntimeError('REST API down') if weid == 'E2' else None
    with pytest.raises(RuntimeError):
        b.claim('E2')
    assert types.SimpleNamespace is type(b.getExecution('E2')) and b.status('E2') == 'Pending'
