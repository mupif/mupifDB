"""
Unit tests of the REST client's lease support (:mod:`mupifDB.api.client_util`, :mod:`mupifDB.api.client_mupif`).

No REST API is needed: ``requests`` is replaced by a fake answering with a configured status.
"""
import json
import types

import pytest


class _FakeResponse:
    def __init__(self, status, method, url, body):
        self.status_code = status
        self.reason = {200: 'OK', 409: 'Conflict', 500: 'Internal Server Error'}[status]
        self.text = json.dumps(body)
        self._body = body
        self.request = types.SimpleNamespace(method=method.upper(), url=url)

    def json(self):
        return self._body


@pytest.fixture
def fakeApi(monkeypatch):
    """Return a namespace: ``sent`` (method, url, headers, json) of the requests; set ``status``/``body`` of the answers."""
    from mupifDB.api import client_util, client_mupif
    ns = types.SimpleNamespace(sent=[], status=200, body={})

    def fakeRequest(method):
        def request(url, headers=None, json=None, **kw):
            ns.sent.append((method, url, dict(headers or {}), json))
            return _FakeResponse(ns.status, method, url, ns.body)
        return request
    for method in ('get', 'post', 'patch', 'put', 'delete'):
        monkeypatch.setattr(client_util.requests, method, fakeRequest(method))
    monkeypatch.setattr(client_mupif, 'getAuthToken', lambda: {'access_token': 't'})
    monkeypatch.delenv('MUPIFDB_LEASE', raising=False)
    return ns


EXEC = {'_id': '6b0000000000000000000001', 'WorkflowID': 'wf', 'WorkflowVersion': 1, 'Status': 'Scheduled', 'CreatedDate': '2026-01-01T10:00:00',
        'RequestedBy': 'u', 'Inputs': '', 'Outputs': '', 'SchedulerID': 's1', 'LeaseEpoch': 1}


def test_conflict_is_distinct(fakeApi):
    """409 raises ConflictResponse (do not retry: lease lost or taken); other errors stay RuntimeError."""
    from mupifDB.api import client_mupif, client_util
    fakeApi.status = 409
    with pytest.raises(client_util.ConflictResponse):
        client_mupif.claimExecution('E1', 's1', 60)
    fakeApi.status = 500
    with pytest.raises(RuntimeError):
        client_mupif.claimExecution('E1', 's1', 60)


def test_lease_header(fakeApi, monkeypatch):
    """Requests carry the lease of the thread's context, else of MUPIFDB_LEASE (the execution script), else none."""
    from mupifDB.api import client_mupif, client_util
    client_mupif.setExecutionParameter('E1', 'label', 'x')
    assert 'X-MupifDB-Lease' not in fakeApi.sent[-1][2]
    with client_util.leaseContext('E1:s1:3'):
        client_mupif.setExecutionParameter('E1', 'label', 'x')
    assert fakeApi.sent[-1][2]['X-MupifDB-Lease'] == 'E1:s1:3'
    client_mupif.setExecutionParameter('E1', 'label', 'x')
    assert 'X-MupifDB-Lease' not in fakeApi.sent[-1][2]
    monkeypatch.setenv('MUPIFDB_LEASE', 'E1:s1:4')
    client_mupif.setExecutionParameter('E1', 'label', 'x')
    assert fakeApi.sent[-1][2]['X-MupifDB-Lease'] == 'E1:s1:4'


def test_lease_requests(fakeApi):
    """The lease operations send what the API expects."""
    from mupifDB.api import client_mupif
    fakeApi.body = EXEC
    rec = client_mupif.claimExecution('E1', 's1', 60)
    assert rec.SchedulerID == 's1' and rec.LeaseEpoch == 1
    assert fakeApi.sent[-1][:2] == ('patch', client_mupif.RESTserver + 'api/executions/E1/claim')
    assert fakeApi.sent[-1][3] == {'scheduler_id': 's1', 'lease_seconds': 60}
    client_mupif.transitionExecution('E1', 's1', 1, 'Pending', incAttempts=-1, backoff=(10, 600, 0.2))
    assert fakeApi.sent[-1][3] == {'scheduler_id': 's1', 'epoch': 1, 'to': 'Pending', 'incAttempts': -1, 'resetAttempts': False,
                                   'backoff': {'base': 10, 'max': 600, 'jitter': 0.2}, 'lease_seconds': None}
    fakeApi.body = {'identity': True, 'held': ['E1']}
    assert client_mupif.schedulerHeartbeat('s1', 'i1', 60, 600, [('E1', 1)])['held'] == ['E1']
    assert fakeApi.sent[-1][3]['leases'] == [{'weid': 'E1', 'epoch': 1}]
    fakeApi.body = {'collection': []}
    client_mupif.getPendingExecutions(num_limit=5, sort='SubmittedDate', ready=True)
    assert fakeApi.sent[-1][1].endswith('&ready=true')


def test_identity_conflict_is_false(fakeApi):
    from mupifDB.api import client_mupif
    fakeApi.status = 409
    assert client_mupif.acquireSchedulerIdentity('s1', 'i2', 'h', 1, 60) is False
