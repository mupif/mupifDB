"""
Tests of the EDM output objects of an execution attempt (:func:`mupifDB.workflowmanager.createOutputEDMMappingObjects`)
and of the REST client's retries in the execution script; the REST client is replaced by fakes.
"""
import types

import pytest


@pytest.fixture
def fakeClient(monkeypatch):
    """The client functions used to create EDM objects; returns the list of calls made."""
    from mupifDB import workflowmanager, models
    calls = []
    M = models.EDMMapping_Model
    execution = types.SimpleNamespace(EDMMapping=[
        M(Name='inp', EDMEntity='T', DBName='db', id='in1'),                               # input, given by the user
        M(Name='res', EDMEntity='T', DBName='db', id='old1', createNew=models.EDMMappingIDs_Model()),   # created by an earlier attempt
        M(Name='cpy', EDMEntity='T', DBName='db', id='old2', createFrom='inp'),
        M(Name='usr', EDMEntity='T', DBName='db', id='u1'),                                # output into a user's object
    ])
    c = workflowmanager.client
    monkeypatch.setattr(c, 'getExecutionRecord', lambda eid: execution)
    monkeypatch.setattr(c, 'getExecutionOutputRecord', lambda eid: [])
    monkeypatch.setattr(c, 'getSafeLinks', lambda **kw: [])
    monkeypatch.setattr(c, 'createEDMData', lambda **kw: calls.append(('create', kw['Type'])) or 'new1')
    monkeypatch.setattr(c, 'cloneEDMData', lambda **kw: calls.append(('clone', kw['ID'])) or 'new2')
    monkeypatch.setattr(c, 'setExecutionOntoBaseObjectID', lambda eid, name, value: calls.append(('setId', name, value)))
    monkeypatch.setattr(c, 'setEDMData', lambda **kw: calls.append(('meta', kw['ID'], kw['data'])))
    return calls


def test_each_attempt_creates_fresh_objects(fakeClient, monkeypatch):
    """
    An attempt creates its own output objects (stamped with its lease epoch) instead of writing into
    those of an earlier attempt, which may still be written by a run cut off from the database; the
    earlier ones are marked orphaned. Objects given by the user are left alone.
    """
    from mupifDB import workflowmanager
    monkeypatch.setenv('MUPIFDB_LEASE', 'E1:s1:3')
    workflowmanager.createOutputEDMMappingObjects(app=None, eid='E1', outputs=[])
    assert ('create', 'T') in fakeClient and ('clone', 'in1') in fakeClient
    assert ('setId', 'res', 'new1') in fakeClient and ('setId', 'cpy', 'new2') in fakeClient
    assert ('meta', 'new1', {'execution': 'E1', 'epoch': 3}) in fakeClient
    assert ('meta', 'old1', {'execution': 'E1', 'orphaned': True, 'replacedByEpoch': 3}) in fakeClient
    assert ('meta', 'old2', {'execution': 'E1', 'orphaned': True, 'replacedByEpoch': 3}) in fakeClient
    assert not any(c[1] in ('u1', 'in1') for c in fakeClient if c[0] in ('meta', 'setId'))


def test_requests_retried_in_the_execution_script(monkeypatch):
    """The execution script (MUPIFDB_RETRY_SECONDS set by the scheduler) waits out a short REST API outage; others do not."""
    import requests
    from mupifDB.api import client_util
    attempts = []

    def get(url, headers=None, **kw):
        attempts.append(url)
        if len(attempts) < 3:
            raise requests.ConnectionError('refused')
        return types.SimpleNamespace(status_code=200, reason='OK', text='{}', json=lambda: {}, request=types.SimpleNamespace(method='GET', url=url))
    monkeypatch.setattr(client_util.requests, 'get', get)
    monkeypatch.setattr(client_util.time, 'sleep', lambda s: None)
    monkeypatch.delenv('MUPIFDB_RETRY_SECONDS', raising=False)
    with pytest.raises(requests.ConnectionError):
        client_util.rGetRaw('api/x')
    attempts.clear()
    monkeypatch.setenv('MUPIFDB_RETRY_SECONDS', '600')
    assert client_util.rGetRaw('api/x').status_code == 200 and len(attempts) == 3
