"""
Tests of the lease endpoints of the REST API (mupifDB.api.main) against a throwaway MongoDB.

The API module connects to MongoDB at import (``MUPIFDB_MONGODB_PORT``), so it is imported by the
fixture once the server runs; authentication is replaced by a fixed user.
"""

import importlib
import os
import sys

import bson
import pytest


@pytest.fixture(scope='module')
def api(mongo):
    """``(TestClient, main module, user)``: the API on the test MongoDB, called as an admin user."""
    os.environ['MUPIFDB_MONGODB_PORT'] = str(mongo.PORT)
    from fastapi.testclient import TestClient
    if 'mupifDB.api.main' in sys.modules:
        main = importlib.reload(sys.modules['mupifDB.api.main'])
    else:
        import mupifDB.api.main as main
    main.db.WorkflowExecutions.delete_many({})
    main.db.Schedulers.delete_many({})
    user = main.User_Model(mail='scheduler@test', name='s', surname='s', password='x', rights_admin=True)
    main.app.dependency_overrides[main.get_current_authenticated_user] = lambda: user
    with TestClient(main.app) as client:
        yield client, main, user


def add(main, status='Pending', **fields) -> str:
    return str(main.db.WorkflowExecutions.insert_one({'Status': status, 'Attempts': 0, 'WorkflowID': 'wf', 'WorkflowVersion': 1,
                                                       'CreatedDate': '2026-01-01T10:00:00', 'RequestedBy': 'u', 'Inputs': '', 'Outputs': '', **fields}).inserted_id)


def test_claim_transition_and_conflicts(api):
    client, main, _ = api
    weid = add(main)
    r = client.patch(f'/api/executions/{weid}/claim', json={'scheduler_id': 's1', 'lease_seconds': 60})
    assert r.status_code == 200 and r.json()['SchedulerID'] == 's1' and r.json()['LeaseEpoch'] == 1
    # taken: a second claim is a conflict
    assert client.patch(f'/api/executions/{weid}/claim', json={'scheduler_id': 's2', 'lease_seconds': 60}).status_code == 409
    # transitions under the lease only
    assert client.post(f'/api/executions/{weid}/transition', json={'scheduler_id': 's2', 'epoch': 1, 'to': 'Running'}).status_code == 409
    r = client.post(f'/api/executions/{weid}/transition', json={'scheduler_id': 's1', 'epoch': 1, 'to': 'Running', 'incAttempts': 1, 'lease_seconds': 60})
    assert r.status_code == 200 and r.json()['Status'] == 'Running' and r.json()['Attempts'] == 1
    r = client.post(f'/api/executions/{weid}/transition', json={'scheduler_id': 's1', 'epoch': 1, 'to': 'Pending', 'incAttempts': -1,
                                                               'backoff': {'base': 10, 'max': 600, 'jitter': 0}})
    assert r.status_code == 200 and r.json()['Status'] == 'Pending' and r.json()['NotBefore'] is not None
    # not due: not listed as ready, not claimable
    ready = client.get('/api/executions', params={'status': 'Pending', 'ready': True}).json()['collection']
    assert weid not in [e['_id'] for e in ready]
    assert client.patch(f'/api/executions/{weid}/claim', json={'scheduler_id': 's1', 'lease_seconds': 60}).status_code == 409


def test_defer_once(api):
    client, main, _ = api
    weid = add(main)
    body = {'backoff': {'base': 10, 'max': 600, 'jitter': 0.2}}
    assert client.patch(f'/api/executions/{weid}/defer', json=body).status_code == 200
    assert client.patch(f'/api/executions/{weid}/defer', json=body).status_code == 409
    assert main.db.WorkflowExecutions.find_one({'_id': bson.ObjectId(weid)})['Attempts'] == 1


def test_identity_heartbeat_takeover(api):
    client, main, _ = api
    acq = {'instance': 'i1', 'host': 'h', 'pid': 1, 'heartbeat_seconds': 60}
    assert client.post('/api/schedulers/s1/acquire', json=acq).status_code == 200
    assert client.post('/api/schedulers/s1/acquire', json=acq | {'instance': 'i2'}).status_code == 409
    weid = add(main)
    client.patch(f'/api/executions/{weid}/claim', json={'scheduler_id': 's1', 'lease_seconds': 60})
    hb = {'instance': 'i1', 'heartbeat_seconds': 60, 'lease_seconds': 60, 'leases': [{'weid': weid, 'epoch': 1}], 'stats': {'running': 1}}
    r = client.post('/api/schedulers/s1/heartbeat', json=hb).json()
    assert r == {'identity': True, 'held': [weid]}
    schedulers = {s['_id']: s for s in client.get('/api/schedulers').json()}
    assert schedulers['s1']['Live'] and schedulers['s1']['Stats'] == {'running': 1}
    # s1 alive: its expired lease is not taken over; after its release it is
    main.db.WorkflowExecutions.update_one({'_id': bson.ObjectId(weid)}, {'$set': {'LeaseExpires': main.datetime(2000, 1, 1)}})
    assert client.post('/api/executions/leases/takeover', json={'scheduler_id': 's2', 'lease_seconds': 60}).json() == []
    client.post('/api/schedulers/s1/release', json={'instance': 'i1'})
    taken = client.post('/api/executions/leases/takeover', json={'scheduler_id': 's2', 'lease_seconds': 60}).json()
    assert [t['_id'] for t in taken] == [weid] and taken[0]['LeaseEpoch'] == 2
    # s1 learns it lost it
    assert client.post('/api/schedulers/s1/heartbeat', json=hb).json() == {'identity': True, 'held': []}


def test_scheduler_account_required(api):
    client, main, user = api
    user.rights_admin = False
    try:
        weid = add(main)
        assert client.patch(f'/api/executions/{weid}/claim', json={'scheduler_id': 's1', 'lease_seconds': 60}).status_code == 403
        os.environ['MUPIFDB_SCHEDULER_ACCOUNTS'] = 'other@test, scheduler@test'
        assert client.patch(f'/api/executions/{weid}/claim', json={'scheduler_id': 's1', 'lease_seconds': 60}).status_code == 200
    finally:
        user.rights_admin = True
        os.environ.pop('MUPIFDB_SCHEDULER_ACCOUNTS', None)
