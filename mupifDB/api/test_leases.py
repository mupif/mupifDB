"""
Tests of :mod:`mupifDB.api.leases` against a real MongoDB (fixture ``mongo`` in conftest.py; skipped when ``mongod`` is missing).

The lease operations rely on MongoDB semantics (atomic conditional updates, update pipelines,
``$$NOW``) which an in-memory fake would only imitate; these tests check the real thing. Times are
the database's: an expired lease is simulated by setting ``LeaseExpires`` into the past.
"""

import datetime
import threading
import time

import bson
import pymongo
import pytest

from mupifDB.api import leases

PAST = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
BACKOFF = (10., 600., 0.)


@pytest.fixture
def db(mongo, request):
    """Fresh collections ``WorkflowExecutions`` and ``Schedulers`` per test."""
    d = mongo[f'test_{request.node.name}'[:60]]
    d.WorkflowExecutions.drop()
    d.Schedulers.drop()
    leases.ensureIndexes(d.WorkflowExecutions, d.Schedulers)
    return d


def add(db, status='Pending', **fields) -> str:
    """Insert an execution record; return its ID."""
    return str(db.WorkflowExecutions.insert_one({'Status': status, 'Attempts': 0, **fields}).inserted_id)


def rec(db, weid) -> dict:
    return db.WorkflowExecutions.find_one({'_id': bson.ObjectId(weid)})


def expire(db, weid):
    db.WorkflowExecutions.update_one({'_id': bson.ObjectId(weid)}, {'$set': {'LeaseExpires': PAST}})


# --- claim -----------------------------------------------------------------------------------------

def test_claim_exactly_one_wins(db):
    """Of 8 schedulers claiming one execution at the same time, exactly one gets it."""
    weid = add(db)
    results = []
    barrier = threading.Barrier(8)

    def go(i):
        barrier.wait()
        results.append((i, leases.claim(db.WorkflowExecutions, weid, f's{i}', 60)))
    threads = [threading.Thread(target=go, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    winners = [(i, r) for i, r in results if r is not None]
    assert len(winners) == 1
    i, r = winners[0]
    assert r['Status'] == 'Scheduled' and r['SchedulerID'] == f's{i}' and r['LeaseEpoch'] == 1
    # BSON dates come back as naive UTC
    assert r['LeaseExpires'] > datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(seconds=50)


def test_claim_only_pending_and_due(db):
    assert leases.claim(db.WorkflowExecutions, add(db, 'Created'), 's1', 60) is None
    later = add(db, NotBefore=datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1))
    assert leases.claim(db.WorkflowExecutions, later, 's1', 60) is None
    due = add(db, NotBefore=PAST)
    assert leases.claim(db.WorkflowExecutions, due, 's1', 60)['SchedulerID'] == 's1'
    ready = {str(r['_id']) for r in db.WorkflowExecutions.find(leases.readyFilter())}
    assert later not in ready


# --- transitions -----------------------------------------------------------------------------------

def test_transition_needs_the_current_lease(db):
    weid = add(db)
    r = leases.claim(db.WorkflowExecutions, weid, 's1', 60)
    coll = db.WorkflowExecutions
    # wrong scheduler / wrong epoch / not allowed from Scheduled
    assert leases.transition(coll, weid, 's2', r['LeaseEpoch'], 'Running') is None
    assert leases.transition(coll, weid, 's1', r['LeaseEpoch'] + 1, 'Running') is None
    assert leases.transition(coll, weid, 's1', r['LeaseEpoch'], 'Finished') is None
    db.WorkflowExecutions.update_one({'_id': bson.ObjectId(weid)}, {'$set': {'ModelServers': {'m': 'old'}, 'workflowURI': 'x', 'loggerURI': 'y'}})
    run = leases.transition(coll, weid, 's1', r['LeaseEpoch'], 'Running', incAttempts=1, leaseSeconds=60)
    assert run['Status'] == 'Running' and run['Attempts'] == 1 and run['StartDate']
    # fields of an earlier attempt are cleared
    assert run['ModelServers'] == {} and run['workflowURI'] is None and run['loggerURI'] is None
    done = leases.transition(coll, weid, 's1', r['LeaseEpoch'], 'Finished')
    assert done['Status'] == 'Finished' and done['LeaseExpires'] is None and done['SchedulerID'] == 's1' and done['EndDate']


def test_requeue_keeps_place_and_backs_off(db):
    weid = add(db, SubmittedDate='2026-01-01T10:00:00')
    coll = db.WorkflowExecutions
    r = leases.claim(coll, weid, 's1', 60)
    leases.transition(coll, weid, 's1', r['LeaseEpoch'], 'Running', incAttempts=1)
    # no free model-server slot: back to the queue, the attempt undone, a retry delay
    p = leases.transition(coll, weid, 's1', r['LeaseEpoch'], 'Pending', incAttempts=-1, backoff=BACKOFF)
    assert p['Status'] == 'Pending' and p['Attempts'] == 0 and p['SubmittedDate'] == '2026-01-01T10:00:00'
    assert p['Deferrals'] == 1 and p['LeaseExpires'] is None
    delay = (p['NotBefore'].replace(tzinfo=datetime.timezone.utc) - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
    assert 8 < delay <= 10.5
    assert leases.claim(coll, weid, 's2', 60) is None
    # the next delay doubles
    coll.update_one({'_id': bson.ObjectId(weid)}, {'$set': {'NotBefore': PAST}})
    r = leases.claim(coll, weid, 's2', 60)
    assert r['LeaseEpoch'] == 2
    p = leases.transition(coll, weid, 's2', 2, 'Pending', backoff=BACKOFF)
    delay = (p['NotBefore'].replace(tzinfo=datetime.timezone.utc) - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
    assert 18 < delay <= 20.5 and p['Deferrals'] == 2


def test_defer_once_per_window(db):
    """Of several schedulers finding the resources unavailable at once, only one counts the attempt."""
    weid = add(db)
    coll = db.WorkflowExecutions
    results = [leases.defer(coll, weid, BACKOFF) for _ in range(3)]
    assert [r is not None for r in results] == [True, False, False]
    assert rec(db, weid)['Attempts'] == 1


# --- renew and takeover ----------------------------------------------------------------------------

def test_renew_and_takeover(db):
    coll, sch = db.WorkflowExecutions, db.Schedulers
    a, b = add(db), add(db)
    ra = leases.claim(coll, a, 's1', 60)
    rb = leases.claim(coll, b, 's1', 60)
    assert leases.renew(coll, 's1', [(a, ra['LeaseEpoch']), (b, rb['LeaseEpoch'])], 60) == {a, b}
    # not expired: nothing to take over
    assert leases.takeOver(coll, sch, 's2', 60, 10) == []
    # b expired, s1 has no live heartbeat: s2 takes b over with a new epoch
    expire(db, b)
    taken = leases.takeOver(coll, sch, 's2', 60, 10)
    assert [str(t['_id']) for t in taken] == [b] and taken[0]['SchedulerID'] == 's2' and taken[0]['LeaseEpoch'] == 2
    assert taken[0]['LeaseTakenFrom'] == 's1'
    # s1 learns it lost b; its writes are refused
    assert leases.renew(coll, 's1', [(a, ra['LeaseEpoch']), (b, rb['LeaseEpoch'])], 60) == {a}
    assert leases.transition(coll, b, 's1', rb['LeaseEpoch'], 'Failed') is None


def test_renew_before_takeover_keeps_the_lease(db):
    coll, sch = db.WorkflowExecutions, db.Schedulers
    a = add(db)
    ra = leases.claim(coll, a, 's1', 60)
    expire(db, a)
    # the holder renews first (e.g. back after a short outage): nothing to take over
    assert leases.renew(coll, 's1', [(a, ra['LeaseEpoch'])], 60) == {a}
    assert leases.takeOver(coll, sch, 's2', 60, 10) == []


def test_live_holder_keeps_expired_leases(db):
    """A scheduler with a live heartbeat keeps its executions even if it could not renew them."""
    coll, sch = db.WorkflowExecutions, db.Schedulers
    assert leases.acquireIdentity(sch, 's1', 'i1', 'h', 1, 60)
    a = add(db)
    leases.claim(coll, a, 's1', 60)
    expire(db, a)
    assert leases.takeOver(coll, sch, 's2', 60, 10) == []
    # its heartbeat expires too: now it is taken over
    sch.update_one({'_id': 's1'}, {'$set': {'HeartbeatExpires': PAST}})
    assert len(leases.takeOver(coll, sch, 's2', 60, 10)) == 1


def test_legacy_records_are_taken_over(db):
    """Scheduled/Running records from before leases (no lease fields) are taken over by the first pass."""
    coll, sch = db.WorkflowExecutions, db.Schedulers
    old = add(db, 'Running', StartDate='2026-01-01T10:00:00')
    taken = leases.takeOver(coll, sch, 's1', 60, 10)
    assert [str(t['_id']) for t in taken] == [old] and taken[0]['LeaseEpoch'] == 1


def test_takeover_own(db):
    """A restarted scheduler takes back its own executions at once, only while it holds the identity."""
    coll, sch = db.WorkflowExecutions, db.Schedulers
    mine, other = add(db), add(db)
    leases.claim(coll, mine, 's1', 600)
    leases.claim(coll, other, 's2', 600)
    with pytest.raises(leases.LeaseError):
        leases.takeOverOwn(coll, sch, 's1', 'new', 60)
    assert leases.acquireIdentity(sch, 's1', 'new', 'h', 1, 60)
    taken = leases.takeOverOwn(coll, sch, 's1', 'new', 60)
    assert [str(t['_id']) for t in taken] == [mine] and taken[0]['LeaseEpoch'] == 2
    assert leases.takeOverOwn(coll, sch, 's1', 'new', 60) == []


# --- identity --------------------------------------------------------------------------------------

def test_identity(db):
    sch = db.Schedulers
    assert leases.acquireIdentity(sch, 's1', 'i1', 'h1', 1, 60)
    # a second process with the same ID is refused while the first lives
    assert not leases.acquireIdentity(sch, 's1', 'i2', 'h2', 2, 60)
    assert leases.heartbeat(sch, 's1', 'i1', 60, stats={'running': 3})
    assert leases.liveSchedulers(sch) == {'s1'}
    # after a clean shutdown (or when the heartbeat expired) the ID is free
    leases.releaseIdentity(sch, 's1', 'i1')
    assert leases.liveSchedulers(sch) == set()
    assert leases.acquireIdentity(sch, 's1', 'i2', 'h2', 2, 60)
    # the old process learns it lost the identity
    assert not leases.heartbeat(sch, 's1', 'i1', 60)
    # a process found dead (crash: identity not released, heartbeat valid) is replaced at once
    assert not leases.acquireIdentity(sch, 's1', 'i3', 'h2', 3, 60)
    assert leases.acquireIdentity(sch, 's1', 'i3', 'h2', 3, 60, replace='i2')
    assert leases.identityHolder(sch, 's1')['Instance'] == 'i3'
    # replace names one instance: no other holder is replaced
    assert not leases.acquireIdentity(sch, 's1', 'i4', 'h2', 4, 60, replace='i2')


# --- fencing ---------------------------------------------------------------------------------------

def test_check_lease(db):
    coll = db.WorkflowExecutions
    free = add(db, 'Created')
    leases.checkLease(coll, free, None)          # not held: writable without a lease
    weid = add(db)
    r = leases.claim(coll, weid, 's1', 60)
    cur = leases.formatLease(weid, 's1', r['LeaseEpoch'])
    leases.checkLease(coll, weid, cur)
    for bad in (None, leases.formatLease(weid, 's1', 99), leases.formatLease(weid, 's2', 1), leases.formatLease(free, 's1', 1)):
        with pytest.raises(leases.LeaseError):
            leases.checkLease(coll, weid, bad)
    # once released, a write naming the old lease is stale
    leases.transition(coll, weid, 's1', 1, 'Running')
    leases.transition(coll, weid, 's1', 1, 'Finished')
    with pytest.raises(leases.LeaseError):
        leases.checkLease(coll, weid, cur)
    # records from before leases (Running without a lease) stay writable
    leases.checkLease(coll, add(db, 'Running'), None)
    assert leases.parseLease('w:host:a:7') == ('w', 'host:a', 7)
