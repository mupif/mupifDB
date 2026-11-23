"""
Contract tests of the scheduler backends' lease operations.

The same scenarios run against the in-memory :class:`~mupifDB.scheduler.tests.fakes.FakeBackend`
(which the scheduler tests use) and against a backend over :mod:`mupifDB.api.leases` on a real
MongoDB (fixture ``mongo``; skipped without ``mongod``), so that the fake cannot drift from what
the REST API does.
"""

import datetime
import types

import bson
import pytest

from mupifDB.api import leases
from mupifDB.scheduler.backend import Lease, LeaseLost
from mupifDB.scheduler.tests.fakes import FakeBackend, FakeStore

BACKOFF = (10., 600., 0.)


class MongoBackend:
    """The lease operations of :class:`~mupifDB.scheduler.backend.RestBackend`, done directly with :mod:`mupifDB.api.leases`."""

    def __init__(self, db, schedulerId, leaseSeconds=600.):
        self.db, self.schedulerId, self.leaseSeconds = db, schedulerId, leaseSeconds
        self.instance = f'{schedulerId}-inst'

    def _lease(self, r):
        return Lease(weid=str(r['_id']), schedulerId=r['SchedulerID'], epoch=r['LeaseEpoch'])

    def _rec(self, r):
        return types.SimpleNamespace(**{('dbID' if k == '_id' else k): (str(v) if k == '_id' else v) for k, v in r.items()})

    def claim(self, weid):
        r = leases.claim(self.db.WorkflowExecutions, weid, self.schedulerId, self.leaseSeconds)
        if r is None:
            raise LeaseLost(weid)
        return self._rec(r), self._lease(r)

    def transition(self, lease, to, *, incAttempts=0, resetAttempts=False, backoff=None, renew=False):
        r = leases.transition(self.db.WorkflowExecutions, lease.weid, lease.schedulerId, lease.epoch, to, incAttempts=incAttempts,
                              resetAttempts=resetAttempts, backoff=backoff, leaseSeconds=(self.leaseSeconds if renew else None))
        if r is None:
            raise LeaseLost(lease.weid)
        return self._rec(r)

    def defer(self, weid, backoff):
        return leases.defer(self.db.WorkflowExecutions, weid, backoff) is not None

    def takeOver(self, limit):
        return [(self._rec(r), self._lease(r)) for r in leases.takeOver(self.db.WorkflowExecutions, self.db.Schedulers, self.schedulerId, self.leaseSeconds, limit)]

    def takeOverOwn(self):
        try:
            rs = leases.takeOverOwn(self.db.WorkflowExecutions, self.db.Schedulers, self.schedulerId, self.instance, self.leaseSeconds)
        except leases.LeaseError as e:
            raise LeaseLost(str(e))
        return [(self._rec(r), self._lease(r)) for r in rs]

    def acquireIdentity(self):
        return leases.acquireIdentity(self.db.Schedulers, self.schedulerId, self.instance, 'h', 1, self.leaseSeconds)

    def heartbeat(self, ls, stats=None, draining=None):
        if not leases.heartbeat(self.db.Schedulers, self.schedulerId, self.instance, self.leaseSeconds, stats=stats, draining=draining):
            return False, set()
        return True, leases.renew(self.db.WorkflowExecutions, self.schedulerId, [(l.weid, l.epoch) for l in ls], self.leaseSeconds)

    def releaseIdentity(self):
        leases.releaseIdentity(self.db.Schedulers, self.schedulerId, self.instance)


@pytest.fixture(params=['fake', 'mongo'])
def world(request):
    """
    Namespace: ``backend(schedulerId)`` makes a scheduler's backend over a shared database;
    ``add()``, ``status(weid)``, ``attempts(weid)``, ``expire(weid)``, ``due(weid)``, ``kill(sid)``
    set up and inspect it.
    """
    if request.param == 'fake':
        store = FakeStore()
        n = iter(range(1000))

        def add(status='Pending', **kw):
            weid = f'E{next(n)}'
            store.addExecution(weid, status=status, **kw)
            return weid
        return types.SimpleNamespace(backend=lambda sid: FakeBackend(store, sid), add=add, status=store.status,
                                     attempts=lambda w: store.executions[w].Attempts, expire=store.expire, due=store.due, kill=store.kill)
    mongo = request.getfixturevalue('mongo')
    db = mongo[f'contract_{request.node.name}'[:60].replace('[', '_').replace(']', '')]
    db.WorkflowExecutions.drop()
    db.Schedulers.drop()
    past = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)

    def one(w):
        return db.WorkflowExecutions.find_one({'_id': bson.ObjectId(w)})
    return types.SimpleNamespace(
        backend=lambda sid: MongoBackend(db, sid),
        add=lambda status='Pending', **kw: str(db.WorkflowExecutions.insert_one({'Status': status, 'Attempts': 0, **kw}).inserted_id),
        status=lambda w: one(w)['Status'], attempts=lambda w: one(w)['Attempts'],
        expire=lambda w: db.WorkflowExecutions.update_one({'_id': bson.ObjectId(w)}, {'$set': {'LeaseExpires': past}}),
        due=lambda w: db.WorkflowExecutions.update_one({'_id': bson.ObjectId(w)}, {'$set': {'NotBefore': past}}),
        kill=lambda sid: db.Schedulers.update_one({'_id': sid}, {'$set': {'HeartbeatExpires': past}}))


def test_claim_once(world):
    s1, s2 = world.backend('s1'), world.backend('s2')
    w = world.add()
    rec, lease = s1.claim(w)
    assert rec.Status == 'Scheduled' and lease == Lease(w, 's1', 1)
    with pytest.raises(LeaseLost):
        s2.claim(w)


def test_transitions_need_the_lease(world):
    s1, s2 = world.backend('s1'), world.backend('s2')
    w = world.add()
    _, lease = s1.claim(w)
    with pytest.raises(LeaseLost):
        s2.transition(Lease(w, 's2', 1), 'Running')
    with pytest.raises(LeaseLost):
        s1.transition(lease, 'Finished')          # not from Scheduled
    assert s1.transition(lease, 'Running', incAttempts=1, renew=True).Status == 'Running'
    assert world.attempts(w) == 1
    assert s1.transition(lease, 'Finished').Status == 'Finished'
    with pytest.raises(LeaseLost):
        s1.transition(lease, 'Failed')            # released


def test_requeue_with_delay(world):
    s1 = world.backend('s1')
    w = world.add()
    _, lease = s1.claim(w)
    s1.transition(lease, 'Running', incAttempts=1)
    p = s1.transition(lease, 'Pending', incAttempts=-1, backoff=BACKOFF)
    assert p.Status == 'Pending' and world.attempts(w) == 0
    with pytest.raises(LeaseLost):
        s1.claim(w)                               # not due
    world.due(w)
    _, lease2 = s1.claim(w)
    assert lease2.epoch == 2


def test_defer_once(world):
    s1, s2 = world.backend('s1'), world.backend('s2')
    w = world.add()
    assert s1.defer(w, BACKOFF) is True
    assert s2.defer(w, BACKOFF) is False
    assert world.attempts(w) == 1


def test_heartbeat_takeover(world):
    s1, s2 = world.backend('s1'), world.backend('s2')
    assert s1.acquireIdentity()
    w = world.add()
    _, lease = s1.claim(w)
    assert s1.heartbeat([lease]) == (True, {w})
    world.expire(w)
    assert s2.takeOver(10) == []                  # s1 lives
    world.kill('s1')
    taken = s2.takeOver(10)
    assert [l for _, l in taken] == [Lease(w, 's2', 2)]
    assert s1.heartbeat([lease]) == (True, set())  # s1 (back) learns it lost the execution
    with pytest.raises(LeaseLost):
        s1.transition(lease, 'Failed')


def test_identity_and_own_takeover(world):
    old, new, dup = world.backend('s1'), world.backend('s1'), world.backend('s1')
    new.instance, dup.instance = 's1-new', 's1-dup'
    assert old.acquireIdentity()
    w = world.add()
    old.claim(w)
    assert not new.acquireIdentity()              # the old process lives
    world.kill('s1')
    assert new.acquireIdentity()
    assert not dup.acquireIdentity()
    assert [l.epoch for _, l in new.takeOverOwn()] == [2]
    assert old.heartbeat([]) == (False, set())    # the old process lost the identity
    with pytest.raises(LeaseLost):
        dup.takeOverOwn()
