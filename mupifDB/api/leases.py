"""
Leases of workflow executions, for several schedulers running at once.

Several schedulers (normally on several hosts) take pending executions from the same database. An
execution is taken by exactly one of them, under a **lease**: fields of the execution record

* ``SchedulerID`` -- the scheduler holding (or, after release, last holding) the execution;
* ``LeaseEpoch`` -- increased by every claim and takeover; ``(SchedulerID, LeaseEpoch)`` is the
  *fencing token*: every write made under the lease names it, and the API refuses writes naming
  an older one (:func:`checkLease`), so a scheduler (or its workflow) which lost the lease cannot
  overwrite the work of the new holder;
* ``LeaseExpires`` -- until when the lease is valid; renewed periodically by its holder
  (:func:`renew`); an expired lease may be taken over by another scheduler (:func:`takeOver`);
* ``NotBefore`` -- the execution is not claimed before this time (retry delay after a transient
  failure, :func:`defer`); ``Deferrals`` -- consecutive delays, from which the next delay grows;
* ``LeaseInstance`` -- process which took the execution over at its start (:func:`takeOverOwn`).

Schedulers register in the collection ``Schedulers`` (:func:`acquireIdentity`, :func:`heartbeat`):
an identity (``scheduler_id``) is held by one process (instance) at a time, and its heartbeat
tells others that the scheduler lives -- leases of a live scheduler are never taken over, even if
it could not renew them for a while.

Every operation is a single atomic MongoDB update with a condition, so concurrent schedulers
cannot both succeed. Times are taken from the database server (``$$NOW``), never from the clocks
of the schedulers or API processes; schedulers only deal in durations. The functions take the
collections as arguments so that they are tested against a real database without the REST API.

Records written before leases existed have none of these fields: a ``Scheduled``/``Running``
execution without ``LeaseExpires`` counts as expired (taken over by the first takeover pass).
Dates of the record which existed before (``ScheduledDate``, ``StartDate``, ...) keep their
format (ISO strings of the API host's local time); only the lease fields are BSON dates.
"""

from __future__ import annotations

import datetime
from typing import Any, Iterable, Optional

import bson.objectid
import pymongo
import pymongo.errors
from pymongo import ReturnDocument
from pymongo.write_concern import WriteConcern

#: statuses of executions held under a lease
LEASED = ('Scheduled', 'Running')
#: transition target -> statuses it may be made from (by the lease holder)
ALLOWED_FROM = {
    'Running': ('Scheduled',),
    'Pending': ('Scheduled', 'Running'),
    'Finished': ('Running',),
    'Failed': ('Scheduled', 'Running'),
    'Created': ('Scheduled',),
}
#: a date before any lease (stands for a missing ``LeaseExpires``/``NotBefore`` in comparisons)
_EPOCH0 = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
#: fields only the lease operations may write (refused by the generic parameter setter)
LEASE_FIELDS = ('SchedulerID', 'LeaseEpoch', 'LeaseExpires', 'LeaseInstance', 'LeaseTakenFrom', 'NotBefore', 'Deferrals')


class LeaseError(Exception):
    """A lease condition failed: the execution is not (any more) held under the given lease, or cannot be claimed."""


def majority(coll: pymongo.collection.Collection) -> pymongo.collection.Collection:
    """
    Return *coll* with write concern ``majority``.

    With a replica set, an acknowledged ``w:1`` write may be rolled back after a failover; a
    claim rolled back that way could be made again by another scheduler while the first one runs.
    """
    return coll.with_options(write_concern=WriteConcern('majority'))


def _oid(weid: str) -> bson.objectid.ObjectId:
    return bson.objectid.ObjectId(weid)


def _lit(v: Any) -> dict:
    """Wrap a value for an update pipeline (a string starting with ``$`` would be read as a field path)."""
    return {'$literal': v}


def _nowPlus(seconds: float) -> dict:
    """Pipeline expression: database time + *seconds*."""
    return {'$add': ['$$NOW', int(seconds * 1000)]}


def _expired(field: str) -> dict:
    """Filter expression: *field* is missing/None or not after the database time."""
    return {'$lte': [{'$ifNull': ['$' + field, _EPOCH0]}, '$$NOW']}


def _backoff(base: float, maximum: float, jitter: float) -> dict:
    """
    Pipeline expression: retry delay in milliseconds, growing with ``Deferrals``.

    ``min(base * 2**Deferrals, maximum)``, varied by ±*jitter* (random, so that executions
    failing together are not retried together).
    """
    grow = {'$multiply': [base * 1000., {'$pow': [2, {'$ifNull': ['$Deferrals', 0]}]}]}
    capped = {'$min': [grow, maximum * 1000.]}
    return {'$toLong': {'$multiply': [capped, {'$add': [1. - jitter, {'$multiply': [2. * jitter, {'$rand': {}}]}]}]}}


def _localNow() -> str:
    """Date string as the API stores the older date fields (local time, ISO format)."""
    return datetime.datetime.now().isoformat()


# --- executions ------------------------------------------------------------------------------------

def claim(coll, weid: str, schedulerId: str, leaseSeconds: float) -> Optional[dict]:
    """
    Take a pending execution under a new lease (``Pending -> Scheduled``).

    Succeeds only if the execution is ``Pending`` and due (``NotBefore`` passed or not set); of
    several schedulers claiming it at once, exactly one succeeds.

    :return: the updated record, or None if it could not be claimed (taken by another scheduler,
             not pending, not due, not found)
    """
    return majority(coll).find_one_and_update(
        {'_id': _oid(weid), 'Status': 'Pending', '$expr': _expired('NotBefore')},
        [{'$set': {
            'Status': 'Scheduled',
            'ScheduledDate': _lit(_localNow()),
            'SchedulerID': _lit(schedulerId),
            'LeaseEpoch': {'$add': [{'$ifNull': ['$LeaseEpoch', 0]}, 1]},
            'LeaseExpires': _nowPlus(leaseSeconds),
        }}],
        return_document=ReturnDocument.AFTER)


def transition(coll, weid: str, schedulerId: str, epoch: int, to: str, *, incAttempts: int = 0, resetAttempts: bool = False,
               backoff: Optional[tuple[float, float, float]] = None, leaseSeconds: Optional[float] = None) -> Optional[dict]:
    """
    Change the status of an execution held under the given lease.

    One conditional update: it succeeds only if ``(SchedulerID, LeaseEpoch)`` is the given lease
    and the current status may change to *to* (:data:`ALLOWED_FROM`).

    * ``Running``: ``StartDate`` set; the fields one run of the workflow writes (log, model
      servers, workflow and logger URIs) are cleared, so values of an earlier attempt do not count;
      the lease is renewed (``leaseSeconds``).
    * ``Finished``/``Failed``: ``EndDate`` set; the lease and ``Deferrals`` are cleared.
    * ``Pending``: returned to the queue keeping its place (``SubmittedDate`` unchanged); with
      *backoff* ``(base, maximum, jitter)`` it is not claimed again before the retry delay
      (``NotBefore``; ``Deferrals`` + 1); the lease is cleared.
    * ``Created``: back to the user (dates, log and lease cleared).

    :param incAttempts: added to ``Attempts`` (e.g. +1 when the workflow is launched, -1 to undo
                        it for a run which found no free model-server slot)
    :param resetAttempts: set ``Attempts`` to 0
    :return: the updated record, or None if the lease is not held (any more) or the transition is
             not allowed from the current status
    """
    if to not in ALLOWED_FROM:
        raise ValueError(f'Invalid transition target {to!r}')
    now = _localNow()
    s: dict[str, Any] = {'Status': _lit(to)}
    release = {'LeaseExpires': None}
    if to == 'Running':
        s.update({'StartDate': _lit(now), 'EndDate': None, 'ExecutionLog': None, 'ModelServers': _lit({}),
                  'workflowURI': None, 'loggerURI': None})
        if leaseSeconds:
            s['LeaseExpires'] = _nowPlus(leaseSeconds)
    elif to in ('Finished', 'Failed'):
        s.update({'EndDate': _lit(now), 'Deferrals': 0, 'NotBefore': None, **release})
    elif to == 'Pending':
        s.update(release)
        if backoff is not None:
            s['NotBefore'] = {'$add': ['$$NOW', _backoff(*backoff)]}
            s['Deferrals'] = {'$add': [{'$ifNull': ['$Deferrals', 0]}, 1]}
    elif to == 'Created':
        s.update({'SubmittedDate': None, 'ScheduledDate': None, 'StartDate': None, 'EndDate': None, 'ExecutionLog': None,
                  'Deferrals': 0, 'NotBefore': None, **release})
    if resetAttempts:
        s['Attempts'] = 0
    elif incAttempts:
        s['Attempts'] = {'$max': [0, {'$add': [{'$ifNull': ['$Attempts', 0]}, incAttempts]}]}
    # the backoff reads Deferrals before this update ($set stages see the old document)
    return majority(coll).find_one_and_update(
        {'_id': _oid(weid), 'SchedulerID': schedulerId, 'LeaseEpoch': int(epoch), 'Status': {'$in': list(ALLOWED_FROM[to])}},
        [{'$set': s}],
        return_document=ReturnDocument.AFTER)


def renew(coll, schedulerId: str, leases: Iterable[tuple[str, int]], leaseSeconds: float) -> set[str]:
    """
    Extend the leases of executions held by *schedulerId*.

    :param leases: ``(execution ID, epoch)`` of the leases to renew
    :return: IDs of the executions still held under these leases (renewed); the others were taken
             over or released meanwhile -- their workflows must be stopped without writing anything
    """
    leases = list(leases)
    if not leases:
        return set()
    cond = {'SchedulerID': schedulerId, 'Status': {'$in': list(LEASED)},
            '$or': [{'_id': _oid(w), 'LeaseEpoch': int(e)} for w, e in leases]}
    majority(coll).update_many(cond, [{'$set': {'LeaseExpires': _nowPlus(leaseSeconds)}}])
    return {str(r['_id']) for r in coll.find(cond, {'_id': 1})}


def takeOver(coll, schedulers, schedulerId: str, leaseSeconds: float, limit: int) -> list[dict]:
    """
    Take over executions whose lease expired, held by schedulers which are not alive (or by the caller itself).

    An execution qualifies if it is ``Scheduled``/``Running``, its lease expired (or it has none:
    records from before leases), and its holder has no live heartbeat (or no registry entry): a
    live scheduler which could not renew in time keeps its executions. Each one is taken in its own
    atomic update (new epoch), so two schedulers never take the same execution.

    The caller's own expired leases qualify too, although it is alive: a scheduler renews the lease
    of every execution it still runs, so one of its leases expires only when no job holds it any
    more -- the job ended without storing its outcome (e.g. the REST API was unreachable at that
    moment) or the scheduler gave up on it. Without this such an execution would stay
    ``Scheduled``/``Running`` until the scheduler restarts, as nobody else may take the executions
    of a live scheduler. (The scheduler calls this only after reaching the database without
    interruption for a whole lease period, so it has renewed every lease it still holds; see
    :class:`mupifDB.scheduler.leases.LeaseKeeper`.)

    The caller must clean up after the previous holder (its model-server jobs, using the record's
    ``loggerURI``) and return the execution to the queue (:func:`transition` to ``Pending``).

    :param schedulers: the ``Schedulers`` collection (heartbeats)
    :return: the records taken over (at most *limit*)
    """
    live = liveSchedulers(schedulers)
    taken = []
    while len(taken) < limit:
        rec = majority(coll).find_one_and_update(
            {'Status': {'$in': list(LEASED)}, 'SchedulerID': {'$nin': sorted(live - {schedulerId})}, '$expr': _expired('LeaseExpires')},
            [{'$set': {
                # the previous holder, for the record (expressions of one $set stage read the old document)
                'LeaseTakenFrom': {'$ifNull': ['$SchedulerID', None]},
                'SchedulerID': _lit(schedulerId),
                'LeaseEpoch': {'$add': [{'$ifNull': ['$LeaseEpoch', 0]}, 1]},
                'LeaseExpires': _nowPlus(leaseSeconds),
            }}],
            return_document=ReturnDocument.AFTER)
        if rec is None:
            break
        taken.append(rec)
    return taken


def takeOverOwn(coll, schedulers, schedulerId: str, instance: str, leaseSeconds: float) -> list[dict]:
    """
    Take over the executions left by an earlier process of this scheduler (same ``scheduler_id``), at its start.

    Only while *instance* holds the identity (:func:`acquireIdentity`): otherwise another live
    process with the same ID could lose its executions. Their leases need not have expired.

    :return: the records taken over (new epoch)
    """
    if schedulers.find_one({'_id': schedulerId, 'Instance': instance}) is None:
        raise LeaseError(f'Scheduler identity {schedulerId} is not held by instance {instance}')
    taken = []
    while True:
        rec = majority(coll).find_one_and_update(
            {'Status': {'$in': list(LEASED)}, 'SchedulerID': schedulerId, 'LeaseInstance': {'$ne': instance}},
            [{'$set': {
                'LeaseEpoch': {'$add': [{'$ifNull': ['$LeaseEpoch', 0]}, 1]},
                'LeaseExpires': _nowPlus(leaseSeconds),
                'LeaseInstance': _lit(instance),
            }}],
            return_document=ReturnDocument.AFTER)
        if rec is None:
            return taken
        taken.append(rec)


def defer(coll, weid: str, backoff: tuple[float, float, float]) -> Optional[dict]:
    """
    Postpone a pending execution whose resources are unavailable, counting an attempt.

    Succeeds only if the execution is ``Pending`` and due, so of several schedulers finding its
    model servers unreachable in the same window only one counts the attempt; ``NotBefore`` is set
    to the retry delay (growing with ``Deferrals``, see :func:`transition`).

    :return: the updated record, or None if another scheduler deferred (or claimed) it meanwhile
    """
    return majority(coll).find_one_and_update(
        {'_id': _oid(weid), 'Status': 'Pending', '$expr': _expired('NotBefore')},
        [{'$set': {
            'Attempts': {'$add': [{'$ifNull': ['$Attempts', 0]}, 1]},
            'NotBefore': {'$add': ['$$NOW', _backoff(*backoff)]},
            'Deferrals': {'$add': [{'$ifNull': ['$Deferrals', 0]}, 1]},
        }}],
        return_document=ReturnDocument.AFTER)


def readyFilter() -> dict:
    """Filter of pending executions which are due (``NotBefore`` passed or not set)."""
    return {'Status': 'Pending', '$expr': _expired('NotBefore')}


def checkLease(coll, weid: str, lease: Optional[str]) -> None:
    """
    Check that a write to an execution is made under its current lease (fencing).

    While an execution is held under a lease (``Scheduled``/``Running`` with a holder), writes to
    it must name that lease; the header ``X-MupifDB-Lease: <weid>:<scheduler_id>:<epoch>`` carries
    it. Executions not held under a lease may be written without it (``--standalone`` runs, users).
    A write naming a lease is refused if the lease is not current, whatever the status (a stale
    workflow writing outputs after its execution was re-run and finished).

    :param lease: value of the header (None: not given)
    :raises LeaseError: if the write must be refused
    """
    rec = coll.find_one({'_id': _oid(weid)}, {'Status': 1, 'SchedulerID': 1, 'LeaseEpoch': 1, 'LeaseExpires': 1})
    if rec is None:
        return
    held = rec.get('Status') in LEASED and rec.get('SchedulerID') is not None and rec.get('LeaseExpires') is not None
    if lease is None:
        if held:
            raise LeaseError(f'Execution {weid} is held by scheduler {rec["SchedulerID"]} (epoch {rec.get("LeaseEpoch")}): writes must name its lease')
        return
    lw, ls, le = parseLease(lease)
    if lw != str(weid):
        raise LeaseError(f'Lease {lease} is not a lease of execution {weid}')
    if rec.get('SchedulerID') != ls or int(rec.get('LeaseEpoch') or 0) != le or rec.get('Status') not in LEASED:
        raise LeaseError(f'Lease {lease} of execution {weid} is not current (held by {rec.get("SchedulerID")}, epoch {rec.get("LeaseEpoch")}, {rec.get("Status")})')


def formatLease(weid: str, schedulerId: str, epoch: int) -> str:
    """Return the header value naming a lease: ``<weid>:<scheduler_id>:<epoch>``."""
    return f'{weid}:{schedulerId}:{int(epoch)}'


def parseLease(lease: str) -> tuple[str, str, int]:
    """Split a lease header value (see :func:`formatLease`); the scheduler ID may contain ``:``."""
    weid, rest = lease.split(':', 1)
    schedulerId, epoch = rest.rsplit(':', 1)
    return weid, schedulerId, int(epoch)


def ensureIndexes(coll, schedulers) -> None:
    """Create the indexes the lease queries use (idempotent)."""
    coll.create_index([('Status', 1), ('SubmittedDate', 1)])
    coll.create_index([('Status', 1), ('LeaseExpires', 1)])
    coll.create_index([('SchedulerID', 1), ('Status', 1)])
    schedulers.create_index([('HeartbeatExpires', 1)])


# --- schedulers (identity, heartbeat) --------------------------------------------------------------

def acquireIdentity(schedulers, schedulerId: str, instance: str, host: str, pid: int, heartbeatSeconds: float, replace: Optional[str] = None) -> bool:
    """
    Take the scheduler identity *schedulerId* for process *instance*, unless another live process holds it.

    A scheduler started twice with the same ID (e.g. configured on two hosts) would take over the
    other's executions; the second one must refuse to start.

    :param replace: take the identity also if it is held by this instance (whose heartbeat has not
                    expired): the caller found that process dead (same host, its process gone; a
                    crash or kill -9 does not release the identity), see :func:`identityHolder`
    :return: whether the identity is held by *instance* now
    """
    fields = {'Instance': _lit(instance), 'Host': _lit(host), 'Pid': int(pid), 'StartedAt': '$$NOW',
              'HeartbeatExpires': _nowPlus(heartbeatSeconds), 'Draining': False}
    # a new identity: the insert succeeds for one process only (unique _id); MongoDB does not allow
    # the $expr condition below in an upsert, hence two steps
    try:
        majority(schedulers).insert_one({'_id': schedulerId, 'Instance': None, 'HeartbeatExpires': _EPOCH0})
    except pymongo.errors.DuplicateKeyError:
        pass
    # take it if free (heartbeat expired) or already ours
    holders = [{'Instance': instance}, {'$expr': _expired('HeartbeatExpires')}] + ([{'Instance': replace}] if replace else [])
    rec = majority(schedulers).find_one_and_update({'_id': schedulerId, '$or': holders}, [{'$set': fields}], return_document=ReturnDocument.AFTER)
    return rec is not None and rec.get('Instance') == instance


def identityHolder(schedulers, schedulerId: str) -> Optional[dict]:
    """Return the registry entry of a scheduler identity (``Instance``, ``Host``, ``Pid``, ``HeartbeatExpires``), or None."""
    return schedulers.find_one({'_id': schedulerId})


def heartbeat(schedulers, schedulerId: str, instance: str, heartbeatSeconds: float, stats: Optional[dict] = None,
              draining: Optional[bool] = None) -> bool:
    """
    Renew the heartbeat of a scheduler (and store its statistics).

    :return: whether *instance* still holds the identity (False: another process took it over --
             this one was considered dead and must stop)
    """
    s: dict[str, Any] = {'HeartbeatExpires': _nowPlus(heartbeatSeconds)}
    if stats is not None:
        s['Stats'] = _lit(stats)
    if draining is not None:
        s['Draining'] = bool(draining)
    r = majority(schedulers).update_one({'_id': schedulerId, 'Instance': instance}, [{'$set': s}])
    return r.matched_count == 1


def releaseIdentity(schedulers, schedulerId: str, instance: str) -> None:
    """Give up the identity at a clean shutdown: the heartbeat ends now (the entry stays, for the record)."""
    majority(schedulers).update_one({'_id': schedulerId, 'Instance': instance}, [{'$set': {'HeartbeatExpires': '$$NOW'}}])


def liveSchedulers(schedulers) -> set[str]:
    """Return the IDs of schedulers whose heartbeat has not expired."""
    return {r['_id'] for r in schedulers.find({'$expr': {'$gt': ['$HeartbeatExpires', '$$NOW']}}, {'_id': 1})}
