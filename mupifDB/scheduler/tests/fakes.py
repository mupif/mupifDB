"""
Test doubles for the scheduler: an in-memory database of executions and workflows.

:class:`FakeStore` holds the records (shared by several schedulers); :class:`FakeBackend` is one
scheduler's :class:`mupifDB.scheduler.backend.ExecutionBackend` over it. The lease operations follow
:mod:`mupifDB.api.leases` (checked by the contract tests in ``test_backend_contract.py``, which run
the same scenarios against a real MongoDB). The store records every status change, can be told to
fail, and has a controllable clock; it is thread-safe, since schedulers call it from job threads.
"""

import itertools
import random
import threading
import time
import types
import uuid
from pathlib import Path
from typing import Any, Callable, Optional

from mupifDB.api.leases import ALLOWED_FROM, LEASED
from mupifDB.scheduler.backend import Lease, LeaseLost


class FakeStore:
    """The shared in-memory database: executions, workflows, the scheduler registry."""

    def __init__(self, clock: Callable[[], float] = time.monotonic):
        """
        Start with no executions and no workflows.

        :param clock: the database's clock (seconds); lease and retry times are computed with it
        """
        self.lock = threading.RLock()
        self.clock = clock
        #: execution ID -> record (SimpleNamespace with dbID, Status, Attempts, WorkflowID, WorkflowVersion, submitted, lease fields)
        self.executions: dict[str, Any] = {}
        #: (workflow ID, version) -> record (SimpleNamespace with modulename, GridFSID, Models)
        self.workflows: dict[tuple[str, int], Any] = {}
        #: scheduler ID -> {'Instance': ..., 'HeartbeatExpires': clock time}
        self.schedulers: dict[str, dict] = {}
        #: every status change, in order: (execution ID, status)
        self.history: list[tuple[str, str]] = []
        #: execution ID -> content of the stored log
        self.logs: dict[str, bytes] = {}
        #: execution IDs whose owner was notified, in order
        self.notified: list[str] = []
        #: execution IDs whose compulsory inputs are not all set
        self.incomplete: set[str] = set()
        #: method name -> function(*args) returning an exception to raise, or None
        self.failures: dict[str, Callable[..., Exception | None]] = {}
        self._submitted = itertools.count()

    # --- setup ---------------------------------------------------------------------------------

    def addWorkflow(self, wid: str = 'wf', version: int = 1, jobmanagers: tuple[str, ...] = ('CVUT.demo01',), modulename: str = 'fakewf',
                    profiles: list[dict] | None = None, templates: tuple[str, ...] = ()) -> Any:
        """
        Add a workflow whose models (``m0``, ``m1``, ...) use the given jobmanagers.

        :param profiles: execution profiles, as in the workflow metadata (dicts)
        :param templates: names of the models marked ``Instantiate: false`` (allocated by the workflow later)
        :return: the workflow record
        """
        rec = types.SimpleNamespace(wid=wid, Version=version, modulename=modulename, GridFSID=f'file-{wid}-{version}',
                                    Models=[types.SimpleNamespace(Name=f'm{i}', Jobmanager=jm, ModelID=None, Instantiate=(False if f'm{i}' in templates else None))
                                            for i, jm in enumerate(jobmanagers)],
                                    ExecutionProfiles=profiles or [])
        self.workflows[(wid, version)] = rec
        return rec

    def addExecution(self, weid: str, wid: str = 'wf', version: int = 1, status: str = 'Pending', attempts: int = 0, loggerURI: str | None = None,
                     profile: int = -1, **lease) -> Any:
        """
        Add an execution; executions are submitted in the order they are added.

        :param profile: execution profile index (-1: the default profile)
        :param lease: lease fields to set (e.g. ``SchedulerID``, ``LeaseEpoch``, ``LeaseExpires``)
        :return: the execution record
        """
        rec = types.SimpleNamespace(dbID=weid, Status=status, Attempts=attempts, WorkflowID=wid, WorkflowVersion=version, submitted=next(self._submitted),
                                    loggerURI=loggerURI, ExecutionProfileIndex=profile, ModelServers={}, workflowURI=None,
                                    SchedulerID=None, LeaseEpoch=0, LeaseExpires=None, LeaseInstance=None, LeaseTakenFrom=None, NotBefore=None, Deferrals=0)
        for k, v in lease.items():
            setattr(rec, k, v)
        self.executions[weid] = rec
        return rec

    def setModelServers(self, weid: str, modelServers: dict[str, str]) -> None:
        """Store the model servers of an execution, as the execution script does once its models are allocated."""
        with self.lock:
            self.executions[weid].ModelServers = dict(modelServers)

    def status(self, weid: str) -> str:
        """Return the current status of an execution."""
        return self.executions[weid].Status

    def statuses(self, weid: str) -> list[str]:
        """All statuses an execution went through, in order (excluding the initial one)."""
        with self.lock:
            return [s for w, s in self.history if w == weid]

    def expire(self, weid: str) -> None:
        """Let the lease of an execution expire now."""
        with self.lock:
            self.executions[weid].LeaseExpires = self.clock() - 1

    def due(self, weid: str) -> None:
        """Let the retry delay of an execution pass now."""
        with self.lock:
            self.executions[weid].NotBefore = None

    def kill(self, schedulerId: str) -> None:
        """Let the heartbeat of a scheduler expire now (it died)."""
        with self.lock:
            if schedulerId in self.schedulers:
                self.schedulers[schedulerId]['HeartbeatExpires'] = self.clock() - 1

    # --- helpers -------------------------------------------------------------------------------

    def maybeFail(self, method: str, *args) -> None:
        """Raise the exception configured for *method*, if any."""
        f = self.failures.get(method)
        if f is not None and (exc := f(*args)) is not None:
            raise exc

    @staticmethod
    def copy(rec: Any) -> Any:
        """Return a copy of a record, as the REST API returns a fresh object every time."""
        return types.SimpleNamespace(**vars(rec))

    def setStatus(self, rec: Any, status: str) -> None:
        rec.Status = status
        self.history.append((rec.dbID, status))

    def held(self, rec: Any) -> bool:
        """Whether the execution is held under a lease (writes must name it)."""
        return rec.Status in LEASED and rec.SchedulerID is not None and rec.LeaseExpires is not None

    def backoffDelay(self, rec: Any, backoff) -> float:
        base, maximum, jitter = backoff
        return min(base * 2 ** (rec.Deferrals or 0), maximum) * (1 + random.uniform(-jitter, jitter))


class FakeBackend:
    """
    One scheduler's :class:`~mupifDB.scheduler.backend.ExecutionBackend` over a :class:`FakeStore`.

    Several backends with different *schedulerId* over one store stand for several schedulers
    sharing a database. Attributes not defined here (``executions``, ``history``, ``addExecution``,
    ...) are those of the store.
    """

    def __init__(self, store: Optional[FakeStore] = None, schedulerId: str = 's1', leaseSeconds: float = 600.):
        self.store = store if store is not None else FakeStore()
        self.schedulerId = schedulerId
        self.leaseSeconds = leaseSeconds
        self.instance = f'{schedulerId}-{uuid.uuid4().hex[:8]}'
        #: host and process the identity is registered with
        self.host, self.pid = 'testhost', 1

    def __getattr__(self, name):
        # only called for attributes not found on the backend: the store's (setup and inspection)
        return getattr(self.__dict__['store'], name)

    @property
    def lock(self):
        return self.store.lock

    def _now(self) -> float:
        return self.store.clock()

    def _lease(self, rec: Any) -> Lease:
        return Lease(weid=rec.dbID, schedulerId=rec.SchedulerID, epoch=rec.LeaseEpoch)

    def _current(self, rec: Any, lease: Lease) -> bool:
        return rec.SchedulerID == lease.schedulerId and rec.LeaseEpoch == lease.epoch

    # --- reading -------------------------------------------------------------------------------

    def pendingExecutions(self, limit: int) -> list[Any]:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.pendingExecutions`: due pending executions, oldest first."""
        with self.lock:
            self.store.maybeFail('pendingExecutions', limit)
            now = self._now()
            recs = sorted((r for r in self.executions.values() if r.Status == 'Pending' and (r.NotBefore is None or r.NotBefore <= now)),
                          key=lambda r: r.submitted)
            return [self.store.copy(r) for r in recs[:limit]]

    def executionsWithStatus(self, status: str, limit: int) -> list[Any]:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.executionsWithStatus`."""
        with self.lock:
            self.store.maybeFail('executionsWithStatus', status, limit)
            return [self.store.copy(r) for r in self.executions.values() if r.Status == status][:limit]

    def getExecution(self, weid: str) -> Any:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.getExecution`."""
        with self.lock:
            self.store.maybeFail('getExecution', weid)
            return self.store.copy(self.executions[weid])

    def getWorkflow(self, wid: str, version: int) -> Any:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.getWorkflow`."""
        with self.lock:
            self.store.maybeFail('getWorkflow', wid, version)
            return self.workflows[(wid, int(version))]

    def inputsComplete(self, weid: str) -> bool:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.inputsComplete`; executions in ``incomplete`` lack inputs."""
        with self.lock:
            self.store.maybeFail('inputsComplete', weid)
            return weid not in self.incomplete

    def statusCounts(self) -> dict[str, int]:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.statusCounts`."""
        with self.lock:
            self.store.maybeFail('statusCounts')
            counts: dict[str, int] = {}
            for r in self.executions.values():
                counts[r.Status] = counts.get(r.Status, 0) + 1
            return counts

    # --- writing -------------------------------------------------------------------------------

    def storeLog(self, weid: str, path: Path, lease: Optional[Lease] = None) -> None:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.storeLog`; fenced like the REST API."""
        with self.lock:
            self.store.maybeFail('storeLog', weid, path)
            rec = self.executions[weid]
            if lease is None and self.store.held(rec) or lease is not None and not (self._current(rec, lease) and rec.Status in LEASED):
                raise LeaseLost(f'{weid}: log refused, lease {lease} is not current')
            self.logs[weid] = Path(path).read_bytes()

    def notifyOwner(self, weid: str) -> None:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.notifyOwner`."""
        with self.lock:
            self.notified.append(weid)

    # --- leases (as mupifDB.api.leases) --------------------------------------------------------

    def claim(self, weid: str) -> tuple[Any, Lease]:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.claim`."""
        with self.lock:
            self.store.maybeFail('claim', weid)
            rec = self.executions.get(weid)
            now = self._now()
            if rec is None or rec.Status != 'Pending' or (rec.NotBefore is not None and rec.NotBefore > now):
                raise LeaseLost(f'{weid} cannot be claimed')
            rec.SchedulerID, rec.LeaseEpoch, rec.LeaseExpires = self.schedulerId, rec.LeaseEpoch + 1, now + self.leaseSeconds
            self.store.setStatus(rec, 'Scheduled')
            return self.store.copy(rec), self._lease(rec)

    def transition(self, lease: Lease, to: str, *, incAttempts: int = 0, resetAttempts: bool = False, backoff=None, renew: bool = False) -> Any:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.transition`."""
        with self.lock:
            self.store.maybeFail('transition', lease.weid, to)
            rec = self.executions[lease.weid]
            if not self._current(rec, lease) or rec.Status not in ALLOWED_FROM[to]:
                raise LeaseLost(f'{lease.weid} is not held under {lease.header()} ({rec.Status}, {rec.SchedulerID}:{rec.LeaseEpoch}), or cannot change to {to}')
            now = self._now()
            if to == 'Running':
                rec.ModelServers, rec.workflowURI, rec.loggerURI = {}, None, None
                if renew:
                    rec.LeaseExpires = now + self.leaseSeconds
            elif to in ('Finished', 'Failed'):
                rec.LeaseExpires, rec.Deferrals, rec.NotBefore = None, 0, None
            elif to == 'Pending':
                rec.LeaseExpires = None
                if backoff is not None:
                    rec.NotBefore = now + self.store.backoffDelay(rec, backoff)
                    rec.Deferrals += 1
            elif to == 'Created':
                rec.LeaseExpires, rec.Deferrals, rec.NotBefore = None, 0, None
            if resetAttempts:
                rec.Attempts = 0
            elif incAttempts:
                rec.Attempts = max(0, rec.Attempts + incAttempts)
            self.store.setStatus(rec, to)
            return self.store.copy(rec)

    def defer(self, weid: str, backoff) -> bool:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.defer`."""
        with self.lock:
            self.store.maybeFail('defer', weid)
            rec = self.executions[weid]
            now = self._now()
            if rec.Status != 'Pending' or (rec.NotBefore is not None and rec.NotBefore > now):
                return False
            rec.Attempts += 1
            rec.NotBefore = now + self.store.backoffDelay(rec, backoff)
            rec.Deferrals += 1
            return True

    def _live(self) -> set[str]:
        now = self._now()
        return {sid for sid, r in self.schedulers.items() if r['HeartbeatExpires'] > now}

    def takeOver(self, limit: int) -> list[tuple[Any, Lease]]:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.takeOver`."""
        with self.lock:
            self.store.maybeFail('takeOver', limit)
            now, live, taken = self._now(), self._live(), []
            for rec in sorted(self.executions.values(), key=lambda r: r.submitted):
                if len(taken) >= limit:
                    break
                if rec.Status in LEASED and (rec.SchedulerID not in live or rec.SchedulerID == self.schedulerId) and (rec.LeaseExpires is None or rec.LeaseExpires <= now):
                    rec.LeaseTakenFrom = rec.SchedulerID
                    rec.SchedulerID, rec.LeaseEpoch, rec.LeaseExpires = self.schedulerId, rec.LeaseEpoch + 1, now + self.leaseSeconds
                    taken.append((self.store.copy(rec), self._lease(rec)))
            return taken

    def takeOverOwn(self) -> list[tuple[Any, Lease]]:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.takeOverOwn`."""
        with self.lock:
            if self.schedulers.get(self.schedulerId, {}).get('Instance') != self.instance:
                raise LeaseLost(f'Scheduler identity {self.schedulerId} is not held by {self.instance}')
            now, taken = self._now(), []
            for rec in self.executions.values():
                if rec.Status in LEASED and rec.SchedulerID == self.schedulerId and rec.LeaseInstance != self.instance:
                    rec.LeaseEpoch, rec.LeaseExpires, rec.LeaseInstance = rec.LeaseEpoch + 1, now + self.leaseSeconds, self.instance
                    taken.append((self.store.copy(rec), self._lease(rec)))
            return taken

    def acquireIdentity(self, replace: Optional[str] = None) -> bool:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.acquireIdentity`."""
        with self.lock:
            self.store.maybeFail('acquireIdentity')
            reg, now = self.schedulers.get(self.schedulerId), self._now()
            if reg is not None and reg['Instance'] not in (self.instance, replace) and reg['HeartbeatExpires'] > now:
                return False
            self.schedulers[self.schedulerId] = {'Instance': self.instance, 'Host': self.host, 'Pid': self.pid,
                                                 'HeartbeatExpires': now + self.leaseSeconds, 'Stats': {}, 'Draining': False}
            return True

    def identityHolder(self) -> Optional[dict]:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.identityHolder`."""
        with self.lock:
            reg = self.schedulers.get(self.schedulerId)
            return None if reg is None else {k: reg.get(k) for k in ('Instance', 'Host', 'Pid')}

    def heartbeat(self, leases: list[Lease], stats: Optional[dict] = None, draining: Optional[bool] = None) -> tuple[bool, set[str]]:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.heartbeat`."""
        with self.lock:
            self.store.maybeFail('heartbeat', leases)
            reg, now = self.schedulers.get(self.schedulerId), self._now()
            if reg is None or reg['Instance'] != self.instance:
                return False, set()
            reg['HeartbeatExpires'] = now + self.leaseSeconds
            if stats is not None:
                reg['Stats'] = dict(stats)
            if draining is not None:
                reg['Draining'] = draining
            held = set()
            for l in leases:
                rec = self.executions.get(l.weid)
                if rec is not None and self._current(rec, l) and rec.Status in LEASED:
                    rec.LeaseExpires = now + self.leaseSeconds
                    held.add(l.weid)
            return True, held

    def releaseIdentity(self) -> None:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.releaseIdentity`."""
        with self.lock:
            reg = self.schedulers.get(self.schedulerId)
            if reg is not None and reg['Instance'] == self.instance:
                reg['HeartbeatExpires'] = self._now()


class FakeModelServers:
    """
    Registered model servers for the scheduler, without a nameserver.

    Provides the two functions the scheduler takes: :meth:`find` (like
    :func:`mupif.modelplacement.findModelServers`) and :meth:`load` (like
    :func:`mupif.modelplacement.serverLoad`). Free slots come from :attr:`free` (server name ->
    slots, 100 if not given, None: unreachable).
    """

    def __init__(self):
        #: registered name -> tags (as a model server registers them: type:jobmanager, its base name, ...)
        self.tags: dict[str, set[str]] = {}
        #: registered name -> free slots (None: unreachable); 100 if missing
        self.free: dict[str, int | None] = {}
        #: maximum slots of every server
        self.max = 100
        #: number of load queries per server
        self.queries: dict[str, int] = {}

    def add(self, name: str, base: str | None = None, tags: tuple[str, ...] = (), free: int | None = 100) -> None:
        """Register a model server *name* (an instance of *base*, default: *name*) with extra *tags*."""
        self.tags[name] = {'type:jobmanager', base or name, *tags}
        self.free[name] = free

    def find(self, req):
        """Compatible servers of a requirement (see mupif.modelplacement.findModelServers)."""
        from mupif.modelplacement import Candidate
        return [Candidate(nsName=n, uri=f'PYRO:{n}@host:1', tags=frozenset(t), score=len(t & req.optional))
                for n, t in sorted(self.tags.items()) if (t & req.identity) and req.required <= t]

    def load(self, server):
        """Load of a server (see mupif.modelplacement.serverLoad)."""
        from mupif.modelplacement import Load
        self.queries[server.nsName] = self.queries.get(server.nsName, 0) + 1
        free = self.free.get(server.nsName, 100)
        return None if free is None else Load(free=free, max=self.max)
