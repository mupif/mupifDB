"""
Test doubles for the scheduler: an in-memory database of executions and workflows.

:class:`FakeBackend` implements :class:`mupifDB.scheduler.backend.ExecutionBackend` the way the REST
API behaves (e.g. returning to ``Pending`` resets the attempt count unless asked to keep it), records
every change, and can be told to fail. It is thread-safe, since the scheduler calls it from its job
threads.
"""

import itertools
import threading
import types
from pathlib import Path
from typing import Any, Callable


class FakeBackend:
    """In-memory :class:`~mupifDB.scheduler.backend.ExecutionBackend`."""

    def __init__(self):
        """Start with no executions and no workflows."""
        self.lock = threading.RLock()
        #: execution ID -> record (SimpleNamespace with dbID, Status, Attempts, WorkflowID, WorkflowVersion, submitted)
        self.executions: dict[str, Any] = {}
        #: (workflow ID, version) -> record (SimpleNamespace with modulename, GridFSID, Models)
        self.workflows: dict[tuple[str, int], Any] = {}
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
                     profile: int = -1) -> Any:
        """
        Add an execution; executions are submitted in the order they are added.

        :param profile: execution profile index (-1: the default profile)
        :return: the execution record
        """
        rec = types.SimpleNamespace(dbID=weid, Status=status, Attempts=attempts, WorkflowID=wid, WorkflowVersion=version, submitted=next(self._submitted),
                                    loggerURI=loggerURI, ExecutionProfileIndex=profile, ModelServers={})
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

    def _maybeFail(self, method: str, *args) -> None:
        """Raise the exception configured for *method*, if any."""
        f = self.failures.get(method)
        if f is not None and (exc := f(*args)) is not None:
            raise exc

    @staticmethod
    def _copy(rec: Any) -> Any:
        """Return a copy of a record, as the REST API returns a fresh object every time."""
        return types.SimpleNamespace(**vars(rec))

    # --- ExecutionBackend -----------------------------------------------------------------------

    def pendingExecutions(self, limit: int) -> list[Any]:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.pendingExecutions`."""
        with self.lock:
            self._maybeFail('pendingExecutions', limit)
            recs = sorted((r for r in self.executions.values() if r.Status == 'Pending'), key=lambda r: r.submitted)
            return [self._copy(r) for r in recs[:limit]]

    def executionsWithStatus(self, status: str, limit: int) -> list[Any]:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.executionsWithStatus`."""
        with self.lock:
            self._maybeFail('executionsWithStatus', status, limit)
            return [self._copy(r) for r in self.executions.values() if r.Status == status][:limit]

    def getExecution(self, weid: str) -> Any:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.getExecution`."""
        with self.lock:
            self._maybeFail('getExecution', weid)
            return self._copy(self.executions[weid])

    def getWorkflow(self, wid: str, version: int) -> Any:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.getWorkflow`."""
        with self.lock:
            self._maybeFail('getWorkflow', wid, version)
            return self.workflows[(wid, int(version))]

    def inputsComplete(self, weid: str) -> bool:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.inputsComplete`; executions in :attr:`incomplete` lack inputs."""
        with self.lock:
            self._maybeFail('inputsComplete', weid)
            return weid not in self.incomplete

    def setStatus(self, weid: str, status: str, keepAttempts: bool = False) -> None:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.setStatus`."""
        with self.lock:
            self._maybeFail('setStatus', weid, status)
            rec = self.executions[weid]
            rec.Status = status
            if status == 'Running':
                # like the REST API: fields set by the execution script belong to one attempt
                rec.ModelServers = {}
                rec.workflowURI = None
            if status == 'Pending' and not keepAttempts:
                rec.Attempts = 0
            self.history.append((weid, status))

    def setAttempts(self, weid: str, attempts: int) -> None:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.setAttempts`."""
        with self.lock:
            self._maybeFail('setAttempts', weid, attempts)
            self.executions[weid].Attempts = attempts

    def storeLog(self, weid: str, path: Path) -> None:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.storeLog`."""
        with self.lock:
            self._maybeFail('storeLog', weid, path)
            self.logs[weid] = Path(path).read_bytes()

    def notifyOwner(self, weid: str) -> None:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.notifyOwner`."""
        with self.lock:
            self.notified.append(weid)

    def statusCounts(self) -> dict[str, int]:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.statusCounts`."""
        with self.lock:
            self._maybeFail('statusCounts')
            counts: dict[str, int] = {}
            for r in self.executions.values():
                counts[r.Status] = counts.get(r.Status, 0) + 1
            return counts


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
