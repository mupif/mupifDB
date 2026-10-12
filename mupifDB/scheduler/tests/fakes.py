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
        #: method name -> function(*args) returning an exception to raise, or None
        self.failures: dict[str, Callable[..., Exception | None]] = {}
        self._submitted = itertools.count()

    # --- setup ---------------------------------------------------------------------------------

    def addWorkflow(self, wid: str = 'wf', version: int = 1, jobmanagers: tuple[str, ...] = ('CVUT.demo01',), modulename: str = 'fakewf') -> Any:
        """
        Add a workflow whose models use the given jobmanagers.

        :return: the workflow record
        """
        rec = types.SimpleNamespace(wid=wid, Version=version, modulename=modulename, GridFSID=f'file-{wid}-{version}',
                                    Models=[types.SimpleNamespace(Name=f'm{i}', Jobmanager=jm) for i, jm in enumerate(jobmanagers)])
        self.workflows[(wid, version)] = rec
        return rec

    def addExecution(self, weid: str, wid: str = 'wf', version: int = 1, status: str = 'Pending', attempts: int = 0, loggerURI: str | None = None) -> Any:
        """
        Add an execution; executions are submitted in the order they are added.

        :return: the execution record
        """
        rec = types.SimpleNamespace(dbID=weid, Status=status, Attempts=attempts, WorkflowID=wid, WorkflowVersion=version, submitted=next(self._submitted),
                                    loggerURI=loggerURI)
        self.executions[weid] = rec
        return rec

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

    def setStatus(self, weid: str, status: str, keepAttempts: bool = False) -> None:
        """See :meth:`mupifDB.scheduler.backend.ExecutionBackend.setStatus`."""
        with self.lock:
            self._maybeFail('setStatus', weid, status)
            rec = self.executions[weid]
            rec.Status = status
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
