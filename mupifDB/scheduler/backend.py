"""
Access of the scheduler to the database of executions and workflows.

The scheduler talks to the database only through :class:`ExecutionBackend`. :class:`RestBackend`
implements it with the REST API client (:mod:`mupifDB.restApiControl`); the tests use the in-memory
``FakeBackend`` from ``mupifDB/scheduler/tests/fakes.py``. Keeping the interface small and explicit
also documents every change the scheduler makes to the database.
"""

import dataclasses
import os
import socket
import uuid
from pathlib import Path
from typing import Any, Literal, Optional, Protocol

#: statuses the scheduler sets
SchedulerStatus = Literal['Created', 'Pending', 'Scheduled', 'Running', 'Finished', 'Failed']
#: retry delay ``(base, maximum, jitter)``: ``min(base * 2**Deferrals, maximum)``, varied by ±jitter
Backoff = tuple[float, float, float]


@dataclasses.dataclass(frozen=True)
class Lease:
    """
    The lease an execution is held under by this scheduler (see :mod:`mupifDB.api.leases`).

    Every write made for the execution names it (:meth:`header`); after a takeover by another
    scheduler (new epoch) such writes are refused, and :class:`LeaseLost` is raised.
    """
    weid: str
    schedulerId: str
    epoch: int

    def header(self) -> str:
        """Value of the request header ``X-MupifDB-Lease``."""
        return f'{self.weid}:{self.schedulerId}:{self.epoch}'


class LeaseLost(Exception):
    """
    A lease condition failed: the execution is not (any more) held under the lease, or was taken by another scheduler.

    Not a transient error: the scheduler must drop what it was doing with the execution, without
    writing anything more for it (another scheduler holds it, or nobody does).
    """


class ExecutionBackend(Protocol):
    """
    Operations of the scheduler on executions and workflows.

    Execution records have (at least) the attributes ``dbID``, ``Status``, ``Attempts``,
    ``WorkflowID`` and ``WorkflowVersion``; workflow records ``modulename``, ``GridFSID`` and
    ``Models`` (see :mod:`mupifDB.models`). Every method raises on failure (e.g. REST API
    unreachable); the scheduler decides how to recover.
    """

    def pendingExecutions(self, limit: int) -> list[Any]:
        """
        Return up to *limit* executions with status ``Pending`` which may be claimed now (retry delay passed), oldest submitted first.

        :param limit: maximum number of records
        :return: execution records
        """

    def executionsWithStatus(self, status: SchedulerStatus, limit: int) -> list[Any]:
        """
        Return up to *limit* executions with the given status (any order).

        :param status: status to look for
        :param limit: maximum number of records
        :return: execution records
        """

    def getExecution(self, weid: str) -> Any:
        """
        Read one execution record.

        :param weid: execution ID
        :return: the record
        """

    def getWorkflow(self, wid: str, version: int) -> Any:
        """
        Read one workflow record.

        :param wid: workflow ID
        :param version: workflow version
        :return: the record
        """

    def inputsComplete(self, weid: str) -> bool:
        """
        Tell whether all compulsory inputs of an execution are set.

        :param weid: execution ID
        :return: true if the execution can run
        """

    def storeLog(self, weid: str, path: Path, lease: Optional[Lease] = None) -> None:
        """
        Upload a log file and link it to the execution as ``ExecutionLog``.

        :param weid: execution ID
        :param path: log file
        :param lease: the lease the execution is held under (the write is refused if it is not current)
        :raises LeaseLost: if the lease is not current
        """

    def notifyOwner(self, weid: str) -> None:
        """
        Tell the owner of the execution about its (new) status, if notifications are configured.

        :param weid: execution ID
        """

    # --- leases (several schedulers; see mupifDB.api.leases) ------------------------------------

    def claim(self, weid: str) -> tuple[Any, Lease]:
        """
        Take a pending, due execution under a new lease (``Pending -> Scheduled``).

        :return: the updated record and its lease
        :raises LeaseLost: if another scheduler took it, or it is not pending or not due
        """

    def transition(self, lease: Lease, to: SchedulerStatus, *, incAttempts: int = 0, resetAttempts: bool = False,
                   backoff: Optional[Backoff] = None, renew: bool = False) -> Any:
        """
        Change the status of an execution held under *lease* (see :func:`mupifDB.api.leases.transition`).

        :param incAttempts: added to ``Attempts``
        :param resetAttempts: set ``Attempts`` to 0
        :param backoff: when returning to ``Pending``: retry delay (not claimed before it passed)
        :param renew: renew the lease (when the workflow is launched)
        :return: the updated record
        :raises LeaseLost: if the execution is not held under *lease* any more
        """

    def defer(self, weid: str, backoff: Backoff) -> bool:
        """
        Postpone a pending execution whose resources are unavailable, counting an attempt.

        :return: whether this scheduler deferred it (False: another one did, or claimed it, meanwhile)
        """

    def takeOver(self, limit: int) -> list[tuple[Any, Lease]]:
        """Take over executions whose lease expired, held by schedulers which are not alive (see :func:`mupifDB.api.leases.takeOver`)."""

    def takeOverOwn(self) -> list[tuple[Any, Lease]]:
        """Take over the executions of an earlier process of this scheduler (same ID), at its start."""

    def acquireIdentity(self, replace: Optional[str] = None) -> bool:
        """Take the scheduler identity for this process; False if another live process holds it (*replace*: an instance found dead)."""

    def identityHolder(self) -> Optional[dict]:
        """Return who holds the scheduler identity: ``{'Instance', 'Host', 'Pid'}``, or None."""

    def heartbeat(self, leases: list[Lease], stats: Optional[dict] = None, draining: Optional[bool] = None) -> tuple[bool, set[str]]:
        """
        Renew the heartbeat of the scheduler and the given leases.

        :return: (whether this process still holds the identity, IDs of the executions still held)
        """

    def releaseIdentity(self) -> None:
        """Give up the identity at a clean shutdown."""

    def statusCounts(self) -> dict[str, int]:
        """
        Count the executions in the database by status, for the statistics.

        Only cheap aggregate numbers: the scheduler samples them every iteration to report how
        many executions wait in the queue (``Pending``) and how many are ``Running`` according to
        the database (to compare with its own processes).

        :return: number of executions by status (statuses without executions may be missing)
        """


class RestBackend:
    """
    :class:`ExecutionBackend` implemented with the REST API client and the e-mail notifications.

    :param schedulerId: identity of the scheduler (``scheduler_id``)
    :param leaseSeconds: validity of leases and of the heartbeat, renewed by the scheduler
    """

    def __init__(self, schedulerId: str = '', leaseSeconds: float = 600.):
        self.schedulerId = schedulerId
        self.leaseSeconds = leaseSeconds
        #: this process (an identity is held by one process at a time)
        self.instance = uuid.uuid4().hex

    @staticmethod
    def _lease(rec: Any) -> Lease:
        return Lease(weid=str(rec.dbID), schedulerId=rec.SchedulerID, epoch=int(rec.LeaseEpoch))

    def claim(self, weid: str) -> tuple[Any, Lease]:
        """See :meth:`ExecutionBackend.claim`."""
        from mupifDB import restApiControl
        from mupifDB.api.client_util import ConflictResponse
        try:
            rec = restApiControl.claimExecution(weid, self.schedulerId, self.leaseSeconds)
        except ConflictResponse as e:
            raise LeaseLost(str(e)) from e
        return rec, self._lease(rec)

    def transition(self, lease: Lease, to: SchedulerStatus, *, incAttempts: int = 0, resetAttempts: bool = False,
                   backoff: Optional[Backoff] = None, renew: bool = False) -> Any:
        """See :meth:`ExecutionBackend.transition`."""
        from mupifDB import restApiControl
        from mupifDB.api.client_util import ConflictResponse
        try:
            return restApiControl.transitionExecution(lease.weid, lease.schedulerId, lease.epoch, to, incAttempts=incAttempts,
                                                      resetAttempts=resetAttempts, backoff=backoff,
                                                      leaseSeconds=(self.leaseSeconds if renew else None))
        except ConflictResponse as e:
            raise LeaseLost(str(e)) from e

    def defer(self, weid: str, backoff: Backoff) -> bool:
        """See :meth:`ExecutionBackend.defer`."""
        from mupifDB import restApiControl
        from mupifDB.api.client_util import ConflictResponse
        try:
            restApiControl.deferExecution(weid, backoff)
            return True
        except ConflictResponse:
            return False

    def takeOver(self, limit: int) -> list[tuple[Any, Lease]]:
        """See :meth:`ExecutionBackend.takeOver`."""
        from mupifDB import restApiControl
        return [(r, self._lease(r)) for r in restApiControl.takeOverExecutions(self.schedulerId, self.leaseSeconds, limit)]

    def takeOverOwn(self) -> list[tuple[Any, Lease]]:
        """See :meth:`ExecutionBackend.takeOverOwn`."""
        from mupifDB import restApiControl
        return [(r, self._lease(r)) for r in restApiControl.takeOverOwnExecutions(self.schedulerId, self.instance, self.leaseSeconds)]

    def acquireIdentity(self, replace: Optional[str] = None) -> bool:
        """See :meth:`ExecutionBackend.acquireIdentity`."""
        from mupifDB import restApiControl
        return restApiControl.acquireSchedulerIdentity(self.schedulerId, self.instance, socket.gethostname(), os.getpid(), self.leaseSeconds, replace=replace)

    def identityHolder(self) -> Optional[dict]:
        """See :meth:`ExecutionBackend.identityHolder`."""
        from mupifDB import restApiControl
        for s in restApiControl.getSchedulers():
            if s.id == self.schedulerId:
                return {'Instance': s.Instance, 'Host': s.Host, 'Pid': s.Pid}
        return None

    def heartbeat(self, leases: list[Lease], stats: Optional[dict] = None, draining: Optional[bool] = None) -> tuple[bool, set[str]]:
        """See :meth:`ExecutionBackend.heartbeat`."""
        from mupifDB import restApiControl
        r = restApiControl.schedulerHeartbeat(self.schedulerId, self.instance, self.leaseSeconds, self.leaseSeconds,
                                              [(l.weid, l.epoch) for l in leases], stats=stats, draining=draining)
        return bool(r['identity']), set(r['held'])

    def releaseIdentity(self) -> None:
        """See :meth:`ExecutionBackend.releaseIdentity`."""
        from mupifDB import restApiControl
        restApiControl.releaseSchedulerIdentity(self.schedulerId, self.instance)

    def pendingExecutions(self, limit: int) -> list[Any]:
        """See :meth:`ExecutionBackend.pendingExecutions`."""
        from mupifDB import restApiControl
        return restApiControl.getPendingExecutions(num_limit=limit, sort='SubmittedDate', ready=True)

    def executionsWithStatus(self, status: SchedulerStatus, limit: int) -> list[Any]:
        """See :meth:`ExecutionBackend.executionsWithStatus`."""
        from mupifDB import restApiControl
        return restApiControl.getExecutionRecords(status=status, num_limit=limit)

    def getExecution(self, weid: str) -> Any:
        """See :meth:`ExecutionBackend.getExecution`."""
        from mupifDB import restApiControl
        return restApiControl.getExecutionRecord(weid)

    def getWorkflow(self, wid: str, version: int) -> Any:
        """See :meth:`ExecutionBackend.getWorkflow`."""
        from mupifDB import restApiControl
        return restApiControl.getWorkflowRecord(wid, int(version))

    def inputsComplete(self, weid: str) -> bool:
        """See :meth:`ExecutionBackend.inputsComplete`."""
        from mupifDB import restApiControl
        return restApiControl.checkExecutionInputs(weid)

    def storeLog(self, weid: str, path: Path, lease: Optional[Lease] = None) -> None:
        """See :meth:`ExecutionBackend.storeLog`."""
        from mupifDB import restApiControl
        from mupifDB.api.client_util import ConflictResponse, leaseContext
        with open(path, 'rb') as f:
            logId = restApiControl.uploadBinaryFile(f)
        if logId is not None:
            try:
                with leaseContext(lease.header() if lease is not None else None):
                    restApiControl.setExecutionParameter(weid, 'ExecutionLog', logId)
            except ConflictResponse as e:
                raise LeaseLost(str(e)) from e

    def notifyOwner(self, weid: str) -> None:
        """See :meth:`ExecutionBackend.notifyOwner`."""
        from mupifDB import my_email
        my_email.sendEmailAboutExecutionStatus(weid)

    def statusCounts(self) -> dict[str, int]:
        """See :meth:`ExecutionBackend.statusCounts`; one aggregation query (``GET execution_statistics``)."""
        from mupifDB import restApiControl
        st = restApiControl.getExecutionStatistics()
        return {'Created': st.createdExecutions, 'Pending': st.pendingExecutions, 'Scheduled': st.scheduledExecutions,
                'Running': st.runningExecutions, 'Finished': st.finishedExecutions, 'Failed': st.failedExecutions}
