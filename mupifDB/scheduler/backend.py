"""
Access of the scheduler to the database of executions and workflows.

The scheduler talks to the database only through :class:`ExecutionBackend`. :class:`RestBackend`
implements it with the REST API client (:mod:`mupifDB.restApiControl`); the tests use the in-memory
``FakeBackend`` from ``mupifDB/scheduler/tests/fakes.py``. Keeping the interface small and explicit
also documents every change the scheduler makes to the database.
"""

from pathlib import Path
from typing import Any, Literal, Protocol

#: statuses the scheduler sets
SchedulerStatus = Literal['Created', 'Pending', 'Scheduled', 'Running', 'Finished', 'Failed']


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
        Return up to *limit* executions with status ``Pending``, oldest submitted first.

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

    def setStatus(self, weid: str, status: SchedulerStatus, keepAttempts: bool = False) -> None:
        """
        Change the status of an execution (the REST API also sets the matching dates).

        :param weid: execution ID
        :param status: new status
        :param keepAttempts: when returning to ``Pending``: keep the attempt count (a retry by the
                             scheduler) instead of resetting it (a new submission)
        """

    def setAttempts(self, weid: str, attempts: int) -> None:
        """
        Set the attempt count of an execution.

        :param weid: execution ID
        :param attempts: new count
        """

    def storeLog(self, weid: str, path: Path) -> None:
        """
        Upload a log file and link it to the execution as ``ExecutionLog``.

        :param weid: execution ID
        :param path: log file
        """

    def notifyOwner(self, weid: str) -> None:
        """
        Tell the owner of the execution about its (new) status, if notifications are configured.

        :param weid: execution ID
        """


class RestBackend:
    """:class:`ExecutionBackend` implemented with the REST API client and the e-mail notifications."""

    def pendingExecutions(self, limit: int) -> list[Any]:
        """See :meth:`ExecutionBackend.pendingExecutions`."""
        from mupifDB import restApiControl
        return restApiControl.getPendingExecutions(num_limit=limit, sort='SubmittedDate')

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

    def setStatus(self, weid: str, status: SchedulerStatus, keepAttempts: bool = False) -> None:
        """See :meth:`ExecutionBackend.setStatus`."""
        from mupifDB import restApiControl
        restApiControl.setExecutionStatus(weid, status, revertPending=keepAttempts)

    def setAttempts(self, weid: str, attempts: int) -> None:
        """See :meth:`ExecutionBackend.setAttempts`."""
        from mupifDB import restApiControl
        restApiControl.setExecutionAttemptsCount(weid, attempts)

    def storeLog(self, weid: str, path: Path) -> None:
        """See :meth:`ExecutionBackend.storeLog`."""
        from mupifDB import restApiControl
        with open(path, 'rb') as f:
            logId = restApiControl.uploadBinaryFile(f)
        if logId is not None:
            restApiControl.setExecutionParameter(weid, 'ExecutionLog', logId)

    def notifyOwner(self, weid: str) -> None:
        """See :meth:`ExecutionBackend.notifyOwner`."""
        from mupifDB import my_email
        my_email.sendEmailAboutExecutionStatus(weid)
