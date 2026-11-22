"""
Model-server slot budget of one scheduling pass.

Moved from ``mupifDB/workflowscheduler.py``. The budget only does the bookkeeping; reading the
models of a workflow and asking a jobmanager for its free slots are passed in as functions, so that
it does not depend on the REST API or the nameserver (and is tested without them).
"""

import logging
from typing import Callable

log = logging.getLogger('scheduler')


class ResourceBudget:
    """
    Free jobmanager slots for one scheduling pass.

    The number of free slots of each jobmanager is read once per pass (on first use) and every
    execution enqueued in the pass takes one slot per model it needs. Without this, all pending
    executions of a pass were checked against the same free slots -- the slots are only taken
    when the workflow subprocess allocates its models -- so many more executions were started than
    the model servers could serve. Slots taken by executions enqueued in an earlier pass but not
    started yet are not known here; such an execution finds no free slot when it starts, exits with
    code 2 and is returned to Pending.
    """

    #: results of :meth:`reserve`
    OK, BUSY, UNAVAILABLE = 'ok', 'busy', 'unavailable'

    def __init__(self, workflowJobmanagers: Callable[[str, int], list[str]], freeJobSlots: Callable[[str], int | None]):
        """
        Start an empty budget; free slots are read on first use.

        :param workflowJobmanagers: ``(workflow ID, version) -> jobmanager names``, one per model
                                    with a remote jobmanager; raises if the workflow cannot be read
        :param freeJobSlots: ``jobmanager name -> free job slots``, None if it cannot be reached
        """
        self._workflowJobmanagers = workflowJobmanagers
        self._freeJobSlots = freeJobSlots
        #: jobmanager name -> free slots left in this pass (None: jobmanager not reachable)
        self.free: dict[str, int | None] = {}
        #: (workflow ID, version) -> jobmanagers needed (see workflowJobmanagers), cached for the pass
        self.workflows: dict[tuple, list[str]] = {}

    def reserve(self, wid: str, version: int) -> str:
        """
        Take the slots needed by one execution of the given workflow, if all are available.

        :param wid: workflow ID of the execution
        :param version: workflow version
        :return: :attr:`OK` if the slots were taken (the execution may be started); :attr:`BUSY` if
                 the jobmanagers are reachable but have too few free slots (the execution just has
                 to wait); :attr:`UNAVAILABLE` if a jobmanager or the workflow record cannot be
                 reached (a real problem: counted as a failed attempt by the caller)
        """
        key = (wid, int(version))
        if key not in self.workflows:
            try:
                self.workflows[key] = self._workflowJobmanagers(wid, version)
            except Exception:
                log.exception(f'Cannot read models of workflow {wid} version {version}')
                return self.UNAVAILABLE
        needed: dict[str, int] = {}
        for jm in self.workflows[key]:
            needed[jm] = needed.get(jm, 0) + 1
        for jm in needed:
            if jm not in self.free:
                self.free[jm] = self._freeJobSlots(jm)
        if any(self.free[jm] is None for jm in needed):
            return self.UNAVAILABLE
        if any(self.free[jm] < n for jm, n in needed.items()):
            return self.BUSY
        for jm, n in needed.items():
            self.free[jm] -= n
        return self.OK


def threadProxy(ns):
    """
    Return a nameserver proxy usable in the calling thread.

    A Pyro proxy belongs to the thread that created (or last claimed) it, and refuses calls from
    other threads; the scheduler queries model servers from its main loop and from job threads.

    :param ns: nameserver proxy (or a stand-in without ``_pyroUri``, returned as is)
    :return: a new proxy of the same nameserver
    """
    import Pyro5.api
    return Pyro5.api.Proxy(ns._pyroUri) if hasattr(ns, '_pyroUri') else ns


def modelServerFreeSlots(ns, jobmanager: str) -> int | None:
    """
    Ask a jobmanager (model server) registered in the nameserver how many more jobs it can run now.

    :param ns: Pyro nameserver proxy
    :param jobmanager: name of the jobmanager in the nameserver
    :return: number of free job slots, or None if the jobmanager is unknown or cannot be reached
    """
    import mupif as mp
    try:
        return int(mp.pyroutil.connectModelServer(threadProxy(ns), jobmanager).getNumberOfFreeJobs())
    except Exception:
        log.exception(f'Cannot get free job slots of jobmanager {jobmanager}')
        return None


def pyroLocation(uri: str) -> str:
    """
    Return the address of the Pyro daemon serving an object: ``PYRO:obj_x@host:port`` -> ``host:port``.

    :param uri: Pyro URI
    :return: the part after ``@`` (the whole string if there is none)
    """
    return uri.split('@', 1)[-1]


def terminateExecutionModelJobs(ns, jobmanagers: list[str], loggerURI: str) -> list[str]:
    """
    Terminate the model-server jobs of one execution.

    The models of an execution log to a log receiver in the Pyro daemon of its execution script;
    the jobmanager reports that receiver as the ``remoteLogUri`` of each job. The execution record
    stores the URI of the script's receiver as ``loggerURI``. Jobs are matched by the daemon address
    (host:port) of these URIs, not the whole URI: the workflow may register a receiver of its own in
    the same daemon (it does when the ``Execution.Log_URI`` it was given is lost, see the mupif
    metadata merge). The address is unique among running execution scripts.

    Normally the execution script terminates its models itself; this cleans up after a script that
    died (killed, crashed, stopped while blocked in a call to a model), whose jobs would otherwise
    keep their slots on the model server indefinitely.

    :param ns: Pyro nameserver proxy
    :param jobmanagers: jobmanagers used by the workflow of the execution
    :param loggerURI: ``loggerURI`` of the execution
    :return: keys of the jobs terminated
    """
    import mupif as mp
    location = pyroLocation(loggerURI)
    ns = threadProxy(ns)
    terminated = []
    for name in sorted(set(jobmanagers)):
        try:
            server = mp.pyroutil.connectModelServer(ns, name)
            for job in server.getStatus():
                if job.remoteLogUri and pyroLocation(job.remoteLogUri) == location:
                    server.terminateJob(job.key)
                    terminated.append(job.key)
        except Exception:
            log.exception(f'Cannot clean up jobs of jobmanager {name}')
    return terminated
