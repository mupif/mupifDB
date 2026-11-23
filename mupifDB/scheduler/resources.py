"""
Model-server capacity check (admission) of one scheduling pass.

Moved from ``mupifDB/workflowscheduler.py``. The models of a workflow run on model servers; which
servers are compatible with a model is decided by mupif (:mod:`mupif.modelplacement`): every
instance of the model server named by the model's ``Jobmanager``, model servers of the same
``ModelID``, filtered and preferred by the execution profile chosen for the execution. The
**workflow** chooses the server when it allocates the model (least loaded first, falling back to
the next one); the scheduler only checks, before it starts a workflow, that its models can find
free slots (:class:`ResourceBudget`). The budget does not tell the workflow where to run.

Finding the servers and asking for their load are passed in as functions, so that the budget does
not depend on the nameserver (and is tested without it).
"""

import dataclasses
import logging
import random
from typing import Any, Callable, Optional

from mupif import modelplacement
from mupif.modelplacement import Candidate, Load, ModelRequirement

log = logging.getLogger('scheduler')


class InvalidProfileError(ValueError):
    """The execution asks for an execution profile its workflow does not have; it cannot run."""


@dataclasses.dataclass
class Reservation:
    """Result of :meth:`ResourceBudget.reserve`."""
    #: :attr:`ResourceBudget.OK`, :attr:`~ResourceBudget.BUSY` or :attr:`~ResourceBudget.UNAVAILABLE`
    status: str
    #: execution profile used (``default`` or ``#<index> "<name>"``)
    profile: str = 'default'
    #: model -> number of reachable compatible model servers
    compatible: dict[str, int] = dataclasses.field(default_factory=dict)
    #: model -> model server its slot was counted on (the workflow may choose another one)
    counted: dict[str, str] = dataclasses.field(default_factory=dict)

    def __str__(self):
        models = ', '.join(f'{m}: {n} compatible model server(s)' for m, n in self.compatible.items())
        return f'profile {self.profile}' + (f'; {models}' if models else '; no remote models')


class ResourceBudget:
    """
    Free model-server slots for one scheduling pass.

    The compatible servers of each model requirement and the load of each server are read once per
    pass (on first use). Every execution taken in the pass counts one slot per remote model on a
    compatible server, chosen with mupif's ranking (:func:`mupif.modelplacement.rankCandidates`)
    over the slots left in the pass, so that executions taken in one pass are spread like the
    workflows will spread themselves. Counting per server (not per model) keeps models of different
    requirements which share servers (e.g. two execution profiles) from being admitted twice on the
    same slots. Without the budget, all pending executions of a pass were checked against the same
    free slots, so many more executions were started than the model servers could serve.

    The model servers count a slot only once a workflow has allocated its model. Executions started
    in an earlier pass whose models are not allocated yet are therefore counted in the budget by the
    scheduler before the pass (:meth:`~mupifDB.scheduler.core.Scheduler.reserveInFlight`). Slots
    taken by other clients meanwhile are not known here; an execution which finds no free slot when
    it starts exits with code 2 and is returned to Pending.

    Every remote model of the workflow is counted, also those marked ``Instantiate: false``: mupif
    allocates them at initialization too (it reads the flag through ``BaseMeta.get``, which ignores
    keys not declared by ``ModelInWorkflowMeta``).
    """

    #: values of :attr:`Reservation.status`
    OK, BUSY, UNAVAILABLE = 'ok', 'busy', 'unavailable'

    def __init__(self, workflowRecord: Callable[[str, int], Any], findServers: Callable[[ModelRequirement], list[Candidate]],
                 serverLoad: Callable[[Candidate], Optional[Load]], rng: Optional[random.Random] = None):
        """
        Start an empty budget; servers and loads are read on first use.

        :param workflowRecord: ``(workflow ID, version) -> workflow record`` (with ``Models`` and
                               ``ExecutionProfiles``); raises if the workflow cannot be read
        :param findServers: ``requirement -> compatible servers`` (see
                            :func:`mupif.modelplacement.findModelServers`); raises if the
                            nameserver cannot be asked
        :param serverLoad: ``server -> load``, None if the server cannot be reached
        :param rng: random generator for the ranking tie-break
        """
        self._workflowRecord = workflowRecord
        self._findServers = findServers
        self._serverLoad = serverLoad
        self._rng = rng
        #: (workflow ID, version) -> workflow record, cached for the pass
        self.workflows: dict[tuple, Any] = {}
        #: requirement key -> compatible servers (None: could not be looked up)
        self.candidates: dict[tuple, Optional[list[Candidate]]] = {}
        #: server name -> load left in this pass (None: not reachable)
        self.loads: dict[str, Optional[Load]] = {}

    def _requirements(self, wid: str, version: int, profileIndex: int) -> Optional[dict[str, ModelRequirement]]:
        """Return the requirements of the workflow's remote models (None if the workflow cannot be read)."""
        key = (wid, int(version))
        if key not in self.workflows:
            try:
                self.workflows[key] = self._workflowRecord(wid, version)
            except Exception:
                log.exception(f'Cannot read workflow {wid} version {version}')
                return None
        try:
            return modelplacement.modelRequirements(self.workflows[key], profileIndex)
        except ValueError as e:
            raise InvalidProfileError(str(e)) from e

    def _candidates(self, req: ModelRequirement) -> Optional[list[Candidate]]:
        """Return the compatible servers of *req*, reading their loads (None if they cannot be looked up)."""
        key = req.key()
        if key not in self.candidates:
            try:
                self.candidates[key] = self._findServers(req)
            except Exception:
                log.exception(f'Cannot look up model servers for model {req.model} ({req.describe()})')
                self.candidates[key] = None
        cands = self.candidates[key]
        for c in cands or ():
            if c.nsName not in self.loads:
                self.loads[c.nsName] = self._serverLoad(c)
        if cands is None:
            return None
        # a server found by another requirement keeps the tags it was found with; the score
        # (optional tags) is specific to this requirement
        return [dataclasses.replace(c, score=len(c.tags & req.optional)) for c in cands]

    def reserve(self, wid: str, version: int, profileIndex: int = -1) -> Reservation:
        """
        Count the slots needed by one execution of the given workflow, if all are available.

        :param wid: workflow ID of the execution
        :param version: workflow version
        :param profileIndex: execution profile of the execution (negative: the default profile)
        :return: status :attr:`OK` if a slot was found for every remote model (the slots are taken
                 from the budget; the execution may be started); :attr:`BUSY` if compatible servers
                 are reachable but have too few free slots (the execution just has to wait);
                 :attr:`UNAVAILABLE` if the workflow record or the nameserver cannot be read, or
                 no compatible server of some model is registered and reachable (a real problem:
                 counted as a failed attempt by the caller)
        :raises InvalidProfileError: the workflow has no execution profile *profileIndex*
        """
        reqs = self._requirements(wid, version, profileIndex)
        if reqs is None:
            return Reservation(self.UNAVAILABLE)
        profile = next(iter(reqs.values())).profile if reqs else modelplacement.profileName(self.workflows[(wid, int(version))], profileIndex)
        res = Reservation(self.OK, profile=profile)
        # slots counted for this execution, committed to self.loads only if every model fits
        loads = dict(self.loads)
        for name, req in reqs.items():
            cands = self._candidates(req)
            if cands is None:
                return dataclasses.replace(res, status=self.UNAVAILABLE)
            for c in cands:
                loads.setdefault(c.nsName, self.loads.get(c.nsName))
            reachable = [c for c in cands if loads.get(c.nsName) is not None]
            res.compatible[name] = len(reachable)
            if not reachable:
                log.info(f'Model {name} of workflow {wid}: no compatible model server registered and reachable ({req.describe()})')
                return dataclasses.replace(res, status=self.UNAVAILABLE)
            ranked = modelplacement.rankCandidates(reachable, loads, self._rng)
            best = ranked[0]
            if loads[best.nsName].free <= 0:
                return dataclasses.replace(res, status=self.BUSY)
            loads[best.nsName] = loads[best.nsName].take()
            res.counted[name] = best.nsName
        self.loads = loads
        return res


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
    :param jobmanagers: registered (nameserver) names of the model servers the execution's models may
                        run on: those it used (``ModelServers`` of the execution) or, if not known,
                        all compatible ones
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
