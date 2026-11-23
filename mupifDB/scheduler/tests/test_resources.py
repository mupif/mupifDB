"""Tests of :class:`mupifDB.scheduler.resources.ResourceBudget`, without REST API or nameserver."""

import random

import pytest

from mupifDB.scheduler.resources import InvalidProfileError, ResourceBudget, terminateExecutionModelJobs
from mupifDB.scheduler.tests.fakes import FakeBackend, FakeModelServers

OK, BUSY, UNAVAILABLE = ResourceBudget.OK, ResourceBudget.BUSY, ResourceBudget.UNAVAILABLE


def _budget(servers: FakeModelServers, backend: FakeBackend, failFind=False) -> ResourceBudget:
    """Budget over the workflows of *backend* and the model servers *servers*."""
    def find(req):
        if failFind:
            raise RuntimeError('nameserver unreachable')
        return servers.find(req)
    return ResourceBudget(backend.getWorkflow, find, servers.load, rng=random.Random(0))


def test_slots_are_consumed_within_a_pass():
    """Each reservation takes one slot per model; loads are read once per server and pass."""
    servers, backend = FakeModelServers(), FakeBackend()
    servers.add('jm1', free=2)
    servers.add('jm2', free=5)
    backend.addWorkflow('wf', 1, jobmanagers=('jm1', 'jm2'))
    b = _budget(servers, backend)
    assert [b.reserve('wf', 1).status for _ in range(3)] == [OK, OK, BUSY]
    assert {n: l.free for n, l in b.loads.items()} == {'jm1': 0, 'jm2': 3}
    assert servers.queries == {'jm1': 1, 'jm2': 1}


def test_instances_add_up_and_spread():
    """The instances of a model server share the load: the less loaded one is counted first."""
    servers, backend = FakeModelServers(), FakeBackend()
    servers.add('CVUT.demo01', free=3)
    servers.add('CVUT.demo01~node2', base='CVUT.demo01', free=2)
    backend.addWorkflow('wf', 1, jobmanagers=('CVUT.demo01',))
    b = _budget(servers, backend)
    res = [b.reserve('wf', 1) for _ in range(6)]
    # 5 slots in total (one server alone would admit 3 or 2)
    assert [r.status for r in res] == [OK] * 5 + [BUSY]
    # used share: 97/100 vs 98/100 -> the first one, then alternating while equal
    assert [r.counted['m0'] for r in res[:3]] == ['CVUT.demo01', 'CVUT.demo01~node2', 'CVUT.demo01']
    assert res[0].compatible == {'m0': 2}
    assert str(res[0]) == 'profile default; m0: 2 compatible model server(s)'


def test_profiles_share_server_slots():
    """Executions with different profiles whose compatible servers overlap do not get the same slot twice."""
    servers, backend = FakeModelServers(), FakeBackend()
    servers.add('CVUT.demo01', tags=('Runtime_seconds',), free=1)
    profiles = [{'Name': 'fast', 'Models': [{'Name': 'm0', 'RequiredModelMetadata': ['Runtime_seconds']}]}]
    backend.addWorkflow('wf', 1, jobmanagers=('CVUT.demo01',), profiles=profiles)
    b = _budget(servers, backend)
    assert b.reserve('wf', 1, 0).status == OK
    assert b.reserve('wf', 1, -1).status == BUSY


def test_profile_requirements():
    """A profile limits the compatible servers (required tags) and prefers some (optional tags)."""
    servers, backend = FakeModelServers(), FakeBackend()
    servers.add('CVUT.demo01', tags=('Runtime_minutes',))
    servers.add('CVUT.demo01~fast', base='CVUT.demo01', tags=('Runtime_seconds', 'Accuracy_High'), free=1)
    profiles = [{'Name': 'fast only', 'Models': [{'Name': 'm0', 'RequiredModelMetadata': ['Runtime_seconds']}]},
                {'Name': 'prefer accurate', 'Models': [{'Name': 'm0', 'OptionalModelMetadata': ['Accuracy_High']}]}]
    backend.addWorkflow('wf', 1, jobmanagers=('CVUT.demo01',), profiles=profiles)
    b = _budget(servers, backend)
    # the preferred server although more loaded (1 of 100 free)
    r = b.reserve('wf', 1, 1)
    assert r.counted == {'m0': 'CVUT.demo01~fast'} and r.profile == '#1 "prefer accurate"'
    # only the fast server qualifies, and its slot is taken now
    r = b.reserve('wf', 1, 0)
    assert r.status == BUSY and r.compatible == {'m0': 1}


def test_unreachable_servers():
    """Unreachable instances are skipped while another one is reachable; none reachable is UNAVAILABLE."""
    servers, backend = FakeModelServers(), FakeBackend()
    servers.add('CVUT.demo01', free=None)
    servers.add('CVUT.demo01~node2', base='CVUT.demo01', free=1)
    backend.addWorkflow('wf', 1, jobmanagers=('CVUT.demo01',))
    b = _budget(servers, backend)
    assert b.reserve('wf', 1).counted == {'m0': 'CVUT.demo01~node2'}
    servers.free['CVUT.demo01~node2'] = None
    assert _budget(servers, backend).reserve('wf', 1).status == UNAVAILABLE


def test_unavailable_and_local():
    """No compatible server, an unreadable workflow or a failed lookup is UNAVAILABLE; a workflow without remote models always fits."""
    servers, backend = FakeModelServers(), FakeBackend()
    backend.addWorkflow('wf', 1, jobmanagers=('jm1',))
    backend.addWorkflow('local', 1, jobmanagers=())
    b = _budget(servers, backend)
    assert b.reserve('wf', 1).status == UNAVAILABLE
    assert b.reserve('missing', 1).status == UNAVAILABLE
    assert b.reserve('local', 1).status == OK
    servers.add('jm1')
    assert _budget(servers, backend, failFind=True).reserve('wf', 1).status == UNAVAILABLE


def test_invalid_profile():
    """An execution profile the workflow does not have is an error of the execution, not of the resources."""
    servers, backend = FakeModelServers(), FakeBackend()
    servers.add('jm1')
    backend.addWorkflow('wf', 1, jobmanagers=('jm1',))
    with pytest.raises(InvalidProfileError):
        _budget(servers, backend).reserve('wf', 1, 0)


def test_terminate_execution_model_jobs(monkeypatch):
    """
    Only the jobs logging to the execution script's Pyro daemon are terminated; an unreachable jobmanager is skipped.

    The jobs may log to another receiver than ``loggerURI`` in the same daemon (the workflow
    registers its own when it loses ``Execution.Log_URI``), so the daemon address is matched.
    """
    import types
    import mupif as mp
    terminated = []

    class Server:
        def getStatus(self):
            return [types.SimpleNamespace(key='1@jm1', remoteLogUri='PYRO:obj_workflowOwn@10.0.0.5:4001'),   # same script daemon
                    types.SimpleNamespace(key='2@jm1', remoteLogUri='PYRO:obj_other@10.0.0.5:4002'),         # another execution
                    types.SimpleNamespace(key='3@jm1', remoteLogUri=None)]

        def terminateJob(self, key):
            terminated.append(key)

    def connectModelServer(ns, name):
        if name == 'jm2':
            raise RuntimeError('unreachable')
        return Server()
    monkeypatch.setattr(mp.pyroutil, 'connectModelServer', connectModelServer)
    assert terminateExecutionModelJobs(object(), ['jm1', 'jm2', 'jm1'], 'PYRO:obj_script@10.0.0.5:4001') == ['1@jm1']
    assert terminated == ['1@jm1']


def test_nameserver_proxy_per_thread(monkeypatch):
    """
    Model servers are looked up through a nameserver proxy of the calling thread.

    The cleanup runs in job threads; using the main thread's proxy there failed with "the calling
    thread is not the owner of this proxy".
    """
    import threading
    import mupif as mp
    import Pyro5.api
    used = []

    def connectModelServer(ns, name):
        used.append(ns)
        raise RuntimeError('stop here')
    monkeypatch.setattr(mp.pyroutil, 'connectModelServer', connectModelServer)
    ns = Pyro5.api.Proxy('PYRO:Pyro.NameServer@127.0.0.1:1')
    t = threading.Thread(target=lambda: terminateExecutionModelJobs(ns, ['jm'], 'PYRO:x@h:1'))
    t.start()
    t.join()
    assert used[0] is not ns and str(used[0]._pyroUri) == str(ns._pyroUri)
