"""Tests of :class:`mupifDB.scheduler.resources.ResourceBudget`, without REST API or nameserver."""

from mupifDB.scheduler.resources import ResourceBudget, modelServerFreeSlots, terminateExecutionModelJobs


def _budget(jobmanagers, free):
    """Budget over fake workflows (``{wid: [jobmanager, ...]}``) and jobmanagers (``{name: slots or None}``)."""
    queries = []

    def freeJobSlots(name):
        queries.append(name)
        return free[name]

    def workflowJobmanagers(wid, version):
        if wid not in jobmanagers:
            raise KeyError(wid)
        return jobmanagers[wid]
    b = ResourceBudget(workflowJobmanagers, freeJobSlots)
    b.queries = queries
    return b


def test_slots_are_consumed_within_a_pass():
    """Each reservation takes one slot per model; free slots are read once per jobmanager."""
    b = _budget({'wf': ['jm1', 'jm2']}, {'jm1': 2, 'jm2': 5})
    assert [b.reserve('wf', 1) for _ in range(3)] == [ResourceBudget.OK, ResourceBudget.OK, ResourceBudget.BUSY]
    assert b.free == {'jm1': 0, 'jm2': 3}
    assert sorted(b.queries) == ['jm1', 'jm2']


def test_unavailable_and_local():
    """An unreachable jobmanager or an unreadable workflow is UNAVAILABLE; a workflow without remote models always fits."""
    b = _budget({'wf': ['jm1'], 'local': []}, {'jm1': None})
    assert b.reserve('wf', 1) == ResourceBudget.UNAVAILABLE
    assert b.reserve('missing', 1) == ResourceBudget.UNAVAILABLE
    assert b.reserve('local', 1) == ResourceBudget.OK


def test_model_server_free_slots_unreachable(monkeypatch):
    """modelServerFreeSlots() reports None instead of raising when the jobmanager cannot be reached."""
    import mupif as mp
    import Pyro5.errors

    def connectModelServer(ns, name):
        raise Pyro5.errors.NamingError(f'unknown name: {name}')
    monkeypatch.setattr(mp.pyroutil, 'connectModelServer', connectModelServer)
    assert modelServerFreeSlots(object(), 'CVUT.missing') is None


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
