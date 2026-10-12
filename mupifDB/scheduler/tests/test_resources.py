"""Tests of :class:`mupifDB.scheduler.resources.ResourceBudget`, without REST API or nameserver."""

from mupifDB.scheduler.resources import ResourceBudget, modelServerFreeSlots


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
