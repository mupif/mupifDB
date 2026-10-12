"""Tests of :class:`mupifDB.scheduler.monitor.SchedulerMonitor` (no Pyro server, REST API replaced)."""

import pytest

from mupifDB.scheduler.monitor import SchedulerMonitor


@pytest.fixture
def synced(monkeypatch):
    """Record what the statistics copy to the database instead of calling the REST API."""
    from mupifDB import restApiControl
    made = []
    monkeypatch.setattr(restApiControl, 'setStatScheduler', lambda **kw: made.append(kw))
    return made


def test_accounting(tmp_path, synced):
    """Counters follow executions: taken -> started -> finished/failed/returned to the queue (finding #9)."""
    m = SchedulerMonitor(str(tmp_path / 'stat.json'), capacity=4)
    for weid in ('E1', 'E2', 'E3', 'E4'):
        m.enqueued(weid)
    for weid in ('E1', 'E2', 'E3'):
        m.started(weid, 'wf')
    m.dropped('E4')
    m.finished('E1', 'Finished')
    m.finished('E2', 'Failed')
    m.finished('E3', 'Pending')
    t, h = m.stat.tasks, m.stat.hist48h
    assert (t.scheduled, t.running, t.processed, t.finished, t.failed) == (0, 0, 2, 1, 1)
    assert (h.pooled[-1], h.processed[-1]) == (4, 2)
    assert [j.status for j in m.stat.lastJobs] == ['Finished', 'Failed', 'Pending']
    assert synced[-1]['processedTasks'] == 2


def test_load_uses_capacity(tmp_path, synced):
    """The load is the percentage of the scheduler's capacity in use."""
    m = SchedulerMonitor(None, capacity=4)
    m.enqueued('E1')
    m.started('E1', 'wf')
    assert m.getStatistics()['currentLoad'] == 25


def test_restart_keeps_totals(tmp_path, synced):
    """Totals survive a restart (statistics file); per-process counters start from zero."""
    path = str(tmp_path / 'stat.json')
    m = SchedulerMonitor(path, capacity=4)
    m.enqueued('E1')
    m.started('E1', 'wf')
    m.finished('E1', 'Finished')
    m.enqueued('E2')
    m.started('E2', 'wf')
    m.persist()
    t = SchedulerMonitor(path, capacity=4).stat.tasks
    assert (t.processed, t.finished, t.running, t.scheduled) == (1, 1, 0, 0)


def test_statistics_format(tmp_path, synced):
    """getStatistics() keeps the keys read by mupif.monitor.schedulerInfo."""
    stats = SchedulerMonitor(None, capacity=4).getStatistics()
    assert {'runningTasks', 'scheduledTasks', 'processedTasks', 'finishedTasks', 'failedTasks', 'currentLoad', 'lastJobs',
            'pooledTasks48', 'processedTasks48', 'finishedTasks48', 'failedTasks48', 'load48'} <= set(stats)


def test_stop_calls_back(tmp_path, synced):
    """stop() stops the scheduler through the callback and unregisters the monitor."""
    stopped, removed = [], []
    m = SchedulerMonitor(None, capacity=4, onStop=lambda: stopped.append(1))
    m.ns = type('NS', (), {'remove': lambda self, name: removed.append(name)})()
    m.stop()
    assert stopped == [1] and removed == ['mupif.scheduler']


def test_only_two_methods_exposed():
    """Over Pyro the monitor offers reading the statistics and stopping, not the update methods."""
    from Pyro5.server import _get_exposed_members
    exposed = _get_exposed_members(SchedulerMonitor(None, capacity=1), only_exposed=True)['methods']
    assert exposed == {'getStatistics', 'stop'}


@pytest.mark.parametrize('address,allowed', [('127.0.0.1', True), ('10.255.7.9', False)])
def test_stop_only_from_scheduler_host(monkeypatch, synced, address, allowed):
    """
    stop() called over Pyro is accepted from the scheduler's host only.

    It used to be callable by anyone able to reach the scheduler (e.g. over the VPN).
    """
    import Pyro5.callcontext
    stopped = []
    m = SchedulerMonitor(None, capacity=1, onStop=lambda: stopped.append(1))
    monkeypatch.setattr(Pyro5.callcontext.current_context, 'client', object(), raising=False)
    monkeypatch.setattr(Pyro5.callcontext.current_context, 'client_sock_addr', (address, 40000), raising=False)
    if allowed:
        m.stop()
    else:
        with pytest.raises(PermissionError):
            m.stop()
    assert stopped == ([1] if allowed else [])


def test_database_errors_do_not_propagate(monkeypatch, tmp_path):
    """A failure to copy the statistics to the database is logged, not raised (finding #16)."""
    from mupifDB import restApiControl

    def fail(**kw):
        raise RuntimeError('401 Unauthorized')
    monkeypatch.setattr(restApiControl, 'setStatScheduler', fail)
    m = SchedulerMonitor(str(tmp_path / 'stat.json'), capacity=2)
    m.enqueued('E1')
    m.persist()
    assert m.stat.tasks.scheduled == 1


def test_fresh_start(synced, tmp_path):
    """Without a statistics file the history is initialized and the statistics can be stored at once."""
    m = SchedulerMonitor(str(tmp_path / 'missing.json'), capacity=2)
    m.persist()
    assert len(m.stat.hist48h.load) == m.stat.hist48h.count


def test_run_server_uses_shared_daemon(monkeypatch):
    """
    The monitor is served by mupif's shared Pyro daemon, whose thread ends with the process.

    Without a daemon, mupif.pyroutil.runServer starts an exclusive one in a non-daemon thread, and
    the scheduler process stayed alive after shutting down (seen on SIGTERM).
    """
    import mupif as mp
    calls = {}
    monkeypatch.setattr(mp.pyroutil, 'getDaemon', lambda proxy=None, exclusive=False: calls.setdefault('getDaemon', (proxy, exclusive)) and 'DAEMON')
    monkeypatch.setattr(mp.pyroutil, 'runServer', lambda **kw: calls.setdefault('runServer', kw) and 'PYRO:monitor')
    ns = object()
    SchedulerMonitor(None, capacity=1).runServer(ns)
    assert calls['getDaemon'] == (ns, False)
    assert calls['runServer']['daemon'] == 'DAEMON' and calls['runServer']['appName'] == 'mupif.scheduler'
