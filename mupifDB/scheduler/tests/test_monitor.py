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


def test_statistics_report_load_history(monkeypatch, synced):
    """getStatistics() reports running and waiting executions with their history and its time axis."""
    from mupifDB.scheduler import stats as statsModule
    monkeypatch.setattr(statsModule.time, 'time', lambda: 1000 * 3600 + 125)
    m = SchedulerMonitor(None, capacity=4)
    m.sample({'Pending': 5, 'Running': 2})
    m.enqueued('E1')
    m.started('E1', 'wf')
    st = m.getStatistics()
    assert (st['capacity'], st['runningTasks'], st['waitingTasks'], st['pendingTasks'], st['dbRunningTasks']) == (4, 1, 4, 4, 2)
    assert len(st['running48']) == len(st['waiting48']) == 48
    assert len(st['running120m']) == len(st['waiting120m']) == 120
    assert st['running120m'][-1] == 1 and st['waiting120m'][-1] == 5     # peak: 5 waiting before E1 started
    assert (st['periodStart48'], st['periodLength48']) == (1000 * 3600, 3600)
    assert (st['periodStart120m'], st['periodLength120m']) == (1000 * 3600 + 120, 60)


def test_stop_calls_back(tmp_path, synced):
    """stop() stops the scheduler through the callback and unregisters the monitor."""
    stopped, removed = [], []
    m = SchedulerMonitor(None, capacity=4, onStop=lambda: stopped.append(1))
    m.ns = type('NS', (), {'remove': lambda self, name: removed.append(name)})()
    m.stop()
    assert stopped == [1] and removed == ['mupif.scheduler']


def test_only_control_methods_exposed():
    """Over Pyro the monitor offers reading the statistics, stopping and drain mode, not the update methods."""
    from Pyro5.server import _get_exposed_members
    exposed = _get_exposed_members(SchedulerMonitor(None, capacity=1), only_exposed=True)['methods']
    assert exposed == {'getStatistics', 'stop', 'setDrain'}


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


def test_sample_and_taken_execution_is_not_counted_twice(synced):
    """A sampled Pending execution taken by the scheduler moves from the queue to the taken ones; waiting does not grow."""
    m = SchedulerMonitor(None, capacity=4)
    m.sample({'Pending': 3, 'Running': 0})
    assert m.stat.tasks.waiting == 3
    m.enqueued('E1')
    assert (m.stat.tasks.pending, m.stat.tasks.scheduled, m.stat.tasks.waiting) == (2, 1, 3)
    m.started('E1', 'wf')
    assert (m.stat.tasks.waiting, m.stat.tasks.running) == (2, 1)


def test_database_copy_is_one_request_with_all_fields(synced):
    """The statistics are copied to the database with all fields at once (they used to be four requests)."""
    m = SchedulerMonitor(None, capacity=4)
    m.sample({'Pending': 2, 'Running': 0})
    m.enqueued('E1')
    assert synced[-1] == dict(runningTasks=0, scheduledTasks=1, processedTasks=0, load=0, waitingTasks=2, pendingTasks=1)


def test_slow_database_copy_does_not_block_statistics(monkeypatch):
    """
    While the statistics are being copied to the database, they can be read and updated.

    The copy used to be made under the monitor's lock: a slow REST API blocked the job threads and
    getStatistics(), which then timed out in the monitoring pages (scheduler shown as unreachable).
    """
    import threading
    from mupifDB import restApiControl
    entered, release, made = threading.Event(), threading.Event(), []

    def slow(**kw):
        made.append(kw)
        entered.set()
        assert release.wait(10)
    monkeypatch.setattr(restApiControl, 'setStatScheduler', slow)
    m = SchedulerMonitor(None, capacity=4)
    t = threading.Thread(target=m.enqueued, args=('E1',))
    t.start()
    assert entered.wait(10)                         # the copy hangs in the REST call
    assert m.getStatistics()['scheduledTasks'] == 1  # not blocked
    t2 = threading.Thread(target=m.started, args=('E1', 'wf'))
    t2.start()
    for _ in range(100):                            # the counter is updated before the copy
        if m.stat.tasks.running == 1:
            break
        threading.Event().wait(0.01)
    assert m.stat.tasks.running == 1
    release.set()
    t.join(10)
    t2.join(10)
    assert made[-1]['runningTasks'] == 1            # the newest statistics were copied last


def test_unchanged_statistics_are_not_copied_again(synced):
    """A thread whose change was already copied by another one does not copy it again."""
    m = SchedulerMonitor(None, capacity=4)
    m.enqueued('E1')
    n = len(synced)
    m.syncDb()
    assert len(synced) == n
