"""Tests of :mod:`mupifDB.scheduler.stats`."""

import os

import pytest

from mupifDB.scheduler.stats import SchedulerStat


def test_save_and_load_roundtrip(tmp_path):
    """Statistics written by save_to_file are read back unchanged, and no temporary file is left behind."""
    stat = SchedulerStat()
    stat.advanceTime()
    stat.tasks.processed = 7
    stat.hist48h.finished[-1] = 3
    path = str(tmp_path / 'stat.json')
    stat.save_to_file(path)
    assert os.listdir(tmp_path) == ['stat.json']
    loaded = SchedulerStat.load_from_file(path)
    assert loaded.tasks.processed == 7 and loaded.hist48h.finished[-1] == 3


def test_failed_write_keeps_previous_file(tmp_path, monkeypatch):
    """If writing fails midway, the previous statistics file stays intact."""
    path = str(tmp_path / 'stat.json')
    old = SchedulerStat()
    old.tasks.processed = 1
    old.save_to_file(path)

    def broken(*args, **kw):
        raise OSError('disk full')
    new = SchedulerStat()
    monkeypatch.setattr(SchedulerStat, 'model_dump_json', broken)
    try:
        new.save_to_file(path)
    except OSError:
        pass
    monkeypatch.undo()
    assert SchedulerStat.load_from_file(path).tasks.processed == 1


def test_stat_hist_advance(monkeypatch):
    """The 48 h history shifts by whole hours, resets after long gaps and stays put if the clock goes back."""
    from mupifDB.scheduler import stats
    now = [100 * 3600 + 10]
    monkeypatch.setattr(stats.time, 'time', lambda: now[0])
    h = SchedulerStat.Hist(interval=3600, count=4)
    h.advance()                                     # initializes the lists
    h.processed[-1] = 5
    now[0] += 60
    h.advance()                                     # same hour: nothing moves
    assert h.processed == [0, 0, 0, 5]
    now[0] += 2 * 3600
    h.advance()
    assert h.processed == [0, 5, 0, 0]
    now[0] += 10 * 3600                             # gap longer than the history
    h.advance()
    assert h.processed == [0, 0, 0, 0]
    h.processed[-1] = 2
    now[0] -= 3 * 3600                              # clock steps back (NTP, VM resync)
    h.advance()                                     # does not raise, nothing moves
    assert h.processed == [0, 0, 0, 2]
    h.processed[-1] += 1                            # updates keep going into the last period
    now[0] += 3 * 3600 + 3600                       # real time catches up and goes on
    h.advance()
    assert h.processed == [0, 0, 3, 0]


def test_monitor_counts_survive_clock_going_back(monkeypatch):
    """A finished execution is subtracted from the running ones even if the clock went back meanwhile."""
    from mupifDB.scheduler import stats
    from mupifDB.scheduler.monitor import SchedulerMonitor
    monkeypatch.setattr(stats.restApiControl, 'setStatScheduler', lambda **kw: None)
    now = [100 * 3600 + 10]
    monkeypatch.setattr(stats.time, 'time', lambda: now[0])
    mon = SchedulerMonitor(None, capacity=4)
    mon.enqueued('e1')
    mon.started('e1', 'w1')
    now[0] -= 2 * 3600
    mon.finished('e1', 'Finished')
    assert mon.stat.tasks.running == 0 and mon.stat.tasks.finished == 1
    assert mon.getStatistics()['runningTasks'] == 0


def test_stat_hist_selfcheck_includes_load():
    """A list of the wrong length (e.g. from an old stat file) is reset; the other lists keep their history (finding #9)."""
    h = SchedulerStat.Hist(interval=3600, count=3, processed=[1, 2, 3], pooled=[1, 2, 3], finished=[1, 2, 3], failed=[0, 0, 0], load=[1.])
    h.selfCheck()
    assert h.load == [0, 0, 0] and h.processed == [1, 2, 3]
    assert h.running == [0, 0, 0] and h.waiting == [0, 0, 0]


def test_gauges_keep_peak_and_carry_level_forward(monkeypatch):
    """
    Gauges (running, waiting, load) keep the peak level of each period; periods without any change get the level of the time.

    Counters start every period at 0, but a gauge must not drop to 0 in an hour in which nothing
    happened: executions started before are still running.
    """
    from mupifDB.scheduler import stats
    now = [100 * 3600 + 10]
    monkeypatch.setattr(stats.time, 'time', lambda: now[0])
    st = SchedulerStat()
    st.hist48h = SchedulerStat.Hist(interval=3600, count=4)
    st.advanceTime()

    def setRunning(n):
        st.advanceTime()
        st.tasks.running = n
        st.updateGauges()
    setRunning(3)
    setRunning(1)                                   # peak 3 stays in this hour
    assert st.hist48h.running[-1] == 3
    now[0] += 2 * 3600                              # two quiet hours, still 1 running
    setRunning(2)
    assert st.hist48h.running == [0, 3, 1, 2]       # skipped hour carries level 1, current peak 2
    st.countEvent('processed')
    assert st.hist48h.processed[-1] == 1 and st.hist2h.processed[-1] == 1


def test_waiting_is_pending_plus_taken():
    """Waiting executions are those in the database queue plus those taken but not started yet."""
    st = SchedulerStat()
    st.advanceTime()
    st.tasks.pending = 5
    st.tasks.scheduled = 2
    st.updateGauges()
    assert st.tasks.waiting == 7
    assert st.hist48h.waiting[-1] == 7 and st.hist2h.waiting[-1] == 7


def test_old_stat_file_keeps_history(tmp_path):
    """A statistics file without the gauges and the 2 h history is read; its counter history is kept."""
    import json
    stat = SchedulerStat()
    stat.advanceTime()
    stat.hist48h.processed[-1] = 4
    data = json.loads(stat.model_dump_json())
    # as written by the previous version
    del data['hist2h'], data['hist48h']['running'], data['hist48h']['waiting']
    for k in ('pending', 'waiting', 'dbRunning'):
        del data['tasks'][k]
    path = tmp_path / 'stat.json'
    path.write_text(json.dumps(data))
    loaded = SchedulerStat.load_from_file(str(path))
    loaded.advanceTime()
    assert loaded.hist48h.processed[-1] == 4
    assert len(loaded.hist48h.running) == 48 and len(loaded.hist2h.waiting) == 120
