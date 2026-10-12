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
    """The 48 h history shifts by whole hours, resets after long gaps and refuses to go back in time."""
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
    now[0] -= 3 * 3600
    with pytest.raises(RuntimeError):
        h.advance()


def test_stat_hist_selfcheck_includes_load():
    """A history with a load list of the wrong length (e.g. from an old stat file) is reset (finding #9)."""
    h = SchedulerStat.Hist(interval=3600, count=3, processed=[1, 2, 3], pooled=[1, 2, 3], finished=[1, 2, 3], failed=[0, 0, 0], load=[1.])
    h.selfCheck()
    assert h.load == [0, 0, 0] and h.processed == [0, 0, 0]
