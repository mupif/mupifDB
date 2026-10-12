"""Tests of :mod:`mupifDB.scheduler.stats`."""

import os

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
