"""
Unit tests of the workflow scheduler (:mod:`mupifDB.workflowscheduler`).

These tests need no running services: no MongoDB, REST API, Pyro nameserver or model server.
Calls to the REST API are replaced by fakes through ``monkeypatch``. End-to-end behaviour is
covered by ``test_scheduler.py`` (pytest + xprocess) and ``tools/scheduler_smoketest.py``
(real local deployment).

Each test names the finding of the refactoring plan it guards against, where applicable.
"""
import os
import subprocess
import sys
import time


def test_import_has_no_side_effects():
    """
    Importing the scheduler module must not sleep, connect to the nameserver or fail.

    Regression for finding #12: the import used to sleep 5 s and connect to the nameserver, so it
    failed (after 5 s) whenever no nameserver was reachable, e.g. in unit tests. The import runs in
    a fresh interpreter, with ``MUPIF_NS`` pointing to a port where nothing listens.
    """
    code = 'import mupifDB.workflowscheduler as ws; assert ws.ns is None and ws.ns_uri is None'
    env = dict(os.environ, MUPIF_NS='127.0.0.1:1')
    # mupifDB itself (mupif, pydantic, ...) takes about a second to import; the scheduler module adds
    # only milliseconds now, while the old import alone took 5 s
    t0 = time.time()
    subprocess.run([sys.executable, '-c', code], env=env, check=True, cwd=os.path.dirname(__file__) + '/..', timeout=60)
    assert time.time() - t0 < 4.5


class _FakeNameserver:
    """Stands in for the Pyro nameserver proxy; records removed names."""

    def __init__(self):
        self.removed = []

    def remove(self, name):
        self.removed.append(name)


def test_stop_ends_main_loop(monkeypatch, tmp_path):
    """
    ``SchedulerMonitor.stop()`` must end the main loop promptly.

    Regression for finding #1: ``stop()`` assigned a local variable instead of the global flag, so
    the loop never ended. The stop must also interrupt the sleep between iterations, so the loop has
    to return well within one ``LOOP_SLEEP_SEC`` interval, without starting another iteration.
    """
    from mupifDB import workflowscheduler as ws
    monkeypatch.setattr(ws, 'LOOP_SLEEP_SEC', 3)
    monkeypatch.setattr(ws, 'schedulerStatFile', str(tmp_path / 'stat.json'))
    monkeypatch.setattr(ws, 'stopEvent', ws.threading.Event())
    fakeNs = _FakeNameserver()
    monitor = ws.SchedulerMonitor(fakeNs)
    iterations = []

    def schedulePending(pool):
        iterations.append(time.time())
        assert len(iterations) == 1, 'loop continued after stop()'
        if len(iterations) == 1:
            # request the stop from another thread while the loop is about to sleep
            ws.threading.Timer(0.2, monitor.stop).start()

    class _Monitor:
        def persistStat(self):
            pass

    monkeypatch.setattr(ws, 'scheduler_schedule_pending', schedulePending)
    t0 = time.time()
    ws.runLoop(pool=None, monitor=_Monitor())
    assert time.time() - t0 < 2
    assert fakeNs.removed == ['mupif.scheduler']
