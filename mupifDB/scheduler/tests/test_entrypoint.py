"""Tests of the entry point ``mupifDB/workflowscheduler.py``. Moved from ``mupifDB/test_scheduler_unit.py``."""

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
    code = 'import mupifDB.workflowscheduler'
    env = dict(os.environ, MUPIF_NS='127.0.0.1:1')
    # mupifDB itself (mupif, pydantic, ...) takes about a second to import; the scheduler module adds
    # only milliseconds now, while the old import alone took 5 s
    t0 = time.time()
    subprocess.run([sys.executable, '-c', code], env=env, check=True, cwd=os.path.dirname(__file__) + '/../../..', timeout=60)
    assert time.time() - t0 < 4.5


def test_build_wires_the_scheduler(monkeypatch, tmp_path):
    """build() connects the monitor's stop to the scheduler and runs the subprocesses against the nameserver."""
    from mupifDB import restApiControl
    from mupifDB import workflowscheduler as ws
    from mupifDB.scheduler.config import SchedulerConfig
    from mupifDB.scheduler.monitor import SchedulerMonitor
    monkeypatch.setattr(restApiControl, 'setStatScheduler', lambda **kw: None)
    served = []
    monkeypatch.setattr(SchedulerMonitor, 'runServer', lambda self, ns: served.append(ns) or 'PYRO:monitor')
    ns = type('NS', (), {'_pyroUri': 'PYRO:Pyro.NameServer@127.0.0.1:1'})()
    scheduler = ws.build(SchedulerConfig(max_jobs=2, stat_file=str(tmp_path / 'stat.json'), work_dir=str(tmp_path)), ns)
    try:
        assert served == [ns]
        assert scheduler.runner.nsUri == 'PYRO:Pyro.NameServer@127.0.0.1:1'
        scheduler.monitor.onStop()
        assert scheduler.stopEvent.is_set()
    finally:
        scheduler.executor.shutdown()


def test_sigterm_stops_the_scheduler():
    """SIGTERM (from the service manager) asks the scheduler to shut down in an orderly way instead of killing it."""
    import signal
    from mupifDB import workflowscheduler as ws
    stopped = []
    scheduler = type('S', (), {'stop': lambda self: stopped.append(1)})()
    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        ws.installSignalHandlers(scheduler)
        os.kill(os.getpid(), signal.SIGTERM)
        time.sleep(0.1)
    finally:
        for sig, h in previous.items():
            signal.signal(sig, h)
    assert stopped == [1]
