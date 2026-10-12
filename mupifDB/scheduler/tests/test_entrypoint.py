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
    code = 'import mupifDB.workflowscheduler as ws; assert ws.ns is None and ws.ns_uri is None'
    env = dict(os.environ, MUPIF_NS='127.0.0.1:1')
    # mupifDB itself (mupif, pydantic, ...) takes about a second to import; the scheduler module adds
    # only milliseconds now, while the old import alone took 5 s
    t0 = time.time()
    subprocess.run([sys.executable, '-c', code], env=env, check=True, cwd=os.path.dirname(__file__) + '/../../..', timeout=60)
    assert time.time() - t0 < 4.5
