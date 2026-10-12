"""
Tests of :mod:`mupifDB.scheduler.runner` with real subprocesses.

The execution script is replaced by a small fake whose behaviour is selected by the execution ID
it is given (``-eid``): finish, fail, report missing resources, kill itself, hang (also with a
child process, or ignoring SIGTERM).
"""

import os
import threading
import time
import types

import pytest

from mupifDB.scheduler.config import SchedulerConfig
from mupifDB.scheduler.runner import Job, JobRunner, statusForExitCode
from mupifDB.scheduler.staging import StagingError

_FAKE_SCRIPT = r'''
import os, signal, subprocess, sys, time
weid = sys.argv[sys.argv.index('-eid') + 1]
print('fake execution', weid, 'module present:', os.path.exists('fakewf.py'), flush=True)
if weid == 'ok':
    sys.exit(0)
if weid == 'fail':
    sys.exit(1)
if weid == 'noresources':
    sys.exit(2)
if weid == 'killself':
    os.kill(os.getpid(), signal.SIGKILL)
if weid == 'ignoreterm':
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
if weid in ('hang', 'ignoreterm'):
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
    with open(os.environ['FAKE_PIDFILE'], 'w') as f:
        f.write(str(child.pid))
    time.sleep(60)
'''


@pytest.fixture
def runner(tmp_path, monkeypatch):
    """Return a JobRunner with the fake execution script, a staging step writing ``fakewf.py`` and a short kill grace."""
    script = tmp_path / 'workflow_execution_script.py'
    script.write_text(_FAKE_SCRIPT)
    monkeypatch.setenv('FAKE_PIDFILE', str(tmp_path / 'child.pid'))

    def stage(record, workDir):
        (workDir / 'fakewf.py').write_text('')
        return workDir / 'fakewf.py'
    config = SchedulerConfig(work_dir=str(tmp_path / 'work'), kill_grace=0.5)
    return JobRunner(config, nsUri='PYRO:Pyro.NameServer@127.0.0.1:1', stage=stage, executionScript=script)


def _run(runner, weid):
    """Run job *weid*; return (result, stored log text, number of onLaunch calls)."""
    logs, launched = [], []
    result = runner.run(Job(weid=weid, record=None), types.SimpleNamespace(),
                        onLaunch=lambda: launched.append(1), storeLog=lambda path: logs.append(path.read_text()))
    return result, (logs[0] if logs else None), len(launched)


@pytest.mark.parametrize('weid,status,exitCode', [('ok', 'Finished', 0), ('fail', 'Failed', 1), ('noresources', 'Pending', 2), ('killself', 'Failed', -9)])
def test_outcome_from_exit_code(runner, weid, status, exitCode):
    """The exit code decides the outcome; the log has the subprocess output between a header and a footer."""
    result, log, launched = _run(runner, weid)
    assert (result.status, result.exitCode, result.stopReason, launched) == (status, exitCode, None, 1)
    assert 'WORKFLOW STARTING' in log and f'fake execution {weid} module present: True' in log
    assert 'exit status of' in log and f': {exitCode} ' in log


def test_staging_error_propagates_before_launch(runner):
    """A staging failure raises before anything is launched (the caller knows the workflow did not run)."""
    def badStage(record, workDir):
        raise StagingError('no module')
    runner.stage = badStage
    launched = []
    with pytest.raises(StagingError):
        runner.run(Job(weid='ok', record=None), None, onLaunch=lambda: launched.append(1), storeLog=lambda p: None)
    assert launched == []


def _waitFor(path, timeout=20):
    """Wait until *path* exists and return its content."""
    t0 = time.time()
    while not os.path.exists(path):
        assert time.time() - t0 < timeout, f'{path} not created'
        time.sleep(0.05)
    time.sleep(0.05)
    return open(path).read()


def _alive(pid):
    """Tell whether process *pid* exists (and is not a zombie)."""
    try:
        with open(f'/proc/{pid}/stat') as f:
            return f.read().split()[2] != 'Z'
    except FileNotFoundError:
        return False


@pytest.mark.parametrize('weid', ['hang', 'ignoreterm'])
def test_stop_kills_process_group(runner, tmp_path, weid):
    """
    Stopping a job terminates the workflow and every process it started.

    Also when the workflow ignores SIGTERM (it gets SIGKILL after the grace period). A stop for
    shutdown returns the execution to Pending, so that it is run again after the restart.
    """
    job = Job(weid=weid, record=None)
    out = {}
    t = threading.Thread(target=lambda: out.update(result=runner.run(job, None, onLaunch=lambda: None, storeLog=lambda p: out.update(log=p.read_text()))))
    t.start()
    childPid = int(_waitFor(str(tmp_path / 'child.pid')))
    t0 = time.time()
    job.stop('shutdown')
    t.join(timeout=20)
    assert not t.is_alive()
    assert time.time() - t0 < 5
    assert (out['result'].status, out['result'].stopReason) == ('Pending', 'shutdown')
    assert 'stopped by the scheduler: shutdown' in out['log']
    for _ in range(50):
        if not _alive(childPid):
            break
        time.sleep(0.1)
    assert not _alive(childPid)


def test_stop_for_other_reasons_fails(runner, tmp_path):
    """A job stopped for any other reason than shutdown (e.g. a timeout) fails."""
    job = Job(weid='hang', record=None)
    out = {}
    t = threading.Thread(target=lambda: out.update(result=runner.run(job, None, onLaunch=lambda: None, storeLog=lambda p: None)))
    t.start()
    _waitFor(str(tmp_path / 'child.pid'))
    job.stop('timeout')
    t.join(timeout=20)
    assert (out['result'].status, out['result'].stopReason) == ('Failed', 'timeout')


def test_environment_of_subprocess(runner, monkeypatch):
    """The subprocess finds mupifDB, the nameserver and the REST API account."""
    monkeypatch.setenv('API_CREDENTIALS_FILE', '/etc/creds.json')
    monkeypatch.setenv('PYTHONPATH', '/somewhere')
    env = runner.environment()
    assert env['MUPIF_NS'] == 'PYRO:Pyro.NameServer@127.0.0.1:1'
    assert env['API_CREDENTIALS_FILE'] == '/etc/creds.json'
    assert env['PYTHONPATH'].startswith('/somewhere' + os.pathsep)


def test_status_for_exit_code():
    """Every code other than 0 (finished) and 2 (no resources) is a failure, signals included."""
    assert [statusForExitCode(c) for c in (0, 1, 2, -9, 137)] == ['Finished', 'Failed', 'Pending', 'Failed', 'Failed']

