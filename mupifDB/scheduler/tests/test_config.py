"""Tests of :class:`mupifDB.scheduler.config.SchedulerConfig`."""

import pydantic
import pytest

from mupifDB.scheduler.config import SchedulerConfig


def test_defaults_match_former_constants():
    """The defaults are the values hard-coded in the scheduler before (pool size, loop interval, attempts, ...)."""
    c = SchedulerConfig()
    assert (c.max_jobs, c.poll_interval, c.startup_delay, c.max_attempts, c.fetch_limit) == (30, 20., 5., 12, 300)
    assert c.stat_file == '/var/lib/mupif/persistent/scheduler-stat.json'


def test_environment_overrides():
    """``MUPIFDB_SCHEDULER_<SETTING>`` variables override the defaults, explicit arguments override both."""
    env = {'MUPIFDB_SCHEDULER_MAX_JOBS': '4', 'MUPIFDB_SCHEDULER_POLL_INTERVAL': '0.5', 'UNRELATED': 'x'}
    c = SchedulerConfig.fromEnv(env)
    assert (c.max_jobs, c.poll_interval) == (4, 0.5)
    assert SchedulerConfig.fromEnv(env, max_jobs=8).max_jobs == 8


def test_invalid_value_is_rejected():
    """An invalid value fails at startup instead of misbehaving later."""
    with pytest.raises(pydantic.ValidationError):
        SchedulerConfig.fromEnv({'MUPIFDB_SCHEDULER_MAX_JOBS': '0'})
    with pytest.raises(pydantic.ValidationError):
        SchedulerConfig(unknown_setting=1)


def test_print_config(capsys):
    """``python -m mupifDB.scheduler --print-config`` documents every setting with its variable."""
    from mupifDB.scheduler.__main__ import main
    assert main(['--print-config']) == 0
    out = capsys.readouterr().out
    for name in SchedulerConfig.model_fields:
        assert name in out and SchedulerConfig.envVar(name) in out
