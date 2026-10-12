"""
Configuration of the workflow scheduler.

All settings have defaults suitable for the deployment container. Each can be overridden by an
environment variable named ``MUPIFDB_SCHEDULER_<SETTING>`` (setting name in upper case), e.g.
``MUPIFDB_SCHEDULER_POLL_INTERVAL=5``. ``python -m mupifDB.scheduler --print-config`` lists all
settings with their current values, defaults and environment variables.

Connection settings shared with other MuPIF components keep their own variables and are not part of
this class: ``MUPIF_NS`` (Pyro nameserver), ``MUPIFDB_REST_SERVER`` (REST API) and
``API_CREDENTIALS_FILE`` (REST API account).
"""

import os
import socket
import tempfile
from typing import Any

import pydantic

#: prefix of the environment variables overriding the settings
ENV_PREFIX = 'MUPIFDB_SCHEDULER_'


class SchedulerConfig(pydantic.BaseModel):
    """
    Settings of one scheduler process.

    Create it with :meth:`fromEnv` to apply the environment overrides; constructing it directly
    (``SchedulerConfig(max_jobs=2)``) uses only the given values and the defaults, which is what
    tests do.
    """

    model_config = pydantic.ConfigDict(extra='forbid', validate_assignment=True)

    scheduler_id: str = pydantic.Field(default_factory=lambda: f'{socket.gethostname()}-{os.getpid()}',
                                       description='Identifier of this scheduler process (default: host name and process ID).')
    max_jobs: int = pydantic.Field(30, ge=1, description='Maximum number of executions run at the same time.')
    poll_interval: float = pydantic.Field(20., gt=0, description='Seconds between two passes over the pending executions.')
    startup_delay: float = pydantic.Field(5., ge=0, description='Seconds to wait at startup, giving the other services of a freshly started deployment time to come up.')
    max_attempts: int = pydantic.Field(12, ge=1, description='Attempts after which an execution whose resources stay unavailable returns to Created.')
    fetch_limit: int = pydantic.Field(300, ge=1, description='Maximum number of pending executions read from the database per pass.')
    work_dir: str = pydantic.Field(default_factory=tempfile.gettempdir, description='Directory in which a temporary working directory is created for each execution.')
    kill_grace: float = pydantic.Field(10., ge=0, description='Seconds a stopped workflow gets between SIGTERM and SIGKILL.')
    stat_file: str = pydantic.Field('/var/lib/mupif/persistent/scheduler-stat.json', description='File keeping the statistics across restarts.')
    pidfile: str = pydantic.Field('mupifDB_scheduler_pidfile', description='PID file preventing a second scheduler from starting; the REST API reads it (relative to its working directory) to report whether the scheduler runs.')

    @classmethod
    def envVar(cls, name: str) -> str:
        """
        Name of the environment variable overriding a setting.

        :param name: setting (field) name, e.g. ``poll_interval``
        :return: e.g. ``MUPIFDB_SCHEDULER_POLL_INTERVAL``
        """
        return ENV_PREFIX + name.upper()

    @classmethod
    def fromEnv(cls, environ: dict[str, str] | None = None, **overrides: Any) -> 'SchedulerConfig':
        """
        Build the configuration from the defaults, the environment and explicit overrides.

        Values from the environment are strings; they are converted and validated by pydantic, so
        an invalid value (e.g. ``MUPIFDB_SCHEDULER_MAX_JOBS=many``) raises an error at startup.

        :param environ: environment to read (default: :data:`os.environ`)
        :param overrides: settings taking precedence over the environment
        :return: validated configuration
        :raises pydantic.ValidationError: if a value is invalid
        """
        environ = os.environ if environ is None else environ
        values: dict[str, Any] = {name: environ[cls.envVar(name)] for name in cls.model_fields if cls.envVar(name) in environ}
        values.update(overrides)
        return cls(**values)

    def describe(self) -> list[tuple[str, str, Any, Any, str]]:
        """
        Document all settings.

        :return: one ``(setting, environment variable, current value, default, description)``
                 tuple per setting
        """
        rows = []
        for name, field in type(self).model_fields.items():
            default = '(computed)' if field.default_factory is not None else field.default
            rows.append((name, self.envVar(name), getattr(self, name), default, field.description or ''))
        return rows
