"""
Entry point of the MupifDB workflow scheduler.

The deployment starts this file as a script (``python3 mupifDB/workflowscheduler.py``, see
``supervisor-mupifdb-master.conf`` in demo_infrastructure); ``python -m mupifDB.scheduler`` does the
same. The scheduler itself is the package :mod:`mupifDB.scheduler` (main loop in
:mod:`mupifDB.scheduler.core`; behaviour described in ``doc/source/scheduler.md``). This module only
wires it up: configuration from the environment (``python -m mupifDB.scheduler --print-config``),
nameserver connection, PID file, statistics monitor served over Pyro, then the main loop.

Environment besides the ``MUPIFDB_SCHEDULER_*`` settings: ``MUPIF_NS`` (Pyro nameserver),
``MUPIFDB_REST_SERVER`` and ``API_CREDENTIALS_FILE`` (REST API).
"""

import sys
import os
# allow running this file as a script from any directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')

import logging
import multiprocessing
import signal
import time

import Pyro5.errors
import pidfile

from mupifDB import restLogger
from mupifDB.scheduler.backend import RestBackend
from mupifDB.scheduler.config import SchedulerConfig
from mupifDB.scheduler.core import Scheduler
from mupifDB.scheduler.monitor import SchedulerMonitor
from mupifDB.scheduler.resources import modelServerFreeSlots, terminateExecutionModelJobs
from mupifDB.scheduler.runner import JobRunner
from mupifDB.scheduler.staging import PackageCache, stageWorkflow

log = logging.getLogger('scheduler')
log.setLevel(logging.INFO)
log.addHandler(restLogger.RestLogHandler())

# decrease verbosity of the HTTP library
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("urllib3").propagate = False


def build(config: SchedulerConfig, ns) -> Scheduler:
    """
    Create the scheduler and serve its statistics monitor over Pyro.

    :param config: settings
    :param ns: Pyro nameserver proxy
    :return: the scheduler, ready to :meth:`~mupifDB.scheduler.core.Scheduler.run`
    """
    monitor = SchedulerMonitor(config.stat_file, capacity=config.max_jobs)
    cache = PackageCache(config.cache_dir or os.path.join(config.work_dir, 'mupifDB-package-cache'), config.cache_max_bytes) if config.cache_max_bytes else None
    runner = JobRunner(config, nsUri=str(ns._pyroUri), stage=lambda record, directory: stageWorkflow(record, directory, cache=cache))
    scheduler = Scheduler(config, RestBackend(), runner, freeJobSlots=lambda jm: modelServerFreeSlots(ns, jm), monitor=monitor,
                          terminateModelJobs=lambda jobmanagers, loggerURI: terminateExecutionModelJobs(ns, jobmanagers, loggerURI))
    monitor.onStop = scheduler.stop
    # copy the statistics of the previous run to the database before anything changes
    monitor.persist()
    monitor.runServer(ns)
    return scheduler


def installSignalHandlers(scheduler: Scheduler) -> None:
    """
    Shut the scheduler down in an orderly way on SIGTERM (service manager) and SIGINT (Ctrl-C).

    The main loop ends and :meth:`~mupifDB.scheduler.core.Scheduler.shutdown` stops the running
    workflows; their executions run again after the restart.

    :param scheduler: the scheduler to stop
    """
    def handler(signum, frame):
        log.warning(f'Received {signal.Signals(signum).name}, shutting down')
        scheduler.stop()
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def main(config: SchedulerConfig | None = None) -> None:
    """
    Run the scheduler until it is stopped.

    Everything with side effects happens here rather than at import time, so that the module can be
    imported (e.g. by tests) without sleeping, connecting to the nameserver or renaming the process.

    :param config: settings (default: :meth:`SchedulerConfig.fromEnv`)
    """
    import mupif as mp
    config = config or SchedulerConfig.fromEnv()
    time.sleep(config.startup_delay)
    multiprocessing.current_process().name = 'mupifDB-scheduler'
    # show remote traceback when remote calls fail
    sys.excepthook = Pyro5.errors.excepthook
    ns = mp.pyroutil.connectNameserver()
    try:
        with pidfile.PIDFile(filename=config.pidfile):
            log.info(f'Starting MupifDB Workflow Scheduler {config.scheduler_id} (up to {config.max_jobs} executions at a time)')
            scheduler = build(config, ns)
            installSignalHandlers(scheduler)
            scheduler.run()
    except pidfile.AlreadyRunningError:
        log.error('Already running.')
    log.info("Exiting MupifDB Workflow Scheduler\n")


if __name__ == '__main__':
    main()
