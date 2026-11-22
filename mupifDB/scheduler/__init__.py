"""
Workflow scheduler of MupifDB.

The scheduler takes executions submitted to the database (status ``Pending``), runs their workflows
and stores the results. It is started by ``mupifDB/workflowscheduler.py`` (or
``python -m mupifDB.scheduler``); its behaviour for users and operators is described in
``doc/source/scheduler.md``.

Components and data flow::

                      +-------------------- scheduler process --------------------+
    REST API  <-----> | ExecutionBackend (backend)                                 |
    (database)        |      ^                                                     |
                      |      |  pending executions, status changes, logs           |
                      | Scheduler (core) -- main loop: every poll_interval         |
                      |      |   take executions within free job slots and the     |
                      |      |   ResourceBudget (resources) of model-server slots  |
                      |      v                                                     |
                      | job threads -- JobRunner (runner): stage (staging),        |
                      |      |        run the subprocess, wait, store the log      |
                      |      |                                                     |
                      | SchedulerMonitor (monitor) <- statistics (stats)           |
                      +------|----------------------------|-----------------------+
                             |                            | Pyro: getStatistics, stop
                             v                            v
              workflow_execution_script.py         monitoring pages
              (one subprocess and process group
               per execution) --Pyro--> model servers

* :mod:`~mupifDB.scheduler.config` -- :class:`SchedulerConfig`: all settings, overridable by
  ``MUPIFDB_SCHEDULER_*`` environment variables.
* :mod:`~mupifDB.scheduler.core` -- :class:`~mupifDB.scheduler.core.Scheduler`: main loop, job threads,
  every status change the scheduler makes (listed in the module documentation).
* :mod:`~mupifDB.scheduler.backend` -- :class:`~mupifDB.scheduler.backend.ExecutionBackend`: all access
  to the database (implemented over the REST API).
* :mod:`~mupifDB.scheduler.runner` -- :class:`~mupifDB.scheduler.runner.JobRunner`: one execution as a
  subprocess; the exit-code contract with the execution script.
* :mod:`~mupifDB.scheduler.resources` -- model-server slot budget of one pass.
* :mod:`~mupifDB.scheduler.staging` -- workflow package into the working directory.
* :mod:`~mupifDB.scheduler.monitor`, :mod:`~mupifDB.scheduler.stats` -- statistics, served over Pyro.

Threads: the main loop; one job thread per running execution (at most ``max_jobs``); the Pyro daemon
of the monitor. Shared state: the set of taken executions (``Scheduler.jobs``, guarded by
``Scheduler.lock``) and the statistics (guarded by the monitor's lock). The workflows run in
subprocesses, never in threads of the scheduler process.
"""

from .config import SchedulerConfig

__all__ = ['SchedulerConfig']
