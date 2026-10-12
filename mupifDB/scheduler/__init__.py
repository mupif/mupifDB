"""
Workflow scheduler of MupifDB, as a package.

The scheduler takes executions submitted to the database (status ``Pending``), runs their workflows
and stores the results; see ``doc/source/scheduler.md`` for its behaviour. This package is being
built up from the former single module ``mupifDB/workflowscheduler.py``, which remains the entry
point started by the deployment.

Modules:

* :mod:`~mupifDB.scheduler.config` -- :class:`SchedulerConfig`, settings and their environment
  variables.
"""

from .config import SchedulerConfig

__all__ = ['SchedulerConfig']
