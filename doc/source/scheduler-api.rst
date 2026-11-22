Scheduler API reference
=======================

Reference of the :mod:`mupifDB.scheduler` package, generated from its documentation strings. For
the behaviour of the scheduler see :doc:`scheduler`.

.. automodule:: mupifDB.scheduler

Entry point
-----------

.. automodule:: mupifDB.workflowscheduler
   :members: build, main, installSignalHandlers

Configuration
-------------

.. automodule:: mupifDB.scheduler.config
   :members:

Scheduler core
--------------

.. automodule:: mupifDB.scheduler.core
   :members:

Database access
---------------

.. automodule:: mupifDB.scheduler.backend
   :members:

Running an execution
--------------------

.. automodule:: mupifDB.scheduler.runner
   :members:

Model-server resources
----------------------

.. automodule:: mupifDB.scheduler.resources
   :members:

Workflow packages
-----------------

.. automodule:: mupifDB.scheduler.staging
   :members:

Statistics
----------

.. automodule:: mupifDB.scheduler.monitor
   :members:

.. automodule:: mupifDB.scheduler.stats
   :members:

Execution script
----------------

.. automodule:: mupifDB.workflow_execution_script
   :members: main, isNoResourcesError, dieWithScheduler, downloadWorkflowFiles, EXIT_TERMINATED
