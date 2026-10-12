"""
Run a single workflow execution; started by the scheduler as a subprocess.

The scheduler copies this script into a temporary directory together with the workflow package and
runs ``python workflow_execution_script.py -eid <execution ID>`` there. The script imports the
workflow class, maps the execution inputs from the database onto the workflow, solves it, and maps
the outputs back to the database.

The process exit code tells the scheduler how the execution ended:

* ``0`` -- the workflow finished;
* ``1`` -- the workflow failed;
* ``2`` -- a model server had no free resources; the scheduler returns the execution to
  ``Pending`` so that it is tried again later.

Environment: ``MUPIF_NS`` (nameserver), ``MUPIFDB_REST_SERVER`` and ``API_CREDENTIALS_FILE``
(REST API access), set by the scheduler.
"""
import importlib
import sys
import os

# os.environ["MUPIF_LOG_LEVEL"] = "INFO"
# os.environ["MUPIF_LOG_FILE"] = "mupif.log"

import logging
import argparse
import mupifDB
import mupif as mp
import Pyro5.errors

#: fully qualified name of the exception a model server raises when it has no free job slot
NO_RESOURCES_EXCEPTION = 'mupif.modelserverbase.ModelServerNoResourcesException'


def isNoResourcesError(err: BaseException | None) -> bool:
    """
    Tell whether *err* (or an exception it was raised from) means "model server has no free slot".

    The exception is raised on the model server and reaches the workflow through Pyro. Pyro
    re-raises it locally as the same class when it can serialize it; when it cannot -- which is
    what happens with :class:`mupif.ModelServerNoResourcesException` (the server reports "Error
    serializing exception") -- the client gets a generic :class:`Pyro5.errors.PyroError` whose
    message and remote traceback name the original class. Both forms are recognized, also when
    wrapped in other exceptions (``raise ... from ...`` or raised while handling it).

    :param err: exception caught around the workflow run
    :return: true if the execution failed only for lack of resources and should be retried later
    """
    seen = set()
    while err is not None and id(err) not in seen:
        seen.add(id(err))
        if isinstance(err, mp.ModelServerNoResourcesException):
            return True
        remoteTraceback = ''.join(getattr(err, '_pyroTraceback', None) or [])
        if NO_RESOURCES_EXCEPTION in str(err) or NO_RESOURCES_EXCEPTION in remoteTraceback:
            return True
        err = err.__cause__ or err.__context__
    return False


def downloadWorkflowFiles(eid):
    import zipfile
    we_rec = mupifDB.restApiControl.getExecutionRecord(eid)
    workflowVersion = int(we_rec.WorkflowVersion)
    wid = we_rec.WorkflowID
    workflow_record = mupifDB.restApiControl.getWorkflowRecord(wid=wid, version=workflowVersion)

    python_script_filename = workflow_record.modulename + ".py"

    fc, fn = mupifDB.restApiControl.getBinaryFileByID(workflow_record.GridFSID)
    file_path = f'./{fn}'
    with open(file_path, "wb") as f:
        f.write(fc)
        f.close()

    if fn.split('.')[-1] == 'py':
        log.info("downloaded .py file..")
        if fn == python_script_filename:
            log.info("Filename check OK")
        else:
            log.info("Filename check FAILED")

    elif fn.split('.')[-1] == 'zip':
        log.info("downloaded .zip file, extracting..")
        log.info(fn)
        zf = zipfile.ZipFile(file_path, mode='r')
        filenames = zipfile.ZipFile.namelist(zf)
        log.info("Zipped files:")
        log.info(filenames)
        zf.extractall(path='./')
        if python_script_filename in filenames:
            log.info("Filename check OK")
        else:
            log.error("Filename check FAILED")


log = logging.getLogger('workflow_execution_script')
log.setLevel(logging.DEBUG)
tailHandler=mp.pyrolog.TailLogHandler(capacity=10000)
log.addHandler(tailHandler)


def main():
    """
    Run one workflow execution and exit; see the module documentation for the exit codes.

    Connecting to the nameserver and registering the Pyro daemon happen here rather than at import
    time, so that the module can be imported (e.g. by unit tests) without a running nameserver.
    """
    log.info(f'Execution script started with args: {sys.argv}')

    # connect to nameserver (uses MUPIF_NS env var) so that getDaemon binds the correct network address
    ns = mp.pyroutil.connectNameserver()
    daemon = mp.pyroutil.getDaemon(proxy=ns)
    logUri = str(daemon.register(mp.pyrolog.PyroLogReceiver(tailHandler=tailHandler)))

    workflow = None
    weid = None
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('-eid', '--executionID', required=True, dest="id")
        parser.add_argument('-download_workflow', '--download_workflow', required=False, dest="download", default=False)
        args = parser.parse_args()
        weid = args.id

        if args.download:
            downloadWorkflowFiles(weid)

        mupifDB.restApiControl.setExecutionStatus(weid, 'Running')
        
        # add REST logging handler for this weid, add 'weid' field to every message automatically
        # (a "from" import: "import mupifDB.restLogger" would make mupifDB a local name of main())
        from mupifDB import restLogger
        log.addHandler(restLogger.RestLogHandler(extraData={'weid': weid}))

        execution_record = mupifDB.restApiControl.getExecutionRecord(weid)
        if execution_record is None:
            log.error("Execution not found")
            sys.exit(1)

        workflow_record = mupifDB.restApiControl.getWorkflowRecord(execution_record.WorkflowID, execution_record.WorkflowVersion)
        if workflow_record is None:
            log.error("Workflow not found")
            mupifDB.restApiControl.setExecutionStatus(weid, 'Failed')
            sys.exit(1)

        #
        moduleImport = importlib.import_module(workflow_record.modulename)
        log.info(f'Imported workflow module {moduleImport}')
        workflow_class = getattr(moduleImport, workflow_record.classname)
        #

        workflow = workflow_class()
        workflow.initialize(metadata={'Execution': {'ID': weid, 'Use_case_ID': workflow_record.UseCase, 'Task_ID': execution_record.Task_ID, 'Log_URI': logUri}})
        wfUri = str(daemon.register(workflow))
        mupifDB.restApiControl.setExecutionParameter(weid, 'workflowURI', wfUri)
        mupifDB.restApiControl.setExecutionParameter(weid, 'loggerURI', logUri)
        mupifDB.workflowmanager.mapInputs(workflow, weid)
        workflow.solve()
        mupifDB.workflowmanager.mapOutputs(workflow, weid, workflow.getExecutionTargetTime())
        workflow.terminate()

    except Exception as err:
        log.exception(err)
        if hasattr(err, '_pyroTraceback'):
            log.error(''.join(Pyro5.errors.get_pyro_traceback()))
        try:
            if workflow is not None:
                workflow.terminate()
        except:
            pass
        if isNoResourcesError(err):
            # not a failure of the workflow: leave the status to the scheduler, which returns the
            # execution to Pending (exit code 2) so that it is retried when resources are free
            log.error('Not enough resources')
            sys.exit(2)
        mupifDB.restApiControl.setExecutionStatus(str(weid), 'Failed')
        sys.exit(1)

    except:
        log.info("Unknown error")
        if workflow is not None:
            workflow.terminate()
        mupifDB.restApiControl.setExecutionStatus(str(weid), 'Failed')
        sys.exit(1)

    mupifDB.restApiControl.setExecutionStatus(weid, 'Finished')
    sys.exit(0)


if __name__ == "__main__":
    main()
