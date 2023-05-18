import importlib
import sys
import os

# os.environ["MUPIF_LOG_LEVEL"] = "INFO"
# os.environ["MUPIF_LOG_FILE"] = "mupif.log"

import logging
import argparse
import mupifDB
import mupif as mp


log = logging.getLogger('workflow_execution_script')
log.setLevel(logging.DEBUG)
tailHandler=mp.pyrolog.TailLogHandler(capacity=10000)
log.addHandler(tailHandler)
log.info(f'Execution script started with args: {sys.argv}')


api_type = os.environ.get('MUPIFDB_REST_SERVER_TYPE', "mupif")
log.info(f'Database API type is {api_type}')

# connect to nameserver (uses MUPIF_NS env var) so that getDaemon binds the correct network address
ns = mp.pyroutil.connectNameserver()
daemon = mp.pyroutil.getDaemon(proxy=ns)
logUri = str(daemon.register(mp.pyrolog.PyroLogReceiver(tailHandler=tailHandler)))

if __name__ == "__main__":
    workflow = None
    weid = None
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('-eid', '--executionID', required=True, dest="id")
        args = parser.parse_args()
        weid = args.id
        
        # add REST logging handler for this weid, add 'weid' field to every message automatically
        import mupifDB.restLogger
        log.addHandler(mupifDB.restLogger.RestLogHandler(extraData={'weid':weid}))

        execution_record = mupifDB.restApiControl.getExecutionRecord(weid)
        if execution_record is None:
            log.error("Execution not found")
            sys.exit(1)

        workflow_record = mupifDB.restApiControl.getWorkflowRecordGeneral(execution_record["WorkflowID"], execution_record["WorkflowVersion"])
        if workflow_record is None:
            log.error("Workflow not found")
            mupifDB.restApiControl.setExecutionStatusFailed(weid)
            sys.exit(1)

        #
        moduleImport = importlib.import_module(workflow_record["modulename"])
        log.info(f'Imported workflow module {moduleImport}')
        workflow_class = getattr(moduleImport, workflow_record["classname"])
        #

        workflow = workflow_class()
        workflow.initialize(metadata={'Execution': {'ID': weid, 'Use_case_ID': workflow_record["UseCase"], 'Task_ID': execution_record["Task_ID"], 'Log_URI': logUri}})
        wfUri = str(daemon.register(workflow))
        mupifDB.restApiControl.setExecutionParameter(weid,'workflowURI',wfUri)
        mupifDB.restApiControl.setExecutionParameter(weid,'loggerURI',logUri)
        mupifDB.workflowmanager.mapInputs(workflow, weid)
        workflow.solve()
        mupifDB.workflowmanager.mapOutputs(workflow, weid, workflow.getExecutionTargetTime())
        workflow.terminate()

    except Exception as err:
        log.exception(err)
        try:
            workflow.terminate()
        except:
            pass
        if type(err) == mp.JobManNoResourcesException:
            log.error('Not enough resources')
            mupifDB.restApiControl.setExecutionStatusFailed(weid)
            sys.exit(2)
        mupifDB.restApiControl.setExecutionStatusFailed(weid)
        sys.exit(1)

    except:
        log.info("Unknown error")
        if workflow is not None:
            workflow.terminate()
        mupifDB.restApiControl.setExecutionStatusFailed(weid)
        sys.exit(1)

    mupifDB.restApiControl.setExecutionStatusFinished(weid)
    sys.exit(0)
