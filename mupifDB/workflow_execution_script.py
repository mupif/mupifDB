import importlib
import sys
import os

# workflowscheduler sets PYTHONPATH env var, thus mupifDB should be normally importable
# unlike mupif

#sys.path.append(os.path.dirname(os.path.abspath(__file__)))
#sys.path.append("/var/lib/mupif/mupifDB/")
#os.environ["MUPIF_LOG_LEVEL"] = "INFO"
#os.environ["MUPIF_LOG_FILE"] = "mupif.log"
import logging
import argparse
import mupifDB
# import mupif as mp

log = logging.getLogger()
log.info('Execution script started')

if __name__ == "__main__":
    workflow = None

    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('-eid', '--executionID', required=True, dest="id")
        args = parser.parse_args()
        weid = args.id

        execution_record = mupifDB.restApiControl.getExecutionRecord(weid)
        if execution_record is None:
            print("Execution not found")
            exit(1)

        workflow_record = mupifDB.restApiControl.getWorkflowRecord(execution_record["WorkflowID"])
        if workflow_record is None:
            print("Workflow not found")
            exit(1)

        #
        moduleImport = importlib.import_module(workflow_record["modulename"])
        print(moduleImport)
        workflow_class = getattr(moduleImport, workflow_record["classname"])
        #

        workflow = workflow_class()
        workflow.initialize(metadata={'Execution': {'ID': weid, 'Use_case_ID': workflow_record["UseCase"], 'Task_ID': execution_record["Task_ID"]}})
        mupifDB.workflowmanager.mapInputs(workflow, args.id)
        workflow.solve()
        mupifDB.workflowmanager.mapOutputs(workflow, args.id, workflow.getExecutionTargetTime())
        workflow.terminate()

    except Exception as err:
        log.exception(err)
        if workflow is not None:
            workflow.terminate()
        sys.exit(1)

    except:
        print("Unknown error")
        log.info("Unknown error")
        if workflow is not None:
            workflow.terminate()
        sys.exit(1)

    sys.exit(0)
