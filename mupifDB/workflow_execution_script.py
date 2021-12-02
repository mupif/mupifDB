import importlib
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import mupif as mp
import logging
import argparse
import mupifDB
from bson import ObjectId
import sys

# from modulename_to_be_replaced import classname_to_be_replaced as workflow_class

log = logging.getLogger()


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

        wec = mupifDB.workflowmanager.WorkflowExecutionContext(ObjectId(args.id))
        inp = wec.getIODataDoc('Inputs')

        workflow = workflow_class()
        workflow.initialize(metadata={'Execution': {'ID': weid, 'Use_case_ID': workflow_record["UseCase"], 'Task_ID': '1'}})
        mupifDB.workflowmanager.mapInputs(workflow, args.id)
        workflow.solve()
        mupifDB.workflowmanager.mapOutputs(workflow, args.id, workflow.getExecutionTargetTime())
        workflow.terminate()

    except Exception as err:
        print("Error:" + repr(err))
        log.info("Error:" + repr(err))
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
