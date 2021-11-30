import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import mupif as mp
import logging
import argparse
import mupifDB
from bson import ObjectId
import sys

from modulename_to_be_replaced import classname_to_be_replaced as workflow_classname

log = logging.getLogger()


if __name__ == "__main__":
    app = None

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

        targetTime = float(execution_record['targetTime'])*mp.U.s
        dt = float(execution_record['dt'])*mp.U.s

        wec = mupifDB.workflowmanager.WorkflowExecutionContext(ObjectId(args.id))
        inp = wec.getIODataDoc('Inputs')

        app = workflow_classname()
        app.initialize(metadata={'Execution': {'ID': weid, 'Use_case_ID': workflow_record["UseCase"], 'Task_ID': '1'}}, targetTime=targetTime, dt=dt)
        mupifDB.workflowmanager.mapInputs(app, args.id)
        app.solve()
        mupifDB.workflowmanager.mapOutputs(app, args.id, app.getLastTimestep())
        app.terminate()

    except Exception as err:
        print("Error:" + repr(err))
        log.info("Error:" + repr(err))
        if app is not None:
            app.terminate()
        sys.exit(1)

    except:
        print("Unknown error")
        log.info("Unknown error")
        if app is not None:
            app.terminate()
        sys.exit(1)

    sys.exit(0)
