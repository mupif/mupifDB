import mupif
import mupif as mp
import logging

log = logging.getLogger()

class workflowdemo (mupif.workflow.Workflow):
    def __init__(self, metadata={}):
        """
        Initializes the workflow.
        """
        MD = {
            'Name': 'Demo thermal Problem',
            'ID': '1',
            'Description': 'Demo thermal problem using finite elements on rectangular domain',
            'Model_refs_ID': [],
            'Inputs': [
                {'Name': 'Effective conductivity', 'Type': 'mupif.Property', 'Required': False, 'Type_ID': 'mupif.DataID.PID_HeatConductivityLiquid', 'Units':'W/m/K', 'Set_at':'timestep'},
                {'Name': 'Dimension', 'Type': 'mupif.Property', 'Required': False,'Type_ID': 'mupif.DataID.PID_Dimension', 'Units':'m', 'Obj_ID': [0,1], 'Set_at':'timestep'},
                {'Name': 'Prescribed temperature', 'Type': 'mupif.Property', 'Required': False,'Type_ID': 'mupif.DataID.PID_dirichletBC', 'Units':'K', 'Obj_ID': [0,1,2,3], 'Set_at':'timestep'},
                {'Name': 'External temperature', 'Type': 'mupif.Property', 'Required': False,'Type_ID': 'mupif.DataID.PID_conventionExternalTemperature', 'Units':'K', 'Obj_ID': [0,1,2,3], 'Set_at':'timestep'},
                {'Name': 'Convention coefficient', 'Type': 'mupif.Property', 'Required': False,'Type_ID': 'mupif.DataID.PID_conventionCoefficient', 'Units':'none', 'Obj_ID': [0,1,2,3], 'Set_at':'timestep'}
            ],
            'Outputs': [
                {'Name':'Temperature field', 'Type': 'mupif.Field', 'Required':True,'Type_ID':'mupif.DataID.FID_Temperature', 'Units':'T'}
            ],
            'Models': [],
        }
        super().__init__(metadata=MD)
        self.updateMetadata(metadata)


    def initialize(self, workdir='', metadata=None, validateMetaData=True, **kwargs):
        super().initialize(workdir=workdir, metadata=metadata, validateMetaData=validateMetaData, **kwargs)

    def solveStep(self, istep, stageID=0, runInBackground=False):
        log.info ('Workflow02 solveStep finished')

    def getField(self, fieldID, time, objectID=0):
        if fieldID == mupif.DataID.FID_Temperature:
            return mupif.field.Field(mupif.mesh.UnstructuredMesh(), mupif.FieldID.FID_Temperature,mupif.ValueType.Scalar, 'none', time)
        else:
            pass

    def getCriticalTimeStep(self):
        return 1*mp.U['s']

    def terminate(self):
         super(workflowdemo, self).terminate()

    def getApplicationSignature(self):
        return "thermal 1.0"

    def getAPIVersion(self):
        return "1.0"


if __name__ == "__main__":
    import argparse
    import sys
    import mupifDB.workflowmanager
    from bson import ObjectId

    # execute only if run as a script
    from pymongo import MongoClient
    client = MongoClient()
    db = client.MuPIF

    workflow = workflowdemo()
    wid = 'Workflow99'

    id = db.Workflows.find_one({"_id":wid})
    if (id is None):
        id = mupifDB.restApiControl.postWorkflowFiles(
            'Demo',
            'file://localhost/home/bp/devel/mupifDB/workflows/workflowdemo01.py',
            []
        )
        print("workflow registered")
        exit

    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('-eid', '--executionID', required=True, dest="id")
        args = parser.parse_args()
        weid = args.id
        print ('WEID:', weid)
        wec = mupifDB.workflowmanager.WorkflowExecutionContext(db, ObjectId(args.id))
        inp = wec.getIODataDoc('Inputs')
        print (inp)

        log.info(inp.get('Effective conductivity', None))
        log.info(inp.get('External temperature', obj_id=0))
        
        app = workflowdemo()
        app.initialize(metaData={'Execution': {'ID': weid,'Use_case_ID': '1_1','Task_ID': '1'}})
        mupifDB.workflowmanager.mapInputs(app, db, args.id)
        
        tstep = mupif.timestep.TimeStep(1.,1.,10,'s')
        app.solveStep(tstep)
        mupifDB.workflowmanager.mapOutputs(app, db, args.id, tstep)
        
        app.terminate()
    except Exception as err:
        log.info("Error:" + repr(err))
        app.terminate()
        sys.exit(1)
    except:
        log.info("Unknown error")
        app.terminate()
        sys.exit(1)


    #f = app.getField(FieldID.FID_Temperature, tstep.getTargetTime())
    #f.field2VTKData().tofile('temperature')
