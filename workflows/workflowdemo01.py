import mupif
import mupif.Physics.PhysicalQuantities as PQ
import logging
from pymongo import MongoClient
import argparse
import mupifDB.workflowmanager
from bson import ObjectId

log = logging.getLogger()

class workflowdemo (mupif.Workflow.Workflow):
    def __init__(self, metaData={}):
        """
        Initializes the workflow.
        """
        MD = {
            'Name': 'Demo thermal Problem',
            'ID': '1',
            'Description': 'Demo thermal problem using finite elements on rectangular domain',
            'Model_refs_ID': [],
            'Inputs': [
                {'Name': 'Effective conductivity', 'Type': 'mupif.Property', 'Required': False, 'Type_ID': 'mupif.PropertyID.PID_effective_conductivity', 'Units':'W/m/K'},
                {'Name': 'Dimension', 'Type': 'mupif.Property', 'Required': False,'Type_ID': 'mupif.PropertyID.PID_Dimension', 'Units':'m', 'Obj_ID': [0,1]},
                {'Name': 'Prescribed temperature', 'Type': 'mupif.Property', 'Required': False,'Type_ID': 'mupif.PropertyID.PID_dirichletBC', 'Units':'K', 'Obj_ID': [0,1,2,3]},
                {'Name': 'External temperature', 'Type': 'mupif.Property', 'Required': False,'Type_ID': 'mupif.PropertyID.PID_conventionExternalTemperature', 'Units':'K', 'Obj_ID': [0,1,2,3]},
                {'Name': 'Convention coefficient', 'Type': 'mupif.Property', 'Required': False,'Type_ID': 'mupif.PropertyID.PID_conventionCoefficient', 'Units':'none', 'Obj_ID': [0,1,2,3]}
            ],
            'Outputs': [
                {'Name':'Temperature field', 'Type': 'mupif.Field', 'Required':True,'Type_ID':'mupif.FieldID.FID_Temperature', 'Units':'T'}
            ]
        }
        super(workflowdemo, self).__init__(metaData=MD)
        self.updateMetadata(metaData)


    def initialize(self, file='', workdir='', targetTime=PQ.PhysicalQuantity(0., 's'), metaData={}, validateMetaData=True, **kwargs):
        super(workflowdemo, self).initialize(file, workdir, targetTime, metaData, validateMetaData, **kwargs)

    def solveStep(self, istep, stageID=0, runInBackground=False):
        log.info ('Workflow02 solveStep finished')

    def getField(self, fieldID, time, objectID=0):
        if fieldID == mupif.FieldID.FID_Temperature:
            return mupif.Field.Field(mupif.Mesh.UnstructuredMesh(), mupif.FieldID.FID_Temperature,mupif.ValueType.Scalar, 'none', time)
        else:
            pass

    def getCriticalTimeStep(self):
        return PQ.PhysicalQuantity(1.0, 's')

    def terminate(self):
         super(workflowdemo, self).terminate()

    def getApplicationSignature(self):
        return "thermal 1.0"

    def getAPIVersion(self):
        return "1.0"


if __name__ == "__main__":
    # execute only if run as a script
    client = MongoClient()
    db = client.MuPIF

    workflow = workflowdemo()
    wid = 'Workflow99'

    id = db.Workflows.find_one({"_id":wid})
    if (id is None):
        id = mupifDB.workflowmanager.insertWorkflowDefinition(db,wid,'Demo','1.0','file://localhost/home/bp/devel/mupifDB/workflows/workflowdemo01.py', 
                                                    'DemoUseCase', workflow.getMetadata('Inputs'), workflow.getMetadata('Outputs'))
        print("workflow registered")
        exit


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

    tstep = mupif.TimeStep.TimeStep(1.,1.,10,'s')
    app.solveStep(tstep)
    mupifDB.workflowmanager.mapOutputs(app, db, args.id, tstep)
    
    app.terminate()

    #f = app.getField(FieldID.FID_Temperature, tstep.getTargetTime())
    #f.field2VTKData().tofile('temperature')