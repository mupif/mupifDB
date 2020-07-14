#!/usr/bin/python3
from pymongo import MongoClient
import mupifDB
from bson import ObjectId
from datetime import datetime
import mupif
import mupif.Physics.PhysicalQuantities as PQ
import argparse

class Workflow02 (mupif.Workflow.Workflow):
    def __init__(self, metaData={}):
        """
        Initializes the workflow.
        """
        MD = {
            'Name': 'Demo Mechanical Problem - cantilever end displacement',
            'ID': 'Workflow02',
            'Description': 'Computes the deflection of a cantilever beam',
            'Model_refs_ID': [],
            'Inputs': [
                {'Type': 'mupif.Property', 'Type_ID': 'mupif.PropertyID.PID_EModulus', 'Name': 'YoungModulus',
                 'Description': 'Young modulus', 'Units': 'Pa', 'Required': True},
                {'Type': 'mupif.Property', 'Type_ID': 'mupif.PropertyID.PID_Dimension', 'Name': 'Dimension','Obj_ID': [0,1,2],
                 'Description': 'Beam Dimensions (LxWxH)', 'Units': 'm', 'Required': True},
                {'Type': 'mupif.Property', 'Type_ID': 'mupif.PropertyID.PID_Force', 'Name': 'Force',
                 'Description': 'End point force', 'Units': 'N', 'Required': True}
            ],
            'Outputs': [
                {'Type': 'mupif.Property', 'Type_ID': 'mupif.PropertyID.PID_maxDisplacement', 'Name': 'End displacement',
                 'Description': 'End of beam displacement', 'Units': 'm'}
            ]
        }
        super(Workflow02, self).__init__(metaData=MD)
        self.updateMetadata(metaData)
    

    def initialize(self, file='', workdir='', targetTime=PQ.PhysicalQuantity(0., 's'), metaData={}, validateMetaData=True, **kwargs):
        super(Workflow02, self).initialize(file, workdir, targetTime, metaData, validateMetaData, **kwargs)

    def solveStep(self, istep, stageID=0, runInBackground=False):
        self.I = (self.b*self.h*self.h*self.h/12.0)
        self.w = self.F*self.l*self.l*self.l/(3.0*self.E*self.I)
        print ('Workflow02 solveStep finished')

    def setProperty(self, property, objectID =0):
        id = property.getPropertyID()
        if (id == mupif.PropertyID.PID_EModulus):
            self.E = property.inUnitsOf('Pa').getValue()
        elif (id == mupif.PropertyID.PID_Force):
            self.F = property.inUnitsOf('N').getValue()
        elif (id == mupif.PropertyID.PID_Dimension):
            val = property.inUnitsOf('m').getValue()
            if (objectID==0):
                self.l = val;
            elif (objectID == 1):
                self.b = val
            elif (objectID == 2):
                self.h = val
        
    def getProperty (self, propID, time, objectID=0):
        md = {
            'Execution': {
                'ID': self.getMetadata('Execution.ID'),
                'Use_case_ID': self.getMetadata('Execution.Use_case_ID'),
                'Task_ID': self.getMetadata('Execution.Task_ID')
            }
        }
        if (propID  == mupif.PropertyID.PID_maxDisplacement):
            return mupif.Property.ConstantProperty(
                    self.w, mupif.PropertyID.PID_maxDisplacement, mupif.ValueType.Scalar, 'm', time, 0,metaData=md)


    def getField(self, fieldID, time, objectID=0):
        pass

    def getCriticalTimeStep(self):
        return PQ.PhysicalQuantity(1.0, 's')

    def terminate(self):
         super(Workflow02, self).terminate()

    def getApplicationSignature(self):
        return "Workflow02 1.0"

    def getAPIVersion(self):
        return "1.0"




if __name__ == "__main__":
    client = MongoClient()
    db = client.MuPIF

    workflow = Workflow02()
    wid = 'Workflow98'

    id = db.Workflows.find_one({"_id":wid})
    if (id is None):
        id = mupifDB.workflowmanager.insertWorkflowDefinition(db,wid,'Demo','1.0','file://localhost/home/bp/devel/mupifDB/workflows/workflowdemo02.py', 
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
    # print (inp)

    app = Workflow02()
    app.initialize(metaData={'Execution': {'ID': weid,'Use_case_ID': '1_1','Task_ID': '1'}})
    mupifDB.workflowmanager.mapInputs(app, db, args.id)

    tstep = mupif.TimeStep.TimeStep(1.,1.,10,'s')
    print("Solving....")
    app.solveStep(tstep)
    mupifDB.workflowmanager.mapOutputs(app, db, args.id, tstep)
    
    app.terminate()

