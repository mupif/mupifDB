#!/usr/bin/python3
from pymongo import MongoClient
import mupifDB
from bson import ObjectId
from datetime import datetime
import mupif
import mupif.Physics.PhysicalQuantities as PQ

class Workflow02 (mupif.Workflow.Workflow):
    def __init__(self, metaData={}):
        """
        Initializes the workflow.
        """
        MD = {
            'Name': 'Demo Mechanical Problem',
            'ID': 'Workflow02',
            'Description': 'Demo mechanical problem using finite elements on rectangular domain',
            'Model_refs_ID': [],
            'Inputs': [
                {'Type': 'mupif.Property', 'Type_ID': 'mupif.PropertyID.PID_EModulus', 'Name': 'YoungModulus',
                 'Description': 'Young modulus', 'Units': 'Pa', 'Required': True},
                {'Type': 'mupif.Property', 'Type_ID': 'mupif.PropertyID.PID_PoissonRatio', 'Name': 'PoissonRatio',
                 'Description': 'Poisson ratio', 'Units': '', 'Required': False},
            ],
            'Outputs': [
                {'Type': 'mupif.Field', 'Type_ID': 'mupif.FieldID.FID_Displacement', 'Name': 'DisplacementVector',
                 'Description': 'Displacement field on 2D domain', 'Units': 'm'}
            ]
        }
        super(Workflow02, self).__init__(metaData=MD)
        self.updateMetadata(metaData)


    def initialize(self, file='', workdir='', metaData={}, validateMetaData=True, **kwargs):
        super(Workflow02, self).initialize(file, workdir, metaData, validateMetaData, **kwargs)

    def solveStep(self, istep, stageID=0, runInBackground=False):
        print ('Workflow02 solveStep finished')
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





client = MongoClient()
db = client.MuPIF

workflow = Workflow02()
wid = 'Workflow02'

id = db.Workflows.find_one({"_id":wid})
if (id is None):
    id = mupifDB.workflowmanager.insertWorkflowDefinition(db,wid,'Demo','1.0','file://localhost/home/bp/devel/mupifDB/examples/Demo01/demo.py', 
                                                'DemoUseCase', workflow.getMetadata('Inputs'), workflow.getMetadata('Outputs'))
c = mupifDB.workflowmanager.WorkflowExecutionContext.create(db, 'Workflow02', 'borek.patzak@gmail.com')

print (c.executionID)
c.getIODataDoc('Inputs').set('YoungModulus', 30.e9)
print (c.getIODataDoc('Inputs').get('YoungModulus'))
c.getIODataDoc('Outputs').set('Displacementvector', ObjectId())

c.set('StartDate', str(datetime.now()))
