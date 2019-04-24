import mupif
import mupif.Physics.PhysicalQuantities as PQ



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
                {'Name': 'Effective conductivity', 'Type': 'mupif.Property', 'Required': False, 'Type_ID': 'mupif.PropertyID.PID_effective_conductivity', 'Units':'W/m/K', 'Obj_ID': None},
                {'Name': 'Dimension', 'Type': 'mupif.Property', 'Required': False,'Type_ID': 'mupif.PropertyID.PID_Dimension', 'Units':'m', 'Obj_ID': (0,1)},
                {'Name': 'Prescribed temperature', 'Type': 'mupif.Property', 'Required': True,'Type_ID': 'mupif.PropertyID.PID_dirichletBC', 'Units':'K', 'Obj_ID': (0,1,2,3)},
                {'Name': 'External temperature', 'Type': 'mupif.Property', 'Required': True,'Type_ID': 'mupif.PropertyID.PID_conventionExternalTemperature', 'Units':'K', 'Obj_ID': (0,1,2,3)},
                {'Name': 'Convention coefficient', 'Type': 'mupif.Property', 'Required': True,'Type_ID': 'PID_conventionCoefficient', 'Units':'none', 'Obj_ID': (0,1,2,3)}
            ],
            'Outputs': [
                {'Name':'Temperature field', 'Type': 'Field', 'Required':True,'Type_ID':'mupif.FieldID.FID_Temperature', 'Units':'T', 'Obj_ID': None}
            ]
        }
        super(workflowdemo, self).__init__(metaData=MD)
        self.updateMetadata(metaData)


    def initialize(self, file='', workdir='', metaData={}, validateMetaData=True, **kwargs):
        super(workflowdemo, self).initialize(file, workdir, metaData, validateMetaData, **kwargs)

    def solveStep(self, istep, stageID=0, runInBackground=False):
        print ('Workflow02 solveStep finished')
    def getField(self, fieldID, time, objectID=0):
        pass

    def getCriticalTimeStep(self):
        return PQ.PhysicalQuantity(1.0, 's')

    def terminate(self):
         super(workflowdemo, self).terminate()

    def getApplicationSignature(self):
        return "thermal 1.0"

    def getAPIVersion(self):
        return "1.0"
