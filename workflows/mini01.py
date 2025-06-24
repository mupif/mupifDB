import mupif
import mupif as mp
import logging
log = logging.getLogger()

class MiniWorkflow1 (mupif.workflow.Workflow):
    def __init__(self, metadata={}):
        """
        Initializes the workflow.
        """
        MD = {
            'Name': 'Minimal Workflow',
            'ID': '1',
            'Description': 'Demo thermal problem using finite elements on rectangular domain',
            'Model_refs_ID': [],
            'Inputs': [
            ],
            'Outputs': [
                {'Name':'Length', 'Type': 'mupif.Property', 'TypeID':'mupif.DataID.PID_Length', 'Units':'m', 'ValueType': 'Scalar', 'Obj_ID': ''}
            ],
            'Models': [],
        }
        super().__init__(metadata=MD)
        self.updateMetadata(metadata)


    def initialize(self, workdir='', metadata=None, validateMetaData=True, **kwargs):
        super().initialize(workdir=workdir, metadata=metadata, validateMetaData=validateMetaData, **kwargs)

    def set(self, obj, objectID=''):
        raise mp.APIError('This workflow has no inputs')

    def get(self, objectTypeID, time=None, objectID=''):
        md = {
            'Execution': {
                'ID': self.getMetadata('Execution.ID'),
                'Use_case_ID': self.getMetadata('Execution.Use_case_ID'),
                'Task_ID': self.getMetadata('Execution.Task_ID')
            }
        }
        if objectTypeID == mp.DataID.PID_Length:
            return mp.ConstantProperty(value=42, propID=mp.DataID.PID_Length, valueType=mp.ValueType.Scalar, unit=mp.U.m, time=time, metadata=md)
        else:
            raise mp.APIError('Unknown property ID')

    def solveStep(self, istep, stageID=0, runInBackground=False):
        log.info ('MiniWorkflow1.solveStep')
        pass

    def getCriticalTimeStep(self):
        return 1.*mp.U.s

    def getApplicationSignature(self):
        return "thermal 1.0"

    def getAPIVersion(self):
        return "1.0"

