import mupifDB
import mupif as mp
import argparse
import logging
log=logging.getLogger()

class Workflow02 (mp.Workflow):
    def __init__(self, metadata=None):
        """
        Initializes the workflow.
        """
        MD = {
            'Name': 'Demo HeavyStruct',
            'ID': 'Workflow04',
            'Description': 'Demo HeavyStruct input/output workflow',
            'Model_refs_ID': [],
            'Inputs': [
                {'Type': 'mupif.HeavyStruct', 'Type_ID': 'mupif.DataID.ID_MoleculeState', 'Name': 'inputHS', 'Description': '', 'Units': '', 'ValueType': 'Scalar', 'Required': True, 'Obj_ID': '', "Set_at": "timestep"}
            ],
            'Outputs': [
                {'Type': 'mupif.HeavyStruct', 'Type_ID': 'mupif.DataID.ID_MoleculeState', 'Name': 'outputHS', 'Description': '', 'Units': '', 'ValueType': 'Scalar', 'Obj_ID': ''}
            ]
        }
        super().__init__(metadata=MD)
        self.updateMetadata(metadata)
        self.inputHS = None
        self.outputHS = None

    def initialize(self, workdir='', metadata=None, validateMetaData=True, **kwargs):
        super().initialize(workdir=workdir, metadata=metadata, validateMetaData=validateMetaData, **kwargs)

    def solveStep(self, istep, stageID=0, runInBackground=False):
        self.outputHS = mp.HeavyStruct(h5path='test-molecule.h5', mode='readonly', id=mp.DataID.ID_MoleculeState)
        print ('Workflow solveStep finished')

    def set(self, obj, objectID=''):
        if obj.isInstance(mp.HeavyStruct) and obj.getDataID() == mp.DataID.ID_MoleculeState and objectID == "":
            self.inputHS = obj
        
    def get(self, objectTypeID, time=None, objectID=''):
        if objectTypeID == mp.DataID.ID_MoleculeState and objectID == "":
            return self.outputHS
        return None

    def getCriticalTimeStep(self):
        return 1*mp.U.s

    def getApplicationSignature(self):
        return "Workflow04 1.0"

    def getAPIVersion(self):
        return "1.0"


if __name__ == "__main__":

    w = Workflow02()

    w.initialize(metadata={'Execution': {'ID': '', 'Use_case_ID': '', 'Task_ID': ''}})

    input = mp.HeavyStruct(h5path='test-molecule.h5', mode='readonly', id=mp.DataID.ID_MoleculeState)
    w.set(input, objectID='')

    w.solve()

    out = w.get(mp.DataID.ID_MoleculeState, objectID='', time=None)
    print(out)

    w.terminate()

