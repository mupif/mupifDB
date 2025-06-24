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
            'Name': 'Demo Mechanical Problem - cantilever end displacement',
            'ID': 'Workflow02',
            'Description': 'Computes the deflection of a cantilever beam',
            'Model_refs_ID': [],
            'Inputs': [
                {'Type': 'mupif.Property', 'Type_ID': 'mupif.DataID.PID_EModulus', 'Name': 'YoungModulus', 'Description': 'Young modulus', 'Units': 'Pa', 'Required': True, 'ValueType': 'Scalar', 'Obj_ID': '', "Set_at": "timestep"},
                {'Type': 'mupif.Property', 'Type_ID': 'mupif.DataID.PID_Dimension', 'Name': 'Dimension', 'Obj_ID': 'length', 'Description': 'Beam length', 'Units': 'm', 'ValueType': 'Scalar', 'Required': True, "Set_at": "timestep"},
                {'Type': 'mupif.Property', 'Type_ID': 'mupif.DataID.PID_Dimension', 'Name': 'Dimension', 'Obj_ID': 'width', 'Description': 'Beam width', 'Units': 'm', 'ValueType': 'Scalar', 'Required': True, "Set_at": "timestep"},
                {'Type': 'mupif.Property', 'Type_ID': 'mupif.DataID.PID_Dimension', 'Name': 'Dimension', 'Obj_ID': 'height', 'Description': 'Beam height', 'Units': 'm', 'ValueType': 'Scalar', 'Required': True, "Set_at": "timestep"},
                {'Type': 'mupif.Property', 'Type_ID': 'mupif.DataID.PID_Force', 'Name': 'Force', 'Description': 'End point force', 'Units': 'N', 'Required': True, 'ValueType': 'Scalar', 'Obj_ID': '', "Set_at": "timestep"}
            ],
            'Outputs': [
                {'Type': 'mupif.Property', 'Type_ID': 'mupif.DataID.PID_maxDisplacement', 'Name': 'End displacement', 'Description': 'End of beam displacement', 'Units': 'm', 'ValueType': 'Scalar', 'Obj_ID': ''}
            ]
        }
        super().__init__(metadata=MD)
        self.updateMetadata(metadata)

        self.E = None
        self.F = None
        self.l = None
        self.b = None
        self.h = None

    def initialize(self, workdir='', metadata=None, validateMetaData=True, **kwargs):
        super().initialize(workdir=workdir, metadata=metadata, validateMetaData=validateMetaData, **kwargs)

    def solveStep(self, istep, stageID=0, runInBackground=False):
        self.I = (self.b*self.h*self.h*self.h/12.0)
        self.w = self.F*self.l*self.l*self.l/(3.0*self.E*self.I)
        print ('Workflow02 solveStep finished')

    def set(self, obj, objectID=''):
        pid = obj.getDataID()
        if pid == mp.DataID.PID_EModulus:
            self.E = obj.inUnitsOf('Pa').getValue()
        elif pid == mp.DataID.PID_Force:
            self.F = obj.inUnitsOf('N').getValue()
        elif pid == mp.DataID.PID_Dimension:
            val = obj.inUnitsOf('m').getValue()
            if objectID == 'length':
                self.l = val
            elif objectID == 'width':
                self.b = val
            elif objectID == 'height':
                self.h = val
        
    def get(self, objectTypeID, time=None, objectID=''):
        if objectTypeID == mp.DataID.PID_maxDisplacement:
            return mp.ConstantProperty(
                value=self.w,
                unit='m',
                propID=mp.DataID.PID_maxDisplacement,
                valueType=mp.ValueType.Scalar,
                time=None
            )
        return None

    def getCriticalTimeStep(self):
        return 1*mp.U.s

    def getApplicationSignature(self):
        return "Workflow02 1.0"

    def getAPIVersion(self):
        return "1.0"


if __name__ == "__main__":

    w = Workflow02()

    w.initialize(metadata={'Execution': {'ID': '', 'Use_case_ID': '', 'Task_ID': ''}})

    w.set(mp.ConstantProperty(value=10000000000, propID=mp.DataID.PID_EModulus, valueType=mp.ValueType.Scalar, unit=mp.U.Pa, time=None), '')
    w.set(mp.ConstantProperty(value=10000, propID=mp.DataID.PID_Force, valueType=mp.ValueType.Scalar, unit=mp.U.N, time=None), '')
    w.set(mp.ConstantProperty(value=10, propID=mp.DataID.PID_Dimension, valueType=mp.ValueType.Scalar, unit=mp.U.m, time=None), 'length')
    w.set(mp.ConstantProperty(value=1, propID=mp.DataID.PID_Dimension, valueType=mp.ValueType.Scalar, unit=mp.U.m, time=None), 'width')
    w.set(mp.ConstantProperty(value=0.4, propID=mp.DataID.PID_Dimension, valueType=mp.ValueType.Scalar, unit=mp.U.m, time=None), 'height')

    w.solve()

    print(w.get(mp.DataID.PID_maxDisplacement))

    w.terminate()

