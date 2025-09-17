import mupif
import copy
import Pyro5
import threading
import time
import logging
import os
import sys
d = os.path.dirname(os.path.abspath(__file__))
sys.path += [d+'/..']

log = logging.getLogger()


@Pyro5.api.expose
class MuPIFThermoMechanicalDemo(mupif.Workflow):

    def __init__(self, metadata=None):
        MD = {
            "ClassName": "MuPIFThermoMechanicalDemo",
            "ModuleName": "workflowdemo03",
            "Name": "MuPIF ThermoMechanical demo of a cantilever beam",
            "ID": "mupif_tmsm_demo",
            "Description": "Calculates thermal and mechanical response of a cantilever beam using two models",
            "Execution_settings": {
                "Type": "Local"
            },
            "Inputs": [
                {"Name": "top_temperature", "Type": "mupif.Property", "Required": True, "description": "", "Type_ID": "mupif.DataID.PID_Temperature", "Obj_ID": "top_temperature", "Units": "deg_C", "Set_at": "timestep", "ValueType": "Scalar"},
                {"Name": "bottom_temperature", "Type": "mupif.Property", "Required": True, "description": "", "Type_ID": "mupif.DataID.PID_Temperature", "Obj_ID": "bottom_temperature", "Units": "deg_C", "Set_at": "timestep", "ValueType": "Scalar"}
            ],
            "Outputs": [
                {"Name": "temperature_field", "Type": "mupif.Field", "description": "", "Type_ID": "mupif.DataID.FID_Temperature", "Obj_ID": "temperature_field", "Units": "deg_C", "ValueType": "Scalar"},
                {"Name": "displacement_field", "Type": "mupif.Field", "description": "", "Type_ID": "mupif.DataID.FID_Displacement", "Obj_ID": "displacement_field", "Units": "m", "ValueType": "Vector"},
                {"Name": "temperature_vtk", "Type": "mupif.PyroFile", "description": "", "Type_ID": "mupif.DataID.ID_VTKFile", "Obj_ID": "temperature_vtk", "Units": "", "ValueType": "Scalar"},
                {"Name": "displacement_vtk", "Type": "mupif.PyroFile", "description": "", "Type_ID": "mupif.DataID.ID_VTKFile", "Obj_ID": "displacement_vtk", "Units": "", "ValueType": "Scalar"},
                {"Name": "temperature_img", "Type": "mupif.PyroFile", "description": "", "Type_ID": "mupif.DataID.ID_Image", "Obj_ID": "temperature_img", "Units": "", "ValueType": "Scalar"},
                {"Name": "displacement_img", "Type": "mupif.PyroFile", "description": "", "Type_ID": "mupif.DataID.ID_Image", "Obj_ID": "displacement_img", "Units": "", "ValueType": "Scalar"}
            ],
            "Models": [
                {
                    "Name": "model_1",
                    "Jobmanager": "CVUT.Thermal_demo",
                    "Instantiate": True
                },
                {
                    "Name": "model_2",
                    "Jobmanager": "CVUT.Mechanical_demo",
                    "Instantiate": True
                }
            ]
        }
        super().__init__(metadata=MD)
        self.updateMetadata(metadata)
        self.daemon = None

        # initialization code of external input (ext_slot_1)
        self.external_input_1 = None
        # It should be defined from outside using set() method.

        # initialization code of external input (ext_slot_2)
        self.external_input_2 = None
        # It should be defined from outside using set() method.

        # initialization code of external input (ext_slot_1)
        self.external_input_3 = None
        # It should be defined from outside using set() method.

        # initialization code of external input (ext_slot_2)
        self.external_input_4 = None
        # It should be defined from outside using set() method.

        # __init__ code of constant_property_1 (Property)
        self.constant_property_1 = mupif.property.ConstantProperty(value=0., propID=mupif.DataID.ID_Displacement, valueType=mupif.ValueType.Scalar, unit=mupif.U.m, time=None)

    def initialize(self, workdir='', metadata=None, validateMetaData=True, **kwargs):
        super().initialize(workdir=workdir, metadata=metadata, validateMetaData=validateMetaData, **kwargs)

        ns = mupif.pyroutil.connectNameserver()
        self.daemon = mupif.pyroutil.getDaemon(ns)

        self.getModel('model_2').set(self.constant_property_1, 'Dirichlet left')

    # set method for all external inputs
    def set(self, obj, objectID=''):

        # in case of mupif.PyroFile
        if obj.isInstance(mupif.PyroFile):
            if objectID == 'tm_input_file':
                self.external_input_3 = obj
                self.getModel('model_1').set(self.external_input_3, 'input_file_thermal')
            if objectID == 'sm_input_file':
                self.external_input_4 = obj
                self.getModel('model_2').set(self.external_input_4, 'input_file_mechanical')

        # in case of mupif.Property
        if obj.isInstance(mupif.Property):
            if objectID == 'top_temperature':
                self.external_input_1 = obj
            if objectID == 'bottom_temperature':
                self.external_input_2 = obj

    # get method for all external outputs
    def get(self, objectTypeID, time=None, objectID=''):
        if objectID == 'temperature_field':
            return self.getModel('model_1').get(mupif.DataID.FID_Temperature, time, '')
        if objectID == 'displacement_field':
            return self.getModel('model_2').get(mupif.DataID.FID_Displacement, time, '')
        if objectID == 'temperature_vtk':
            return self.getModel('model_1').get(mupif.DataID.ID_VTKFile, time, '')
        if objectID == 'displacement_vtk':
            return self.getModel('model_2').get(mupif.DataID.ID_VTKFile, time, '')
        if objectID == 'temperature_img':
            return self.getModel('model_1').get(mupif.DataID.ID_Image, time, '')
        if objectID == 'displacement_img':
            return self.getModel('model_2').get(mupif.DataID.ID_Image, time, '')

        return None

    def solveStep(self, tstep, stageID=0, runInBackground=False):
        pass

        # execution code of model_1 (Stationary thermal problem)
        # self.getModel('model_1').set(self.external_input_1, 'top_edge')
        self.getModel('model_1').set(self.external_input_1, 'Dirichlet top')
        # self.getModel('model_1').set(self.external_input_2, 'bottom_edge')
        self.getModel('model_1').set(self.external_input_2, 'Dirichlet bottom')
        self.getModel('model_1').solveStep(tstep=tstep, runInBackground=False)

        # execution code of model_2 (Plane stress linear elastic)
        self.getModel('model_2').set(self.getModel('model_1').get(mupif.DataID.FID_Temperature, tstep.getTime(), ''), '')
        self.getModel('model_2').solveStep(tstep=tstep, runInBackground=False)


if __name__ == '__main__':
    w = MuPIFThermoMechanicalDemo()
    md = {
        'Execution': {
            'ID': '1',
            'Use_case_ID': '1_1',
            'Task_ID': '1'
        }
    }
    w.initialize(metadata=md)

    # w.set(
    #     mupif.ConstantProperty(value=100. * mupif.U.s, propID=mupif.DataID.PID_Time, valueType=mupif.ValueType.Scalar, unit=mupif.U.s),
    #     objectID='targetTime')

    # thermalInputFile = mupif.PyroFile(filename='.' + os.path.sep + 'tmin.in', mode="rb", dataID=mupif.DataID.ID_InputFile)
    # w.daemon.register(thermalInputFile)
    # w.set(thermalInputFile, 'input_file_thermal')
    #
    # mechanicalInputFile = mupif.PyroFile(filename='.' + os.path.sep + 'smin.in', mode="rb", dataID=mupif.DataID.ID_InputFile)
    # w.daemon.register(mechanicalInputFile)
    # w.set(mechanicalInputFile, 'input_file_mechanical')

    w.set(mupif.ConstantProperty(value=-100., propID=mupif.DataID.PID_Temperature, valueType=mupif.ValueType.Scalar, unit=mupif.U.deg_C), objectID='top_temperature')
    w.set(mupif.ConstantProperty(value=100., propID=mupif.DataID.PID_Temperature, valueType=mupif.ValueType.Scalar, unit=mupif.U.deg_C), objectID='bottom_temperature')

    w.solve()

    tf = w.get(mupif.DataID.FID_Temperature, 1*mupif.U.s, 'temperature_field')
    mf = w.get(mupif.DataID.FID_Displacement, 1*mupif.U.s, 'displacement_field')
    tff = w.get(mupif.DataID.ID_VTKFile, 1*mupif.U.s, 'temperature_vtk')
    mupif.PyroFile.copy(tff, d + os.path.sep + 'field_tm.vtk')
    mff = w.get(mupif.DataID.ID_VTKFile, 1*mupif.U.s, 'displacement_vtk')
    mupif.PyroFile.copy(mff, d + os.path.sep + 'field_sm.vtk')

    tfi = w.get(mupif.DataID.ID_Image, 1 * mupif.U.s, 'temperature_img')
    mupif.PyroFile.copy(tfi, d + os.path.sep + 'field_tm.png')
    mfi = w.get(mupif.DataID.ID_Image, 1 * mupif.U.s, 'displacement_img')
    mupif.PyroFile.copy(mfi, d + os.path.sep + 'field_sm.png')

    # tf.plot2D(fileName='ft.png')
    # mf.plot2D(fileName='fm.png')

    w.terminate()
