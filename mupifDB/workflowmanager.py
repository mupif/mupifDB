import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")
import tempfile
import re
from ast import literal_eval
from mupifDB import restApiControl

import mupif

import table_structures


def insertWorkflowDefinition(wid, description, source, useCase, workflowInputs, workflowOutputs, modulename, classname):
    """
    Inserts new workflow definition into DB. 
    Note there is workflow versioning schema: the current (latest) workflow version are stored in workflows collection.
    The old versions (with the same wid but different version) are stored in workflowsHistory.
    @param wid unique workflow id
    @param description Description
    @param source source URL
    @param useCase useCase ID the workflow belongs to
    @param workflowInputs workflow input metadata (list of dicts)
    @param workflowOutputs workflow output metadata (list of dicts)
    @param modulename
    @param classname
    """
    # prepare document to insert
    # save workflow source to gridfs
    # to allow for more general case, workflow should be tar.gz 
    # archive of all workflow inplamentation files
    sourceID = None

    with open(source, 'rb') as f:
        sourceID = restApiControl.uploadBinaryFileContent(f)
        f.close()

    rec = {'wid': wid, 'Description': description, 'GridFSID': sourceID, 'UseCase': useCase, 'IOCard': None, 'modulename': modulename, 'classname': classname}
    Inputs = []
    for i in workflowInputs:
        irec = {'Name': i['Name'], 'Description': i.get('Description', None), 'Type': i['Type'], 'TypeID': i['Type_ID'], 'ValueType': i['ValueType'], 'Units': i['Units'], 'ObjID': i.get('Obj_ID', ""), 'Compulsory': i['Required']}
        Inputs.append(irec)
    Outputs = []
    for i in workflowOutputs:
        irec = {'Name': i['Name'], 'Description': i.get('Description', None), 'Type': i['Type'], 'TypeID': i['Type_ID'], 'ValueType': i['ValueType'], 'Units': i['Units'], 'ObjID': i.get('Obj_ID', "")}
        Outputs.append(irec)
    rec['IOCard'] = {'Inputs': Inputs, 'Outputs': Outputs}
    
    # first check if workflow with wid already exist in workflows
    w_rec = restApiControl.getWorkflowRecord(wid)
    if w_rec is None:  # can safely create a new record in workflows collection
        version = 1
        rec['Version'] = version
        new_id = restApiControl.insertWorkflow(rec)
        return new_id

    else:
        # the workflow already exists, need to make a new version
        # clone latest version to History
        # print(w_rec)
        w_rec.pop('_id')  # remove original document id
        restApiControl.insertWorkflowHistory(w_rec)
        # update the latest document
        version = (1+int(w_rec.get('Version', 1)))
        rec['Version'] = version
        res_id = restApiControl.updateWorkflow(rec)
        if res_id:
            return res_id
        else:
            print("Update failed")
    return None


def getWorkflowDoc(wid, version=-1):
    """ 
        Returns workflow document with given wid and version
        @param version workflow version, version == -1 means return the most recent version    
    """
    wdoclatest = restApiControl.getWorkflowRecord(wid)
    if wdoclatest is None:
        raise KeyError("Workflow document with WID" + wid + " not found")
    lastversion = int(wdoclatest['Version'])
    if version == -1 or version == lastversion:  # get the latest one
        return wdoclatest
    elif version < lastversion:
        wdoc = restApiControl.getWorkflowRecordFromHistory(wid, version)
        if wdoc is None:
            raise KeyError("Workflow document with WID" + wid + "Version" + str(version) + " not found")
        return wdoc
    else:
        raise KeyError("Workflow document with WID" + wid + "Version" + str(version) + ": bad version")


class WorkflowExecutionIODataSet:
    def __init__(self, wec, IOid, weid):
        self.wec = wec
        self.IOid = IOid
        self.weid = weid
    
    @staticmethod
    def create(workflowID, type, workflowVer=-1):
        rec = {'Type': type, 'DataSet': []}

        wdoc = getWorkflowDoc(workflowID, version=workflowVer)
        if wdoc is None:
            raise KeyError("Workflow document with ID" + workflowID + " not found")

        IOCard = wdoc['IOCard']
        rec = {}
        data = []
        for io in IOCard[type]:  # loop over workflow inputs/outputs
            if isinstance(io.get('ObjID', ""), list) or isinstance(io.get('ObjID', ""), tuple):
                for objid in io['ObjID']:
                    # make separate input entry for each obj_id
                    data.append({'Name': io['Name'], 'Type': io['Type'], 'Value': None, 'ValueType': io['ValueType'], 'TypeID': io['TypeID'], 'Units': io['Units'], 'ObjID': objid, 'Compulsory': io.get('Compulsory', None), 'Source': None, 'OriginId': None, 'FileID': None, 'Link': {'ExecID': "", 'Name': "", 'ObjID': ""}})
            else:  # single obj_id provided
                data.append({'Name': io['Name'], 'Type': io['Type'], 'Value': None, 'ValueType': io['ValueType'], 'TypeID': io['TypeID'], 'Units': io['Units'], 'ObjID': io.get('ObjID', ""), 'Compulsory': io.get('Compulsory', None), 'Source': None, 'OriginId': None, 'FileID': None, 'Link': {'ExecID': "", 'Name': "", 'ObjID': ""}})
        rec['Type'] = type
        rec['DataSet'] = data
        rec_id = restApiControl.insertIODataRecord(rec)
        return rec_id

    def _getDocument(self):
        """
        Returns workflowExection document corresponding to self.executionID
        """
        iod_record = restApiControl.getIODataRecord(self.IOid)
        if iod_record is None:
            raise KeyError("Document with ID" + self.IOid + " not found")
        return iod_record

    def getRec(self, name, obj_id=""):
        """
        Returns the input record identified by name
        @param: input name
        @return: associated record
        @throws: KeyError if input parameter name not found
        """
        doc = self._getDocument()
    
        for rec in doc['DataSet']:
            if (rec['Name'] == name) and (rec['ObjID'] == obj_id):
                return rec
        raise KeyError("Input parameter " + name + " Obj_ID " + str(obj_id) + " not found")
         
    def get(self, name, obj_id=""):
        """
        Returns the value of input parameter identified by name
        @param: input name
        @return: associated value
        @throws: KeyError if input parameter name not found
        """
        return self.getRec(name, obj_id)['Value']
    
    def set(self, name, value, obj_id=""):
        """
        Sets the value of output parameter identified by name to given value
        @param: name parameter name
        @param: value associated value
        @throws: KeyError if input parameter name not found
        """
        if self.wec.getStatus() == 'Created':
            restApiControl.setExecutionInputValue(self.weid, name, value, obj_id)
        else:
            raise KeyError("Inputs cannot be changed as workflow execution status is not Created")

    def setOutputVal(self, name, value, obj_id=""):
        """
        Sets the value of output parameter attributes identified by name to given value
        @param: name parameter name
        @param: value value to set
        @param: value associated value
        """
        restApiControl.setExecutionOutputValue(self.weid, name, value, obj_id)

    def setOutputFileID(self, name, fileID, obj_id=""):
        """
        Sets the value of output parameter attributes identified by name to given value
        @param: name parameter name
        @param: value value to set
        @param: value associated value
        """
        restApiControl.setExecutionOutputFileID(self.weid, name, fileID, obj_id)


class WorkflowExecutionContext:

    def __init__(self, executionID, **kwargs):
        self.executionID = executionID

    @staticmethod
    def create(workflowID, requestedBy='', workflowVer=-1, ip=''):
        """
        """
        # first query for workflow document
        wdoc = getWorkflowDoc(workflowID, version=workflowVer)
        if wdoc is not None:
            # IOCard = wdoc['IOCard']
            rec = table_structures.tableExecution.copy()
            rec['WorkflowID'] = workflowID
            rec['WorkflowVersion'] = wdoc.get('Version', 1)
            rec['RequestedBy'] = requestedBy
            rec['UserIP'] = ip
            rec['CreatedDate'] = str(datetime.datetime.now())
            rec['Inputs'] = WorkflowExecutionIODataSet.create(workflowID, 'Inputs')
            rec['Outputs'] = WorkflowExecutionIODataSet.create(workflowID, 'Outputs')
            new_id = restApiControl.insertExecutionRecord(rec)
            return WorkflowExecutionContext(new_id)

        else:
            raise KeyError("Workflow record " + workflowID + ", Version " + str(workflowVer) + " not found")

    def _getWorkflowExecutionDocument(self):
        """
        Returns workflowExection document corresponding to self.executionID
        """
        we_rec = restApiControl.getExecutionRecord(self.executionID)
        if we_rec is None:
            raise KeyError("Record with id=" + self.executionID + " not found")
        return we_rec

    def _getWorkflowDocument(self):
        """
        Returns workflow document corresponding to self.executionID
        """
        doc = self._getWorkflowExecutionDocument()
        wid = doc['WorkflowID']
        version = doc['WorkflowVersion']
        wdoc = getWorkflowDoc(wid, version=version)
        if wdoc is None:
            raise KeyError("Workflow document with ID" + str(wid) + " not found")
        return wdoc

    def set(self, name, value):
        """
        Updates workflow execution attribute identified by name
        """
        doc = self._getWorkflowExecutionDocument()
        if name in doc:
            restApiControl.setExecutionParameter(self.executionID, name, value)

    def get(self, name):
        """
        Returns workflow execution attribute identified by name
        """
        doc = self._getWorkflowExecutionDocument()
        return doc[name]

    def getIODataDoc(self, type='Inputs'):
        doc = self._getWorkflowExecutionDocument()    
        return WorkflowExecutionIODataSet(self, self.get(type), self.executionID)

    def getStatus(self):
        wed = self._getWorkflowExecutionDocument()
        return wed['Status']


def ObjIDIsIterable(val):
    try:
        a = val[0]
        if not isinstance(val, str):
            return True
    except:
        return False


def checkInputs(eid):
    execution = restApiControl.getExecutionRecord(eid)
    workflow = restApiControl.getWorkflowRecordGeneral(execution['WorkflowID'], execution['WorkflowVersion'])
    workflow_input_templates = workflow['IOCard']['Inputs']
    execution_inputs = restApiControl.getIODataRecord(execution['Inputs'])['DataSet']

    for input_template in workflow_input_templates:
        name = input_template['Name']
        object_type = input_template['Type']
        valueType = input_template['ValueType']
        typeID = input_template['TypeID']
        # try to get raw PID from typeID
        match = re.search('\w+\Z', typeID)
        if match:
            typeID = match.group()

        objID = input_template.get('ObjID', "")
        compulsory = input_template['Compulsory']

        if not ObjIDIsIterable(objID):
            objID = [objID]

        for oid in objID:
            if compulsory:
                if object_type == 'mupif.Property':

                    part_input_check = False

                    inp_record = None
                    for exec_inp in execution_inputs:
                        if exec_inp['Name'] == name and exec_inp['ObjID'] == oid:
                            inp_record = exec_inp
                            break

                    if inp_record is not None:
                        if inp_record['Link']['ExecID'] != "" and inp_record['Link']['Name'] != "":
                            # Link -> check if it finds an existing record first
                            m_execution = restApiControl.getExecutionRecord(inp_record['Link']['ExecID'])
                            m_execution_outputs = restApiControl.getIODataRecord(m_execution['Outputs'])['DataSet']

                            m_inp_record = None
                            for exec_out in m_execution_outputs:
                                if exec_out['Name'] == inp_record['Link']['Name'] and exec_out['ObjID'] == inp_record['Link']['ObjID']:
                                    m_inp_record = exec_out
                                    break

                            if m_inp_record is not None:
                                # now check the value
                                if m_inp_record['FileID'] is None:
                                    if literal_eval(m_inp_record['Value']) is not None:
                                        part_input_check = True
                                    print('A')
                                else:
                                    pfile = restApiControl.getBinaryFileContentByID(m_inp_record['FileID'])
                                    with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
                                        full_path = tempDir + "/file.h5"
                                        f = open(full_path, 'wb')
                                        f.write(pfile)
                                        f.close()
                                        prop = mupif.ConstantProperty.loadHdf5(full_path)
                                        if prop.getValue() is not None:
                                            part_input_check = True
                                    print('B')

                        else:
                            # check only not None value
                            if inp_record['Value'] != 'None' and inp_record['Value'] != '' and inp_record['Value'] is not None:
                                part_input_check = True

                    if part_input_check is False:
                        return False
                else:
                    raise ValueError('Handling of io param of type %s not implemented' % object_type)

    return True


def mapInputs(app, eid):
    execution = restApiControl.getExecutionRecord(eid)
    workflow = restApiControl.getWorkflowRecordGeneral(execution['WorkflowID'], execution['WorkflowVersion'])
    workflow_input_templates = workflow['IOCard']['Inputs']
    execution_inputs = restApiControl.getIODataRecord(execution['Inputs'])['DataSet']

    for input_template in workflow_input_templates:
        name = input_template['Name']
        object_type = input_template['Type']
        valueType = input_template['ValueType']
        typeID = input_template['TypeID']
        # try to get raw PID from typeID
        match = re.search('\w+\Z', typeID)
        if match:
            typeID = match.group()
        
        objID = input_template.get('ObjID', "")
        compulsory = input_template['Compulsory']
        units = input_template['Units']
        if units == 'None':
            units = mupif.U.none

        vts = {
            "Scalar": mupif.ValueType.Scalar,
            "Vector": mupif.ValueType.Vector,
            "Tensor": mupif.ValueType.Tensor,
            "ScalarArray": mupif.ValueType.ScalarArray,
            "VectorArray": mupif.ValueType.VectorArray,
            "TensorArray": mupif.ValueType.TensorArray
        }

        if not ObjIDIsIterable(objID):
            objID = [objID]

        vt = vts[valueType]
        for oid in objID:
            if object_type == 'mupif.Property':
                inp_record = None
                for exec_inp in execution_inputs:
                    if exec_inp['Name'] == name and exec_inp['ObjID'] == oid:
                        inp_record = exec_inp
                        break

                if inp_record is not None:
                    if inp_record['Link']['ExecID'] != "" and inp_record['Link']['Name'] != "":
                        # map linked value
                        m_execution = restApiControl.getExecutionRecord(inp_record['Link']['ExecID'])
                        m_execution_outputs = restApiControl.getIODataRecord(m_execution['Outputs'])['DataSet']

                        m_inp_record = None
                        for exec_out in m_execution_outputs:
                            if exec_out['Name'] == inp_record['Link']['Name'] and exec_out['ObjID'] == inp_record['Link']['ObjID']:
                                m_inp_record = exec_out
                                break

                        if m_inp_record is not None:
                            if m_inp_record['FileID'] is None:
                                prop = mupif.ConstantProperty(value=literal_eval(m_inp_record['Value']), propID=mupif.DataID[typeID], valueType=vt, unit=units)
                                app.set(prop, oid)
                            else:
                                pfile = restApiControl.getBinaryFileContentByID(m_inp_record['FileID'])
                                with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
                                    full_path = tempDir + "/file.h5"
                                    f = open(full_path, 'wb')
                                    f.write(pfile)
                                    f.close()
                                    prop = mupif.ConstantProperty.loadHdf5(full_path)
                                    app.set(prop, oid)

                    else:
                        # map classic value stored in string
                        if literal_eval(inp_record['Value']) is not None:
                            prop = mupif.ConstantProperty(value=literal_eval(inp_record['Value']), propID=mupif.DataID[typeID], valueType=vt, unit=units)
                            app.set(prop, oid)
            else:
                raise ValueError('Handling of io param of type %s not implemented' % object_type)


def mapOutputs(app, eid, time):
    print("mapOutputs()")

    execution = restApiControl.getExecutionRecord(eid)
    workflow = restApiControl.getWorkflowRecordGeneral(execution['WorkflowID'], execution['WorkflowVersion'])
    workflow_output_templates = workflow['IOCard']['Outputs']

    for output_template in workflow_output_templates:
        name = output_template['Name']
        object_type = output_template['Type']
        units = output_template['Units']
        typeID = output_template['TypeID']
        # try to get raw PID from typeID
        match = re.search('\w+\Z', typeID)
        if match:
            typeID = match.group()
        
        objID = output_template.get('ObjID', "")

        if not ObjIDIsIterable(objID):
            objID = [objID]

        for oid in objID:
            if object_type == 'mupif.Property':
                print("Requesting %s, objID %s, time %s" % (mupif.DataID[typeID], oid, time), flush=True)
                prop = app.get(mupif.DataID[typeID], time, oid)
                if prop.valueType == mupif.ValueType.Scalar:
                    val = prop.inUnitsOf(units).getValue()
                    val = str(val)
                    restApiControl.setExecutionOutputValue(eid, name, val, oid)
                elif prop.valueType in [mupif.ValueType.Vector, mupif.ValueType.Tensor]:
                    # filling the string Value
                    val = prop.inUnitsOf(units).getValue()
                    val = str(tuple(val))
                    restApiControl.setExecutionOutputValue(eid, name, val, oid)
                elif prop.valueType in [mupif.ValueType.ScalarArray, mupif.ValueType.VectorArray, mupif.ValueType.TensorArray]:
                    with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
                        full_path = tempDir + "/file.h5"
                        prop.saveHdf5(full_path)
                        fileID = None
                        with open(full_path, 'rb') as f:
                            fileID = restApiControl.uploadBinaryFileContent(f)
                            f.close()
                        if fileID is not None:
                            restApiControl.setExecutionOutputFileID(eid, name, fileID, oid)
                        else:
                            print("hdf5 file was not saved")

            else:
                raise ValueError('Handling of io param of type %s not implemented' % object_type)
