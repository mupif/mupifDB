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

api_type = os.environ.get('MUPIFDB_REST_SERVER_TYPE', "mupif")

ns = mupif.pyroutil.connectNameServer()
daemon = mupif.pyroutil.getDaemon(ns)


def insertWorkflowDefinition(wid, description, source, useCase, workflowInputs, workflowOutputs, modulename, classname, models_md):
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
        sourceID = restApiControl.uploadBinaryFile(f)
        f.close()

    rec = {'wid': wid, 'Description': description, 'GridFSID': sourceID, 'UseCase': useCase, 'IOCard': None, 'modulename': modulename, 'classname': classname, 'Models': models_md}
    Inputs = []
    for i in workflowInputs:
        irec = {'Name': i['Name'], 'Description': i.get('Description', None), 'Type': i['Type'], 'TypeID': i['Type_ID'], 'ValueType': i.get('ValueType', ''), 'Units': i.get('Units', ''), 'ObjID': i.get('Obj_ID', ''), 'Compulsory': i['Required'], 'Set_at': i['Set_at']}
        Inputs.append(irec)
    Outputs = []
    for i in workflowOutputs:
        irec = {'Name': i['Name'], 'Description': i.get('Description', None), 'Type': i['Type'], 'TypeID': i['Type_ID'], 'ValueType': i.get('ValueType', ''), 'Units': i.get('Units', ''), 'ObjID': i.get('Obj_ID', '')}
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
        res_id = restApiControl.updateWorkflow(rec)['_id']
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
                    data.append({'Name': io['Name'], 'Type': io['Type'], 'Object': {}, 'Value': None, 'ValueType': io['ValueType'], 'TypeID': io['TypeID'], 'Units': io['Units'], 'ObjID': objid, 'Compulsory': io.get('Compulsory', None), 'FileID': None, 'Link': {'ExecID': "", 'Name': "", 'ObjID': ""}})
            else:  # single obj_id provided
                data.append({'Name': io['Name'], 'Type': io['Type'], 'Object': {}, 'Value': None, 'ValueType': io['ValueType'], 'TypeID': io['TypeID'], 'Units': io['Units'], 'ObjID': io.get('ObjID', ""), 'Compulsory': io.get('Compulsory', None), 'FileID': None, 'Link': {'ExecID': "", 'Name': "", 'ObjID': ""}})
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


def checkInput(eid, name, obj_id, object_type, data_id, linked_output=False):
    if linked_output:
        inp_record = restApiControl.getExecutionOutputRecordItem(eid, name, obj_id)
    else:
        inp_record = restApiControl.getExecutionInputRecordItem(eid, name, obj_id)
    if inp_record is not None:
        link_eid = inp_record['Link'].get('ExecID', '')
        link_name = inp_record['Link'].get('Name', '')
        link_oid = inp_record['Link'].get('ObjID', '')
        if link_eid != "" and link_name != "":
            # check linked value
            return checkInput(
                eid=link_eid,
                name=link_name,
                obj_id=link_oid,
                object_type=object_type,
                data_id=data_id,
                linked_output=True
            )

        else:
            # check value from database record
            if object_type == 'mupif.Property':
                file_id = inp_record['Object'].get('FileID', None)
                if file_id is None:
                    # property from dict
                    try:
                        prop = mupif.ConstantProperty.from_db_dict(inp_record['Object'])
                        return True
                    except:
                        return False
                else:
                    # property from hdf5 file
                    pfile, fn = restApiControl.getBinaryFileByID(file_id)
                    with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
                        full_path = tempDir + "/file.h5"
                        f = open(full_path, 'wb')
                        f.write(pfile)
                        f.close()
                        try:
                            prop = mupif.ConstantProperty.loadHdf5(full_path)
                            return True
                        except:
                            return False

            elif object_type == 'mupif.String':
                # property from dict
                try:
                    prop = mupif.String.from_db_dict(inp_record['Object'])
                    return True
                except:
                    return False

            elif object_type == 'mupif.HeavyStruct':
                file_id = inp_record['Object'].get('FileID', None)
                if file_id is not None:
                    # load from hdf5 file
                    pfile, fn = restApiControl.getBinaryFileByID(file_id)
                    with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
                        full_path = tempDir + "/file.h5"
                        f = open(full_path, 'wb')
                        f.write(pfile)
                        f.close()
                        try:
                            hs = mupif.HeavyStruct(h5path=full_path, mode='readonly', id=mupif.DataID[data_id])
                            return True
                        except:
                            return False
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
        data_id = input_template['TypeID']
        # try to get raw PID from typeID
        match = re.search('\w+\Z', data_id)
        if match:
            data_id = match.group()

        objID = input_template.get('ObjID', "")
        compulsory = input_template['Compulsory']
        if compulsory:
            if not ObjIDIsIterable(objID):
                objID = [objID]

            for oid in objID:
                if checkInput(
                    eid=eid,
                    name=name,
                    obj_id=oid,
                    data_id=data_id,
                    object_type=object_type
                ) is False:
                    return False

    return True


def mapInput(app, eid, name, obj_id, app_obj_id, object_type, data_id, linked_output=False):
    if linked_output:
        inp_record = restApiControl.getExecutionOutputRecordItem(eid, name, obj_id)
    else:
        inp_record = restApiControl.getExecutionInputRecordItem(eid, name, obj_id)

    if api_type == 'granta':
        inp_record = restApiControl._getGrantaExecutionInputItem(eid, name)

    if inp_record is not None:
        link_eid = inp_record['Link'].get('ExecID', '')
        link_name = inp_record['Link'].get('Name', '')
        link_oid = inp_record['Link'].get('ObjID', '')
        if link_eid != "" and link_name != "":
            # map linked value
            mapInput(
                app=app,
                eid=link_eid,
                name=link_name,
                obj_id=link_oid,
                app_obj_id=app_obj_id,
                object_type=object_type,
                data_id=data_id,
                linked_output=True
            )

        else:
            # map value from database record
            if object_type == 'mupif.Property':
                file_id = inp_record['Object'].get('FileID', None)
                if file_id is None:
                    # property from dict
                    prop = mupif.ConstantProperty.from_db_dict(inp_record['Object'])
                    app.set(prop, app_obj_id)
                else:
                    # property from hdf5 file
                    pfile, fn = restApiControl.getBinaryFileByID(file_id)
                    with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
                        full_path = tempDir + "/file.h5"
                        f = open(full_path, 'wb')
                        f.write(pfile)
                        f.close()
                        prop = mupif.ConstantProperty.loadHdf5(full_path)
                        app.set(prop, app_obj_id)

            elif object_type == 'mupif.String':
                # property from dict
                prop = mupif.String.from_db_dict(inp_record['Object'])
                app.set(prop, app_obj_id)

            elif object_type == 'mupif.PyroFile':
                file_id = inp_record['Object'].get('FileID', None)
                if file_id is not None:
                    # load from hdf5 file
                    pfile, fn = restApiControl.getBinaryFileByID(file_id)
                    with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
                        # full_path = "/home/stanislav/tmp/" + fn  # tempDir
                        full_path = tempDir + "/" + fn
                        f = open(full_path, 'wb')
                        f.write(pfile)
                        f.close()
                        pf = mupif.PyroFile(filename=full_path, mode='rb')
                        daemon.register(pf)
                        app.set(pf, app_obj_id)

            elif object_type == 'mupif.HeavyStruct':
                file_id = inp_record['Object'].get('FileID', None)
                if file_id is not None:
                    # load from hdf5 file
                    pfile, fn = restApiControl.getBinaryFileByID(file_id)
                    with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
                        full_path = tempDir + "/file.h5"
                        f = open(full_path, 'wb')
                        f.write(pfile)
                        f.close()
                        hs = mupif.HeavyStruct(h5path=full_path, mode='copy-readwrite', id=mupif.DataID[data_id])
                        # hs = hs.deepcopy()
                        daemon.register(hs)
                        hs.exposeData()
                        app.set(hs, app_obj_id)
            else:
                raise ValueError('Handling of io param of type %s is not implemented' % object_type)


def mapInputs(app, eid):
    execution = restApiControl.getExecutionRecord(eid)
    workflow = restApiControl.getWorkflowRecordGeneral(execution['WorkflowID'], execution['WorkflowVersion'])
    workflow_input_templates = workflow['IOCard']['Inputs']
    if api_type == 'granta':
        workflow_input_templates = restApiControl._getGrantaWorkflowMetadataFromDatabase(execution['WorkflowID']).get('Inputs', [])

    for input_template in workflow_input_templates:
        name = input_template['Name']
        object_type = input_template.get('Type', '')
        data_id = input_template.get('TypeID', input_template.get('Type_ID'))
        # try to get raw PID from typeID
        match = re.search('\w+\Z', data_id)
        if match:
            data_id = match.group()

        objID = input_template.get('ObjID', input_template.get('Obj_ID', ''))
        if not ObjIDIsIterable(objID):
            objID = [objID]

        for oid in objID:
            mapInput(
                app=app,
                eid=eid,
                name=name,
                obj_id=oid,
                app_obj_id=oid,
                data_id=data_id,
                object_type=object_type
            )


def mapOutput(app, eid, name, obj_id, data_id, time, object_type):
    if object_type == 'mupif.Property':
        prop = app.get(mupif.DataID[data_id], time, obj_id)

        if prop.valueType in [mupif.ValueType.Scalar, mupif.ValueType.Vector, mupif.ValueType.Tensor]:
            restApiControl.setExecutionOutputObject(eid, name, obj_id, prop.to_db_dict())
        elif prop.valueType in [mupif.ValueType.ScalarArray, mupif.ValueType.VectorArray, mupif.ValueType.TensorArray]:
            with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
                full_path = tempDir + "/file.h5"
                prop.saveHdf5(full_path)
                fileID = None
                with open(full_path, 'rb') as f:
                    fileID = restApiControl.uploadBinaryFile(f)
                    f.close()
                if fileID is not None:
                    restApiControl.setExecutionOutputObject(eid, name, obj_id, {'FileID': fileID})
                else:
                    print("hdf5 file was not saved")

    elif object_type == 'mupif.String':
        prop = app.get(mupif.DataID[data_id], time, obj_id)
        restApiControl.setExecutionOutputObject(eid, name, obj_id, prop.to_db_dict())

    elif object_type == 'mupif.HeavyStruct':
        hs = app.get(mupif.DataID[data_id], time, obj_id)

        with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
            full_path = tempDir + "/file.h5"
            hs_copy = hs.deepcopy()
            hs.moveStorage(full_path)
            fileID = None
            with open(full_path, 'rb') as f:
                fileID = restApiControl.uploadBinaryFile(f)
                f.close()
            if fileID is not None:
                restApiControl.setExecutionOutputObject(eid, name, obj_id, {'FileID': fileID})
            else:
                print("hdf5 file was not saved")

    else:
        raise ValueError('Handling of io param of type %s not implemented' % object_type)


def _getGrantaOutput(app, eid, name, obj_id, data_id, time, object_type):
    if object_type == 'mupif.Property':
        prop = app.get(mupif.DataID[data_id], time, obj_id)
        if prop.valueType == mupif.ValueType.Scalar:
            return {
                "name": str(name),
                "value": prop.quantity.value.tolist(),
                "type": "float"
            }
    if object_type == 'mupif.HeavyStruct':
        hs = app.get(mupif.DataID[data_id], time, obj_id)

        with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
            full_path = tempDir + "/file.h5"
            hs_copy = hs.deepcopy()
            hs.moveStorage(full_path)
            fileID = None
            with open(full_path, 'rb') as f:
                fileID = restApiControl.uploadBinaryFile(f)
                f.close()
            if fileID is None:
                print("hdf5 file was not saved")
            return {
                "name": str(name),
                "value": "https://musicode.grantami.com/musicode/filestore/%s" % str(fileID),
                "type": "hyperlink"
            }

    return None


def mapOutputs(app, eid, time):
    execution = restApiControl.getExecutionRecord(eid)
    workflow = restApiControl.getWorkflowRecordGeneral(execution['WorkflowID'], execution['WorkflowVersion'])
    workflow_output_templates = workflow['IOCard']['Outputs']
    if api_type == 'granta':
        workflow_output_templates = restApiControl._getGrantaWorkflowMetadataFromDatabase(execution['WorkflowID']).get('Outputs', [])

    granta_output_data = []

    for output_template in workflow_output_templates:
        name = output_template['Name']
        object_type = output_template['Type']
        typeID = output_template.get('TypeID', output_template.get('Type_ID'))
        # try to get raw PID from typeID
        match = re.search('\w+\Z', typeID)
        if match:
            typeID = match.group()

        objID = output_template.get('ObjID', output_template.get('Obj_ID', ''))
        if not ObjIDIsIterable(objID):
            objID = [objID]

        for oid in objID:
            if api_type == 'granta':
                output = _getGrantaOutput(
                    app=app,
                    eid=eid,
                    name=name,
                    obj_id=oid,
                    data_id=typeID,
                    time=time,
                    object_type=object_type
                )
                if output is not None:
                    granta_output_data.append(output)
            else:
                mapOutput(
                    app=app,
                    eid=eid,
                    name=name,
                    obj_id=oid,
                    data_id=typeID,
                    time=time,
                    object_type=object_type
                )

    if api_type == 'granta':
        restApiControl._setGrantaExecutionResults(eid, granta_output_data)

