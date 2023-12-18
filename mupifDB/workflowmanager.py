import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")
import tempfile
import re
from ast import literal_eval
from mupifDB import restApiControl
import Pyro5
import mupif

import table_structures

api_type = os.environ.get('MUPIFDB_REST_SERVER_TYPE', "mupif")

daemon = None
def getDaemon():
    global daemon
    if daemon is None:
        ns = mupif.pyroutil.connectNameServer()
        daemon = mupif.pyroutil.getDaemon(ns)
    return daemon


def insertWorkflowDefinition(wid, description, source, useCase, workflowInputs, workflowOutputs, modulename, classname, models_md, EDM_Mapping=None):
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
        if i.get('EDMPath', None) is not None:
            irec['EDMPath'] = i.get('EDMPath')
        if i.get('EDMList', None) is not None:
            irec['EDMList'] = i.get('EDMList')
        Inputs.append(irec)
    Outputs = []
    for i in workflowOutputs:
        irec = {'Name': i['Name'], 'Description': i.get('Description', None), 'Type': i['Type'], 'TypeID': i['Type_ID'], 'ValueType': i.get('ValueType', ''), 'Units': i.get('Units', ''), 'ObjID': i.get('Obj_ID', '')}
        if i.get('EDMPath', None) is not None:
            irec['EDMPath'] = i.get('EDMPath')
        if i.get('EDMList', None) is not None:
            irec['EDMList'] = i.get('EDMList')
        Outputs.append(irec)
    rec['IOCard'] = {'Inputs': Inputs, 'Outputs': Outputs}

    rec['EDMMapping'] = []
    if EDM_Mapping is not None:
        rec['EDMMapping'] = EDM_Mapping

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
    def create(workflowID, type, workflowVer=-1, no_onto=False):
        wdoc = getWorkflowDoc(workflowID, version=workflowVer)
        if wdoc is None:
            raise KeyError("Workflow document with ID" + workflowID + " not found")

        IOCard = wdoc['IOCard']
        rec = {}
        data = []
        for io in IOCard[type]:  # loop over workflow inputs/outputs
            OID = [""]
            if io.get('ObjID', None) is not None:
                if isinstance(io.get('ObjID'), str):
                    OID = [io.get('ObjID')]
                if isinstance(io.get('ObjID'), list) or isinstance(io.get('ObjID'), tuple):
                    OID = io.get('ObjID')

            for objid in OID:
                data.append({
                    'Name': io['Name'],
                    'Type': io['Type'],
                    'Object': {},
                    'Value': None,
                    'ValueType': io['ValueType'],
                    'TypeID': io['TypeID'],
                    'Units': io['Units'],
                    'ObjID': objid,
                    'EDMPath': None if no_onto else io.get('EDMPath', None),
                    'EDMList': False if no_onto else io.get('EDMList', False),
                    'Compulsory': io.get('Compulsory', None),
                    'FileID': None,
                    'Link': {'ExecID': "", 'Name': "", 'ObjID': ""}
                })
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
    
    # def set(self, name, value, obj_id=""):
    #     """
    #     Sets the value of output parameter identified by name to given value
    #     @param: name parameter name
    #     @param: value associated value
    #     @throws: KeyError if input parameter name not found
    #     """
    #     if self.wec.getStatus() == 'Created':
    #         restApiControl.setExecutionInputValue(self.weid, name, value, obj_id)
    #     else:
    #         raise KeyError("Inputs cannot be changed as workflow execution status is not Created")


class WorkflowExecutionContext:

    def __init__(self, executionID, **kwargs):
        self.executionID = executionID

    @staticmethod
    def create(workflowID, requestedBy='', workflowVer=-1, ip='', no_onto=False):
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
            rec['Inputs'] = WorkflowExecutionIODataSet.create(workflowID, 'Inputs', workflowVer=workflowVer, no_onto=no_onto)
            rec['Outputs'] = WorkflowExecutionIODataSet.create(workflowID, 'Outputs', workflowVer=workflowVer, no_onto=no_onto)
            if no_onto:
                rec['EDMMapping'] = []
            else:
                OBO = []
                for obo in wdoc.get('EDMMapping', []):
                    obo['id'] = None
                    obo['ids'] = None
                    OBO.append(obo)
                rec['EDMMapping'] = OBO
            new_id = restApiControl.insertExecution(rec)
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


def checkInput(eid, name, obj_id, object_type, data_id, linked_output=False, onto_path=None, onto_base_objects=None):
    if linked_output:
        inp_record = restApiControl.getExecutionOutputRecordItem(eid, name, obj_id)
    else:
        inp_record = restApiControl.getExecutionInputRecordItem(eid, name, obj_id)

    if onto_path is not None:
        splitted = onto_path.split('.', 1)
        base_object_name = splitted[0]
        object_path = splitted[1]

        # find base object info
        info = {}
        for i in onto_base_objects:
            if i['Name'] == base_object_name:
                info = i

        # get the desired object
        edm_data = restApiControl.getEDMData(info.get('DBName', ''), info.get('EDMEntity', ''), info.get('id', ''), object_path)
        if edm_data is not None:
            if object_type == 'mupif.Property':
                if edm_data.get('value', None) is not None:
                    return True
            if object_type == 'mupif.String':
                if edm_data.get('value', None) is not None:
                    return True
        return False

    else:
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

    for input_template in execution_inputs:
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
                    object_type=object_type,
                    onto_path=input_template.get('EDMPath', None),
                    onto_base_objects=execution.get('EDMMapping', []),
                ) is False:
                    return False

    return True


# ##################################################
# Instantiate data from EDM

def getEDMPropertyInstance(edm_data, data_id, value_type):
    obj = None
    if value_type in ['Scalar', 'Vector', 'Tensor']:
        value = edm_data.get('value', None)
        unit = edm_data.get('unit', '')
        obj = mupif.ConstantProperty.from_db_dict({
            "Value": value,
            "DataID": data_id,
            "ValueType": value_type,
            "Unit": unit,
            "Time": None
        })
    elif  value_type in ['ScalarArray', 'VectorArray', 'TensorArray']:
        raise ValueError('Handling of Onto input Property of ValueType %s is not implemented' % value_type)
    return obj


def getEDMStringInstance(edm_data, data_id, value_type):
    value = edm_data.get('value', None)
    unit = edm_data.get('unit', '')
    obj = mupif.String.from_db_dict({
        "Value": value,
        "DataID": data_id,
        "ValueType": value_type
    })
    return obj


def getEDMTemporalPropertyInstance(edm_data, data_id, value_type):
    obj = mupif.DbDictable.from_db_dict(edm_data, dialect='edm')
    return obj


def mapInput(app, eid, name, obj_id, app_obj_id, object_type, data_id, linked_output=False, onto_path=None, onto_base_objects={}, value_type=None, edm_list=False):
    if linked_output:
        inp_record = restApiControl.getExecutionOutputRecordItem(eid, name, obj_id)
    else:
        inp_record = restApiControl.getExecutionInputRecordItem(eid, name, obj_id)

    if api_type == 'granta':
        inp_record = restApiControl._getGrantaExecutionInputItem(eid, name)

    op = inp_record.get('EDMPath', None)
    if op is not None:
        # onto_path is used
        splitted = op.split('.', 1)
        base_object_name = splitted[0]
        object_path = splitted[1]

        # find base object info
        info = {}
        for i in onto_base_objects:
            if i['Name'] == base_object_name:
                info = i

        edm_dbname = info.get('DBName', '')
        edm_entity = info.get('EDMEntity', '')
        edm_id = info.get('id', '')
        edm_ids = info.get('ids', [])

        if object_type == 'mupif.Property':
            edm_data = restApiControl.getEDMData(edm_dbname, edm_entity, edm_id, object_path)
            obj = getEDMPropertyInstance(edm_data=edm_data, data_id=data_id, value_type=value_type)
            app.set(obj, app_obj_id)

        elif object_type == 'mupif.String':
            edm_data = restApiControl.getEDMData(edm_dbname, edm_entity, edm_id, object_path)
            obj = getEDMStringInstance(edm_data=edm_data, data_id=data_id, value_type=value_type)
            app.set(obj, app_obj_id)

        elif object_type == 'mupif.TemporalProperty':
            edm_data = restApiControl.getEDMData(edm_dbname, edm_entity, edm_id, object_path)
            obj = getEDMTemporalPropertyInstance(edm_data=edm_data, data_id=data_id, value_type=value_type)
            app.set(obj, app_obj_id)

        elif object_type.startswith('mupif.DataList') and edm_list is True:
            datalist_object_type = object_type.replace(']', '').split('[')[1]
            obj_list = []

            if datalist_object_type == 'mupif.Property':
                tot = len(edm_ids)
                cur = 0
                stage = 0
                for e_id in edm_ids:
                    edm_data = restApiControl.getEDMData(edm_dbname, edm_entity, e_id, object_path)
                    obj = getEDMPropertyInstance(edm_data=edm_data, data_id=data_id, value_type=value_type)
                    obj_list.append(obj)
                    cur += 1
                    if cur / tot * 100 >= stage:
                        print("%d %%" % stage)
                        while stage <= cur / tot * 100:
                            stage += 10

            elif datalist_object_type == 'mupif.String':
                tot = len(edm_ids)
                cur = 0
                stage = 0
                for e_id in edm_ids:
                    edm_data = restApiControl.getEDMData(edm_dbname, edm_entity, e_id, object_path)
                    obj = getEDMStringInstance(edm_data=edm_data, data_id=data_id, value_type=value_type)
                    obj_list.append(obj)
                    cur += 1
                    if cur / tot * 100 >= stage:
                        print("%d %%" % stage)
                        while stage <= cur / tot * 100:
                            stage += 10

            elif datalist_object_type == 'mupif.TemporalProperty':
                tot = len(edm_ids)
                cur = 0
                stage = 0
                for e_id in edm_ids:
                    edm_data = restApiControl.getEDMData(edm_dbname, edm_entity, e_id, object_path)
                    obj = getEDMTemporalPropertyInstance(edm_data=edm_data, data_id=data_id, value_type=value_type)
                    obj_list.append(obj)
                    cur += 1
                    if cur / tot * 100 >= stage:
                        print("%d %%" % stage)
                        while stage <= cur / tot * 100:
                            stage += 10

            did = mupif.DataID[data_id.replace('mupif.DataID.', '')]
            datalist_instance = mupif.DataList(objs=obj_list, dataID=did)
            app.set(datalist_instance, app_obj_id)

        else:
            raise ValueError('Handling of Onto io param of type %s is not implemented' % object_type)

    else:
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
                            getDaemon().register(pf)
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
                            getDaemon().register(hs)
                            hs.exposeData()
                            app.set(hs, app_obj_id)

                elif object_type == 'mupif.Field':
                    file_id = inp_record['Object'].get('FileID', None)
                    if file_id is not None:
                        # load from hdf5 file
                        pfile, fn = restApiControl.getBinaryFileByID(file_id)
                        with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
                            full_path = tempDir + "/file.h5"
                            f = open(full_path, 'wb')
                            f.write(pfile)
                            f.close()
                            field = mupif.Field.makeFromHdf5(full_path)[0]
                            app.set(field, app_obj_id)
                else:
                    raise ValueError('Handling of io param of type %s is not implemented' % object_type)


def getOntoBaseObjectByName(objects, name):
    for i in objects:
        if i['Name'] == name:
            return i
    return None

def createOutputEDMMappingObjects(app, eid, outputs):
    execution = restApiControl.getExecutionRecord(eid)
    OBO = execution.get('EDMMapping', [])
    if len(OBO):
        print("Creating EDM output objects")
        outputs = restApiControl.getExecutionOutputRecord(eid)
        edmpaths = []
        for path in [out.get('EDMPath', None) for out in outputs]:
            if path is not None and path != '':
                edmpaths.append(path)

        for obo in OBO:
            if not obo.get('id', None) and not obo.get('ids', None):
                if obo.get('createFrom', None) is not None:
                    source_obo = getOntoBaseObjectByName(OBO, obo.get('createFrom'))
                    if source_obo is not None:
                        if source_obo.get('id', None) is not None and source_obo.get('id', None) != '':
                            edm_name = obo.get('Name', '')
                            valid_emdpaths = filter(lambda p: p.startswith(edm_name), edmpaths)
                            valid_emdpaths = [p.replace(obo.get('Name')+'.', '') for p in valid_emdpaths]

                            safe_links = restApiControl.getSafeLinks(
                                DBName=source_obo.get('DBName', ''),
                                Type=source_obo.get('EDMEntity', ''),
                                ID=source_obo.get('id', ''),
                                paths=valid_emdpaths
                            )

                            if obo.get('EDMList', False) is True:
                                num = getEDMListLength(app=app, eid=eid, edm_name=obo.get('Name', ''), outputs=outputs)
                                new_ids = []
                                for i in range(0, num):
                                    new_id = restApiControl.cloneEDMData(
                                        DBName=source_obo.get('DBName', ''),
                                        Type=source_obo.get('EDMEntity', ''),
                                        ID=source_obo.get('id'),
                                        shallow=safe_links
                                    )
                                    new_ids.append(new_id)
                                restApiControl.setExecutionOntoBaseObjectIDs(eid, name=obo.get('Name'), value=new_ids)
                                for new_id in new_ids:
                                    restApiControl.setEDMData(DBName=obo.get('DBName', ''), Type=obo.get('EDMEntity', ''), ID=new_id, path="meta", data={"execution": eid})
                            else:
                                new_id = restApiControl.cloneEDMData(
                                    DBName=source_obo.get('DBName', ''),
                                    Type=source_obo.get('EDMEntity', ''),
                                    ID=source_obo.get('id'),
                                    shallow=safe_links
                                )
                                restApiControl.setExecutionOntoBaseObjectID(eid, name=obo.get('Name'), value=new_id)
                                restApiControl.setEDMData(DBName=obo.get('DBName', ''), Type=obo.get('EDMEntity', ''), ID=new_id, path="meta", data={"execution": eid})

                elif obo.get('createNew', None) is not None:
                    edm_name = obo.get('Name', '')
                    valid_emdpaths = filter(lambda p: p.startswith(edm_name), edmpaths)
                    valid_emdpaths = [p.replace(obo.get('Name') + '.', '') for p in valid_emdpaths]

                    new_id = restApiControl.createEDMData(
                        DBName=obo.get('DBName', ''),
                        Type=obo.get('EDMEntity', ''),
                        data=obo.get('createNew')
                    )
                    restApiControl.setExecutionOntoBaseObjectID(eid, name=obo.get('Name'), value=new_id)
                    restApiControl.setEDMData(DBName=obo.get('DBName', ''), Type=obo.get('EDMEntity', ''), ID=new_id, path="meta", data={"execution": eid})
        print("Creating EDM output objects finished")


def mapInputs(app, eid):
    execution = restApiControl.getExecutionRecord(eid)
    workflow = restApiControl.getWorkflowRecordGeneral(execution['WorkflowID'], execution['WorkflowVersion'])
    workflow_input_templates = workflow['IOCard']['Inputs']
    if api_type == 'granta':
        workflow_input_templates = restApiControl._getGrantaWorkflowMetadataFromDatabase(execution['WorkflowID']).get('Inputs', [])

    for input_template in workflow_input_templates:
        print("Mapping input " + str(input_template))
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

        edmlist = False
        edmpath = input_template.get('EDMPath', None)
        edm_base_objects = execution.get('EDMMapping', [])
        if edmpath is not None:
            splitted = edmpath.split('.', 1)
            base_object_name = splitted[0]
            info = {}
            for i in edm_base_objects:
                if i['Name'] == base_object_name:
                    info = i
                    edmlist = info.get('EDMList', False)
                    break

        for oid in objID:
            mapInput(
                app=app,
                eid=eid,
                name=name,
                obj_id=oid,
                app_obj_id=oid,
                data_id=data_id,
                object_type=object_type,
                onto_path=input_template.get('EDMPath', None),
                onto_base_objects=execution.get('EDMMapping', []),
                value_type=input_template.get('ValueType', ''),
                edm_list=edmlist
            )


def getEDMListLength(app, eid, edm_name, outputs):
    for outitem in outputs:
        if outitem.get('EDMList', False) is True:
            name = outitem['Name']
            object_type = outitem['Type']
            typeID = outitem.get('TypeID', outitem.get('Type_ID'))
            # try to get raw PID from typeID
            match = re.search('\w+\Z', typeID)
            if match:
                typeID = match.group()

            objID = outitem.get('ObjID', outitem.get('Obj_ID', ''))
            if not ObjIDIsIterable(objID):
                objID = [objID]

            for oid in objID:
                edmpath = outitem.get('EDMPath', None)
                if edmpath is not None:
                    edmpath_base = edmpath.split('.')[0] if len(edmpath.split('.')) else None
                    if edmpath_base == edm_name:
                        obj = app.get(mupif.DataID[typeID], None, oid)
                        if obj.isInstance(mupif.DataList):
                            return len(obj.objs)

    return 0


def setEDMDataToList(dbname, edmentity, edm_ids, object_path, data):
    tot = len(edm_ids)
    cur = 0
    stage = 0
    for edm_id in edm_ids:
        restApiControl.setEDMData(dbname, edmentity, edm_id, object_path, data=data)
        cur += 1
        if cur / tot * 100 >= stage:
            print("%d %%" % stage)
            while stage <= cur / tot * 100:
                stage += 10


def mapOutput(app, eid, name, obj_id, data_id, time, object_type, onto_path=None, onto_base_objects={}, edm_list=False):
    if object_type == 'mupif.Property':
        prop = app.get(mupif.DataID[data_id], time, obj_id)

        if prop.valueType in [mupif.ValueType.Scalar, mupif.ValueType.Vector, mupif.ValueType.Tensor]:
            if onto_path is not None:
                splitted = onto_path.split('.', 1)
                base_object_name = splitted[0]
                object_path = splitted[1]
                # find base object info
                info = {}
                for i in onto_base_objects:
                    if i['Name'] == base_object_name:
                        info = i
                data = prop.to_db_dict(dialect='edm')
                if edm_list is True:
                    setEDMDataToList(info.get('DBName', ''), info.get('EDMEntity', ''), info.get('ids', []), object_path, data=data)
                else:
                    restApiControl.setEDMData(info.get('DBName', ''), info.get('EDMEntity', ''), info.get('id', ''), object_path, data=data)
            else:
                restApiControl.setExecutionOutputObject(eid, name, obj_id, prop.to_db_dict())
        elif prop.valueType in [mupif.ValueType.ScalarArray, mupif.ValueType.VectorArray, mupif.ValueType.TensorArray]:
            with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
                full_path = tempDir + "/file.h5"
                prop.saveHdf5(full_path)
                if onto_path is not None:
                    splitted = onto_path.split('.', 1)
                    base_object_name = splitted[0]
                    object_path = splitted[1]
                    # find base object info
                    info = {}
                    for i in onto_base_objects:
                        if i['Name'] == base_object_name:
                            info = i
                    fileID = None
                    with open(full_path, 'rb') as f:
                        fileID = restApiControl.uploadEDMBinaryFile(info.get('DBName', ''), f)
                        f.close()
                    if fileID is not None:
                        data = {
                            "ClassName": "ConstantProperty",
                            "FileID": fileID,
                            "ValueType": prop.valueType.name,
                            "DataID": prop.propID.name
                        }
                        if edm_list is True:
                            setEDMDataToList(info.get('DBName', ''), info.get('EDMEntity', ''), info.get('ids', []), object_path, data=data)
                        else:
                            restApiControl.setEDMData(info.get('DBName', ''), info.get('EDMEntity', ''), info.get('id', ''), object_path, data=data)
                    else:
                        raise ConnectionError("hdf5 file was not saved")

                else:
                    fileID = None
                    with open(full_path, 'rb') as f:
                        fileID = restApiControl.uploadBinaryFile(f)
                        f.close()
                    if fileID is not None:
                        restApiControl.setExecutionOutputObject(eid, name, obj_id, {'FileID': fileID})
                    else:
                        raise ConnectionError("hdf5 file was not saved")

    elif object_type == 'mupif.TemporalProperty':
        prop = app.get(mupif.DataID[data_id], time, obj_id)

        if onto_path is not None:
            splitted = onto_path.split('.', 1)
            base_object_name = splitted[0]
            object_path = splitted[1]
            # find base object info
            info = {}
            for i in onto_base_objects:
                if i['Name'] == base_object_name:
                    info = i
            data = prop.to_db_dict(dialect='edm')
            if edm_list is True:
                setEDMDataToList(info.get('DBName', ''), info.get('EDMEntity', ''), info.get('ids', []), object_path, data=data)
            else:
                restApiControl.setEDMData(info.get('DBName', ''), info.get('EDMEntity', ''), info.get('id', ''), object_path, data=data)
        else:
            restApiControl.setExecutionOutputObject(eid, name, obj_id, prop.to_db_dict())

    elif object_type == 'mupif.String':
        prop = app.get(mupif.DataID[data_id], time, obj_id)
        if onto_path is not None:
            splitted = onto_path.split('.', 1)
            base_object_name = splitted[0]
            object_path = splitted[1]
            # find base object info
            info = {}
            for i in onto_base_objects:
                if i['Name'] == base_object_name:
                    info = i
            # set the desired object
            data = prop.to_db_dict(dialect='edm')
            if edm_list is True:
                setEDMDataToList(info.get('DBName', ''), info.get('EDMEntity', ''), info.get('ids', []), object_path, data=data)
            else:
                restApiControl.setEDMData(info.get('DBName', ''), info.get('EDMEntity', ''), info.get('id', ''), object_path, data=data)
        else:
            restApiControl.setExecutionOutputObject(eid, name, obj_id, prop.to_db_dict())

    elif object_type.startswith('mupif.DataList'):
        dl = app.get(mupif.DataID[data_id], time, obj_id)
        if onto_path is not None and edm_list is True:
            splitted = onto_path.split('.', 1)
            base_object_name = splitted[0]
            object_path = splitted[1]
            # find base object info
            info = {}
            for i in onto_base_objects:
                if i['Name'] == base_object_name:
                    info = i
            ids = info.get('ids', [])
            lids = len(ids)
            lobjs = len(dl.objs)
            if len(ids) == len(dl.objs):
                for idx in range(0, len(ids)):
                    data = dl.objs[idx].to_db_dict(dialect='edm')
                    restApiControl.setEDMData(info.get('DBName', ''), info.get('EDMEntity', ''), ids[idx], object_path, data=data)
            else:
                raise ValueError('The length of the DataList does not match the length of the EDM objects list. (%d x %s)' % (len(ids), len(dl.objs)))
        else:
            raise ValueError('Handling of io param of type %s not implemented' % object_type)

    # elif object_type == 'mupif.PyroFile':
    #     prop = app.get(mupif.DataID[data_id], time, obj_id)
    #     restApiControl.setExecutionOutputObject(eid, name, obj_id, prop.to_db_dict())

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

    elif object_type == 'mupif.Field':
        field = app.get(mupif.DataID[data_id], time, obj_id)
        with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
            full_path = tempDir + "/file.h5"
            field.toHdf5(fileName=full_path)
            fileID = None
            with open(full_path, 'rb') as f:
                fileID = restApiControl.uploadBinaryFile(f)
                f.close()
            if fileID is not None:
                restApiControl.setExecutionOutputObject(eid, name, obj_id, {'FileID': fileID})
            else:
                print("hdf5 file was not saved")

    elif object_type == 'mupif.TemporalField':
        field = app.get(mupif.DataID[data_id], time, obj_id)
        with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
            full_path = tempDir + "/file.h5"
            field.toHdf5(full_path)
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

    elif object_type == 'mupif.String':
        string = app.get(mupif.DataID[data_id], time, obj_id)
        return {
            "name": str(name),
            "value": prop.getValue(),
            "type": "str"
        }

    if object_type == 'mupif.HeavyStruct':
        hs = app.get(mupif.DataID[data_id], time, obj_id)
        with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
            full_path = tempDir + "/file.h5"
            if isinstance(hs, Pyro5.api.Proxy):
                hs = hs.copyRemote()
            hs_copy = hs.deepcopy()
            hs_copy.moveStorage(full_path)
            fileID = None
            with open(full_path, 'rb') as f:
                fileID = restApiControl.uploadBinaryFile(f)
                f.close()
            if fileID is None:
                print("hdf5 file was not saved")
            else:
                return {
                    "name": str(name),
                    "value": {
                        "url": "https://musicode.grantami.com/musicode/filestore/%s" % str(fileID),
                        "description": None
                    },
                    "type": "hyperlink"
                }

    elif object_type == 'mupif.Field':
        field = app.get(mupif.DataID[data_id], time, obj_id)
        with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
            full_path = tempDir + "/file.h5"
            field.toHdf5(fileName=full_path)
            fileID = None
            with open(full_path, 'rb') as f:
                fileID = restApiControl.uploadBinaryFile(f)
                f.close()
            if fileID is None:
                print("hdf5 file was not saved")
            else:
                return {
                    "name": str(name),
                    "value": {
                        "url": "https://musicode.grantami.com/musicode/filestore/%s" % str(fileID),
                        "description": None
                    },
                    "type": "hyperlink"
                }

    elif object_type == 'mupif.PyroFile':
        pf = app.get(mupif.DataID[data_id], time, obj_id)
        fn = pf.getBasename()
        with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
            full_path = tempDir + "/" + fn
            mupif.PyroFile.copy(pf, full_path)
            fileID = None
            with open(full_path, 'rb') as f:
                fileID = restApiControl.uploadBinaryFile(f)
                f.close()
            if fileID is None:
                print("hdf5 file was not saved")
            else:
                return {
                    "name": str(name),
                    "value": {
                        "url": "https://musicode.grantami.com/musicode/filestore/%s" % str(fileID),
                        "description": None
                    },
                    "type": "hyperlink"
                }

    elif object_type == 'mupif.Function':
        obj = app.get(mupif.DataID[data_id], time, obj_id)
        return {
            "name": str(name),
            "value": {"x": obj.x, "y": obj.y},
            "type": "series",
            "unit": str(obj.unit)
        }

    return None


def mapOutputs(app, eid, time):
    execution = restApiControl.getExecutionRecord(eid)
    workflow = restApiControl.getWorkflowRecordGeneral(execution['WorkflowID'], execution['WorkflowVersion'])
    workflow_output_templates = workflow['IOCard']['Outputs']
    if api_type == 'granta':
        workflow_output_templates = restApiControl._getGrantaWorkflowMetadataFromDatabase(execution['WorkflowID']).get('Outputs', [])

    granta_output_data = []

    outputs = restApiControl.getExecutionOutputRecord(eid)
    if api_type == 'granta':
        outputs = workflow_output_templates

    createOutputEDMMappingObjects(app=app, eid=eid, outputs=outputs)
    execution = restApiControl.getExecutionRecord(eid)

    for outitem in outputs:
        print("Mapping output " + str(outitem))
        name = outitem['Name']
        object_type = outitem['Type']
        typeID = outitem.get('TypeID', outitem.get('Type_ID'))
        # try to get raw PID from typeID
        match = re.search('\w+\Z', typeID)
        if match:
            typeID = match.group()

        objID = outitem.get('ObjID', outitem.get('Obj_ID', ''))
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
                edmlist = False
                edmpath = outitem.get('EDMPath', None)
                edm_base_objects = execution.get('EDMMapping', [])
                if edmpath is not None:
                    splitted = edmpath.split('.', 1)
                    base_object_name = splitted[0]
                    info = {}
                    for i in edm_base_objects:
                        if i['Name'] == base_object_name:
                            info = i
                            edmlist = info.get('EDMList', False)
                            break

                mapOutput(
                    app=app,
                    eid=eid,
                    name=name,
                    obj_id=oid,
                    data_id=typeID,
                    time=time,
                    object_type=object_type,
                    onto_path=edmpath,
                    onto_base_objects=edm_base_objects,
                    edm_list=edmlist  # outitem.get('EDMList', False)
                )

    if api_type == 'granta':
        restApiControl._setGrantaExecutionResults(eid, granta_output_data)

