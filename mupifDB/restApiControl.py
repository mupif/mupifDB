import requests
from requests.auth import HTTPBasicAuth
import json
import datetime
import sys
import os
import table_structures
import tempfile
import importlib
import re
import logging
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")


#

def Request(*, method, url, headers=None, auth=None, data=None, timeout=None, files={}, params={}):
    if method == 'get':
        response = requests.get(url=url, timeout=timeout, headers=headers, auth=auth, data=data, params=params)
    elif method == 'post':
        response = requests.post(url=url, timeout=timeout, headers=headers, auth=auth, data=data, files=files)
    elif method == 'patch':
        response = requests.patch(url=url, timeout=timeout, headers=headers, auth=auth, data=data)
    elif method == 'put':
        response = requests.put(url=url, timeout=timeout, headers=headers, auth=auth, data=data)
    elif method == 'delete':
        response = requests.delete(url=url, timeout=timeout, headers=headers, auth=auth, data=data)
    else:
        raise Exception('Unknown API method')

    if response.status_code >= 200 and response.status_code <= 299:
        return response
    else:
        raise Exception('API returned code ' + str(response.status_code))
    return None

def rGet(*, url, headers=None, auth=None, timeout=10, params={}):
    return Request(method='get', url=url, headers=headers, auth=auth, timeout=timeout, params=params)

def rPost(*, url, headers=None, auth=None, data=None, timeout=10, files={}):
    return Request(method='post', url=url, headers=headers, auth=auth, timeout=timeout, data=data, files=files)

def rPatch(*, url, headers=None, auth=None, data=None, timeout=10):
    return Request(method='patch', url=url, headers=headers, auth=auth, timeout=timeout, data=data)

def rPut(*, url, headers=None, auth=None, data=None, timeout=10):
    return Request(method='put', url=url, headers=headers, auth=auth, timeout=timeout, data=data)

def rDelete(*, url, headers=None, auth=None, timeout=10):
    return Request(method='delete', url=url, headers=headers, auth=auth)

#

RESTserver = os.environ.get('MUPIFDB_REST_SERVER', "http://127.0.0.1:8005/")
RESTserver = RESTserver.replace('5000', '8005')

# RESTserver *must* have trailing /, fix if not
if not RESTserver[-1] == '/':
    RESTserver += '/'

RESTserverMuPIF = RESTserver

granta_credentials = {'username': '', 'password': ''}

api_type = os.environ.get('MUPIFDB_REST_SERVER_TYPE', "mupif")
if api_type == 'granta':
    RESTserver = 'https://musicode.grantami.com/musicode/api/'

    with open("/var/lib/mupif/persistent/granta_api_login.json") as json_data_file:
        credentials = json.load(json_data_file)
        granta_credentials = {'username': credentials['username'], 'password': credentials['password']}
        username = credentials['username']
        password = credentials['password']
        server = credentials['server']

# --------------------------------------------------
# Users
# --------------------------------------------------

def getUserByIP(ip):
    if api_type == 'granta':
        return None
    response = rGet(url=RESTserver + "users/" + str(ip))
    return response.json()


# --------------------------------------------------
# Usecases
# --------------------------------------------------

def getUsecaseRecords():
    if api_type == 'granta':
        return []
    data = []
    response = rGet(url=RESTserver + "usecases/")
    for record in response.json():
        data.append(record)
    return data


def getUsecaseRecord(ucid):
    if api_type == 'granta':
        return None
    response = rGet(url=RESTserver + "usecases/" + ucid)
    return response.json()


def insertUsecaseRecord(ucid, description):
    if api_type == 'granta':
        return None
    response = rPost(url=RESTserver + "usecases/", data=json.dumps({"ucid": ucid, "description": description}))
    return response.json()


# --------------------------------------------------
# Workflows
# --------------------------------------------------

def getWorkflowRecords():
    if api_type == 'granta':
        return []
    data = []
    response = rGet(url=RESTserver + "workflows/")
    for record in response.json():
        data.append(record)
    return data


def getWorkflowRecordsWithUsecase(usecase):
    if api_type == 'granta':
        return []
    data = []
    response = rGet(url=RESTserver + "usecases/" + str(usecase) + "/workflows")
    for record in response.json():
        data.append(record)
    return data


def getWorkflowRecord(wid):
    if api_type == 'granta':
        return None
    response = rGet(url=RESTserver + "workflows/" + wid)
    return response.json()


def insertWorkflow(data):
    if api_type == 'granta':
        return None
    response = rPost(url=RESTserver + "workflows/", data=json.dumps({"entity": data}))
    return response.json()


def updateWorkflow(data):
    if api_type == 'granta':
        return None
    response = rPatch(url=RESTserver + "workflows/", data=json.dumps({"entity": data}))
    return response.json()


def fix_json(val):
    import re
    val = re.sub(",[ \t\r\n]+}", "}", val)
    val = re.sub(",[ \t\r\n]+\]", "]", val)
    val
    return val


def getWorkflowRecordGeneral(wid, version):
    if api_type == 'granta':
        url = RESTserver + 'templates/' + str(wid)
        headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        r = rGet(url=url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']))
        r_json = r.json()
        workflow = table_structures.extendRecord({}, table_structures.tableWorkflow)
        workflow['_id'] = r_json['guid']
        workflow['wid'] = r_json['guid']
        workflow['WorkflowVersion'] = 1

        workflow['modulename'] = 'unknown'
        workflow['classname'] = 'unknown'
        workflow['GridFSID'] = 'unknown'
        workflow['UseCase'] = ''
        workflow['metadata'] = ''

        fid = None
        gmds = r_json['metadata']

        for gmd in gmds:
            if gmd['name'] == 'muPIF metadata':
                md = json.loads(fix_json(gmd['value']))
                workflow['metadata'] = md
                workflow['classname'] = md['ClassName']
                workflow['modulename'] = md['ModuleName']

            if gmd['name'] == 'workflow python file':
                fid = gmd['value']['url'].split('/')[-1]

        if fid:
            file, filename = getBinaryFileByID(fid)
            workflow['GridFSID'] = fid
            workflow['modulename'] = filename.replace('.py', '')

        return workflow

    workflow_newest = getWorkflowRecord(wid)
    if workflow_newest is not None:
        if workflow_newest['Version'] == version or version == -1 or version == None:
            return workflow_newest
    return getWorkflowRecordFromHistory(wid, version)


def _getGrantaWorkflowMetadataFromDatabase(wid):
    workflow_record = getWorkflowRecordGeneral(wid, -1)
    return workflow_record.get('metadata', {})


def _getGrantaWorkflowMetadataFromFile(wid, key=None):
    workflow_record = getWorkflowRecordGeneral(wid, -1)
    if workflow_record.get('GridFSID', None) is not None:
        with tempfile.TemporaryDirectory(dir='/tmp', prefix='mupifDB') as tempDir:
            try:
                fc, fn = getBinaryFileByID(workflow_record['GridFSID'])
                if fn.split('.')[-1] == 'py':
                    with open(tempDir + '/' + workflow_record['modulename'] + '.py', "wb") as f:
                        f.write(fc)
                        f.close()

                        moduleImport = importlib.import_module(workflow_record['modulename'])
                        workflow_class = getattr(moduleImport, workflow_record["classname"])
                        workflow = workflow_class()
                        if key is None:
                            return workflow.getAllMetadata()
                        else:
                            return workflow.getMetadata(key)
            except Exception as e:
                print(e)
                return {}
    return {}


def _getGrantaExecutionInputItem(eid, name):
    def ObjIDIsIterable(val):
        try:
            a = val[0]
            if not isinstance(val, str):
                return True
        except:
            return False

    url = RESTserver + 'executions/' + str(eid) + '/inputs'
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    r = rGet(url=url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']))
    for inp in r.json():
        if name == inp['name']:
            if inp['type'] == 'float':
                # fint units first :(
                execution_record = getExecutionRecord(eid)
                w_inputs = _getGrantaWorkflowMetadataFromDatabase(execution_record['WorkflowID']).get('Inputs', [])
                units = ''
                for w_i in w_inputs:
                    if w_i['Name'] == name:
                        units = w_i['Units']
                return {
                    'Compulsory': True,
                    'Description': '',
                    'Name': inp['name'],
                    'ObjID': inp['name'],
                    'Type': 'mupif.Property',
                    'TypeID': w_i['Type_ID'],
                    'Units': units,  # todo
                    'ValueType': 'Scalar',
                    'Link': {},
                    'Object': {
                        'ClassName': 'ConstantProperty',
                        'ValueType': 'Scalar',
                        'DataID': w_i['Type_ID'].replace('mupif.DataID.', ''),
                        'Unit': units,  # todo
                        'Value': inp['value'],
                        'Time': None
                    }
                }

            if inp['type'] == 'str':
                # fint units first :(
                execution_record = getExecutionRecord(eid)
                w_inputs = _getGrantaWorkflowMetadataFromDatabase(execution_record['WorkflowID']).get('Inputs', [])
                units = ''
                for w_i in w_inputs:
                    if w_i['Name'] == name:
                        units = w_i['Units']
                return {
                    'Compulsory': True,
                    'Description': '',
                    'Name': inp['name'],
                    'ObjID': inp['name'],
                    'Type': 'mupif.String',
                    'TypeID': w_i['Type_ID'],
                    'Units': units,  # todo
                    'ValueType': 'Scalar',
                    'Link': {},
                    'Object': {
                        'ClassName': 'String',
                        'DataID': w_i['Type_ID'].replace('mupif.DataID.', ''),
                        'Value': inp['value']
                    }
                }

            if inp['type'] == 'hyperlink':
                execution_record = getExecutionRecord(eid)
                w_inputs = _getGrantaWorkflowMetadataFromDatabase(execution_record['WorkflowID']).get('Inputs', [])
                units = ''
                obj_type = ''
                for w_i in w_inputs:
                    if w_i['Name'] == name:
                        units = w_i['Units']
                        obj_type = w_i['Type']

                if obj_type == 'mupif.HeavyStruct':
                    return {
                        'Compulsory': True,
                        'Description': '',
                        'Name': inp['name'],
                        'ObjID': inp['name'],
                        'Type': 'mupif.HeavyStruct',
                        'TypeID': w_i['Type_ID'],
                        'Units': '',
                        'ValueType': 'Scalar',
                        'Link': {},
                        'Object': {
                            'FileID': inp['value'].split('/')[-1]
                        }
                    }

                if obj_type == 'mupif.PyroFile':
                    return {
                        'Compulsory': True,
                        'Description': '',
                        'Name': inp['name'],
                        'ObjID': inp['name'],
                        'Type': 'mupif.PyroFile',
                        'TypeID': w_i['Type_ID'],
                        'Units': '',
                        'ValueType': 'Scalar',
                        'Link': {},
                        'Object': {
                            'FileID': inp['value'].split('/')[-1]
                        }
                    }

                if obj_type == 'mupif.Field':
                    return {
                        'Compulsory': True,
                        'Description': '',
                        'Name': inp['name'],
                        'ObjID': inp['name'],
                        'Type': 'mupif.Field',
                        'TypeID': w_i['Type_ID'],
                        'Units': '',
                        'ValueType': 'Scalar',
                        'Link': {},
                        'Object': {
                            'FileID': inp['value'].split('/')[-1]
                        }
                    }
    return None


# --------------------------------------------------
# Workflows history
# --------------------------------------------------

def getWorkflowRecordFromHistory(wid, version):
    if api_type == 'granta':
        return None
    response = rGet(url=RESTserver + "workflows_history/" + wid + "/" + str(version))
    return response.json()


def insertWorkflowHistory(data):
    if api_type == 'granta':
        return None
    response = rPost(url=RESTserver + "workflows_history/", data=json.dumps({"entity": data}))
    return response.json()


# --------------------------------------------------
# Executions
# --------------------------------------------------

def getExecutionRecords(workflow_id=None, workflow_version=None, label=None, num_limit=None, status=None):
    if api_type == 'granta':
        url = RESTserver + 'executions/'
        headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        r = rGet(url=url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']))
        res = []
        for ex in r.json():
            execution = table_structures.extendRecord({}, table_structures.tableExecution)
            execution['_id'] = ex['guid']
            execution['WorkflowID'] = ex['template_guid']
            execution['WorkflowVersion'] = -1
            st = ex['status']
            if st == 'Ready':
                st = 'Pending'
            if st == 'On-going':
                st = 'Running'
            if st == 'Completed':
                st = 'Finished'
            if st == 'Completed, to be reviewed':
                st = 'Finished'
            if st == 'Completed & reviewed':
                st = 'Finished'
            if st == 'Canceled':
                st = 'Failed'
            execution['Status'] = st
            execution['Task_ID'] = ''
            res.append(execution)
        return res
    data = []
    endpoint_address = RESTserver + "executions/?noparam"
    if num_limit is not None:
        endpoint_address += "&num_limit=" + str(num_limit)
    if label is not None:
        endpoint_address += "&label=" + str(label)
    if workflow_id is not None:
        endpoint_address += "&workflow_id=" + str(workflow_id)
    if workflow_version is not None:
        endpoint_address += "&workflow_version=" + str(workflow_version)
    if status:
        endpoint_address += "&status=" + str(status)
    response = rGet(url=endpoint_address)
    for record in response.json():
        data.append(record)
    return data


def getExecutionRecord(weid):
    if api_type == 'granta':
        url = RESTserver + 'executions/' + str(weid)
        headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        r = rGet(url=url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']))
        r_json = r.json()
        execution = table_structures.extendRecord({}, table_structures.tableExecution)
        execution['_id'] = r_json['guid']
        execution['WorkflowID'] = r_json['template_guid']
        execution['WorkflowVersion'] = -1
        execution['Status'] = 'unknown'

        st = r_json['status']
        if st == 'Ready':
            st = 'Pending'
        if st == 'On-going':
            st = 'Running'
        if st == 'Completed':
            st = 'Finished'
        if st == 'Completed, to be reviewed':
            st = 'Finished'
        if st == 'Completed & reviewed':
            st = 'Finished'
        if st == 'Canceled':
            st = 'Failed'
        execution['Status'] = st

        execution['Task_ID'] = ''
        return execution
    response = rGet(url=RESTserver + "executions/" + str(weid))
    return response.json()


def getScheduledExecutions():
    if api_type == 'granta':
        return []
    return getExecutionRecords(status="Scheduled")


def getPendingExecutions():
    if api_type == 'granta':
        url = RESTserver + 'executions/?status=Ready'
        headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        r = rGet(url=url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']))
        res = []
        for ex in r.json():
            execution = table_structures.extendRecord({}, table_structures.tableExecution)
            execution['_id'] = ex['guid']
            execution['WorkflowID'] = ex['template_guid']
            execution['WorkflowVersion'] = -1
            execution['Status'] = 'Pending'
            execution['Task_ID'] = ''
            res.append(execution)
        return res
    return getExecutionRecords(status="Pending")


def scheduleExecution(execution_id):
    if api_type == 'granta':
        return None
    response = rPatch(url=RESTserver + "executions/" + str(execution_id) + "/schedule")
    return response.json()


def setExecutionParameter(execution_id, param, value, val_type="str"):
    if api_type == 'granta':
        return None
    response = rPatch(url=RESTserver + "executions/" + str(execution_id), data=json.dumps({"key": str(param), "value": value}))
    return response.json()


def setExecutionOntoBaseObjectID(execution_id, name, value):
    if api_type == 'granta':
        return None
    response = rPatch(url=RESTserver + "executions/" + str(execution_id) + "/set_onto_base_object_id/", data=json.dumps({"name": str(name), "value": value}))
    if response.status_code == 200:
        return response.json()
    return None


def setExecutionOntoBaseObjectIDs(execution_id, name, value):
    if api_type == 'granta':
        return None
    response = rPatch(url=RESTserver + "executions/" + str(execution_id) + "/set_onto_base_object_ids/", data=json.dumps({"name": str(name), "value": value}))
    if response.status_code == 200:
        return response.json()
    return None


def setExecutionAttemptsCount(execution_id, val):
    if api_type == 'granta':
        return None
    return setExecutionParameter(execution_id, "Attempts", val, "int")


def _setGrantaExecutionResults(eid, val_list):
    url = RESTserver + 'executions/' + str(eid)
    headers = {'content-type': 'application/json', 'charset': 'UTF-8', 'accept': 'application/json', 'Accept-Charset': 'UTF-8'}
    newdata = {"results": val_list}
    r = rPatch(url=url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']), data=json.dumps(newdata))
    if r.status_code == 200:
        return True
    return False


def _setGrantaExecutionStatus(eid, val):
    url = RESTserver + 'executions/' + str(eid)
    headers = {'content-type': 'application/json', 'charset': 'UTF-8', 'accept': 'application/json', 'Accept-Charset': 'UTF-8'}
    newdata = {"status": str(val)}
    r = rPatch(url=url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']), data=json.dumps(newdata))
    if r.status_code == 200:
        return True
    return False


def setExecutionStatusScheduled(execution_id):
    if api_type == 'granta':
        return _setGrantaExecutionStatus(execution_id, 'Scheduled')
    return setExecutionParameter(execution_id, "Status", "Scheduled")


def setExecutionStatusCreated(execution_id):  # only reverted
    if api_type == 'granta':
        return None
    setExecutionParameter(execution_id, "SubmittedDate", "")
    return setExecutionParameter(execution_id, "Status", "Created")


def setExecutionStatusPending(execution_id, reverted=False):
    if api_type == 'granta':
        return _setGrantaExecutionStatus(execution_id, 'Ready')
    if reverted:
        setExecutionParameter(execution_id, "StartDate", "")
    else:
        setExecutionParameter(execution_id, "SubmittedDate", str(datetime.datetime.now()))
        setExecutionAttemptsCount(execution_id, 0)
    return setExecutionParameter(execution_id, "Status", "Pending")


def setExecutionStatusRunning(execution_id):
    if api_type == 'granta':
        return _setGrantaExecutionStatus(execution_id, 'Running')
    setExecutionParameter(execution_id, "StartDate", str(datetime.datetime.now()))
    return setExecutionParameter(execution_id, "Status", "Running")


def setExecutionStatusFinished(execution_id):
    if api_type == 'granta':
        return _setGrantaExecutionStatus(execution_id, 'Completed, to be reviewed')
    setExecutionParameter(execution_id, "EndDate", str(datetime.datetime.now()))
    return setExecutionParameter(execution_id, "Status", "Finished")


def setExecutionStatusFailed(execution_id):
    if api_type == 'granta':
        return _setGrantaExecutionStatus(execution_id, 'Failed')
    setExecutionParameter(execution_id, "EndDate", str(datetime.datetime.now()))
    return setExecutionParameter(execution_id, "Status", "Failed")


def createExecution(wid, version, ip, no_onto=False):
    if api_type == 'granta':
        return None
    response = rPost(url=RESTserver + "executions/create/", data=json.dumps({"wid": str(wid), "version": str(version), "ip": str(ip), "no_onto": no_onto}))
    return response.json()


def insertExecution(data):
    if api_type == 'granta':
        return None
    response = rPost(url=RESTserver + "executions/", data=json.dumps({"entity": data}))
    return response.json()


def getExecutionInputRecord(weid):
    if api_type == 'granta':
        return None
    response = rGet(url=RESTserver + "executions/" + str(weid) + "/inputs/")
    return response.json()


def getExecutionOutputRecord(weid):
    if api_type == 'granta':
        return None
    response = rGet(url=RESTserver + "executions/" + str(weid) + "/outputs/")
    return response.json()


def getExecutionInputRecordItem(weid, name, obj_id):
    if api_type == 'granta':
        return None
    io_data = getExecutionInputRecord(weid)
    for elem in io_data:
        if elem.get('Name', None) == name and elem.get('ObjID', '') == obj_id:
            return elem


def getExecutionOutputRecordItem(weid, name, obj_id):
    io_data = getExecutionOutputRecord(weid)
    for elem in io_data:
        if elem.get('Name', None) == name and elem.get('ObjID', '') == obj_id:
            return elem


# --------------------------------------------------
# IO Data
# --------------------------------------------------

def getIODataRecord(iod_id):
    if api_type == 'granta':
        return None
    response = rGet(url=RESTserver + "iodata/" + str(iod_id))
    return response.json()


def insertIODataRecord(data):
    if api_type == 'granta':
        return None
    response = rPost(url=RESTserver + "iodata/", data=json.dumps({"entity": data}))
    return response.json()


def setExecutionInputLink(weid, name, obj_id, link_eid, link_name, link_obj_id):
    if api_type == 'granta':
        return None
    response = rPatch(url=RESTserver + "executions/" + str(weid) + "/input_item/" + str(name) + "/" + str(obj_id) + "/", data=json.dumps({"link": {"ExecID": link_eid, "Name": link_name, "ObjID": link_obj_id}}))
    return response.json()


def setExecutionInputObject(weid, name, obj_id, object_dict):
    if api_type == 'granta':
        return None
    response = rPatch(url=RESTserver + "executions/" + str(weid) + "/input_item/" + str(name) + "/" + str(obj_id) + "/", data=json.dumps({"object": object_dict}))
    return response.json()


def setExecutionOutputObject(weid, name, obj_id, object_dict):
    if api_type == 'granta':
        return None
    response = rPatch(url=RESTserver + "executions/" + str(weid) + "/output_item/" + str(name) + "/" + str(obj_id) + "/", data=json.dumps({"object": object_dict}))
    return response.json()


def getPropertyArrayData(file_id, i_start, i_count):  # may not be used
    if api_type == 'granta':
        return None
    response = rGet(url=RESTserver + "property_array_data/" + str(file_id) + "/" + str(i_start) + "/" + str(i_count) + "/")
    return response.json()


# --------------------------------------------------
# Files
# --------------------------------------------------

def getBinaryFileByID(fid):
    if api_type == 'granta':
        url = RESTserver.replace('/api/', '/filestore/')
        response = rGet(url=url + str(fid), auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']), allow_redirects=True)
        return response.content, response.headers['content-disposition'].split('filename=')[1].replace('"', '')

    response = rGet(url=RESTserver + "file/" + str(fid))
    d = response.headers['Content-Disposition']
    filename = re.findall("filename=(.+)", d)[0]
    return response.content, filename


def uploadBinaryFile(binary_data):
    if api_type == 'granta':
        url = RESTserver.replace('/api/', '/filestore')
        response = rPost(url=url, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']), files={"file": binary_data})
        return response.json()['guid']

    response = rPost(url=RESTserver + "file/", files={"file": binary_data})
    return response.json()

# --------------------------------------------------
# Logging
# --------------------------------------------------

def logMessage(*,name,levelno,pathname,lineno,created,**kw):
    '''
    Logging message; compulsory fileds are present in standard logging.LogRecord, their name
    should not be changed.

    - *name*: logger name; comes from logging.getLogger(name)
    - *levelno*: number of logging severity (e.g. 30 for logging.WARNING etc)
    - *pathname*: full path to file where the message originated
    - *lineno*: line number within file where the message originated
    - *created*: epoch time; use datetime.datetime.fromtimestamp(...) for higher-level representation

    Other possibly important fields in logging.LogRecord (not enforced by this function signature) are:

    - *exc_info*, *exc_text*: exception information when using log.exception(...) in the client code

       .. note:: exc_info is a python object (includes exception class and traceback),
                 there must be a custom routine to convert it to JSON.

    Constant extra fields might be added on the level of the handler: RestLogHandler(extraData=...).

    Variable extra fields might added in when calling the logging function, e.g. log.error(...,extra={'another-field':123})
    '''
    # return
    # re-assemble the dictionary
    data = dict(name=name,levelno=levelno,pathname=pathname,lineno=lineno,created=created,**kw)
    data['msg'] = data['msg'] % data['args']
    del data['args']
    # import rich.pretty
    # rich.pretty.pprint(data)
    previous_level = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        response = rPost(url=RESTserverMuPIF + "logs/", data=json.dumps({"entity": data}))
    finally:
        logging.disable(previous_level)
    return response.json()


# --------------------------------------------------
# Stat
# --------------------------------------------------

def getStatus():
    if api_type == 'granta':
        return None
    response = rGet(url=RESTserver + "status/")
    return response.json()


def getStatScheduler():
    if api_type == 'granta':
        return {"runningTasks": 0, "scheduledTasks": 0, "load": 0, "processedTasks": 0}
    response = rGet(url=RESTserver + "scheduler_statistics/")
    response_json = response.json()
    keys = ["runningTasks", "scheduledTasks", "load", "processedTasks"]
    for k in keys:
        if k not in response_json:
            response_json[k] = None
    return response_json

# session is the requests module by default (one-off session for each request) but can be passed 
# a custom requests.Session() object with config such as retries and timeouts.
# This feature is implemented only for setStatsScheduler to cleanly handle scheduler startup.
def setStatScheduler(runningTasks=None, scheduledTasks=None, load=None, processedTasks=None, session=requests):
    if api_type == 'granta':
        return None
    if runningTasks is not None:
        response = session.patch(RESTserver + "scheduler_statistics/", data=json.dumps({"key": "scheduler.runningTasks", "value": runningTasks}))
    if scheduledTasks is not None:
        response = session.patch(RESTserver + "scheduler_statistics/", data=json.dumps({"key": "scheduler.scheduledTasks", "value": scheduledTasks}))
    if load is not None:
        response = session.patch(RESTserver + "scheduler_statistics/", data=json.dumps({"key": "scheduler.load", "value": load}))
    if processedTasks is not None:
        response = session.patch(RESTserver + "scheduler_statistics/", data=json.dumps({"key": "scheduler.processedTasks", "value": processedTasks}))


def updateStatScheduler(runningTasks=None, scheduledTasks=None, load=None, processedTasks=None):
    if api_type == 'granta':
        return None
    if runningTasks is not None:
        response = rPatch(url=RESTserver + "scheduler_statistics/", data=json.dumps({"key": "scheduler.runningTasks", "value": runningTasks}))
    if scheduledTasks is not None:
        response = rPatch(url=RESTserver + "scheduler_statistics/", data=json.dumps({"key": "scheduler.scheduledTasks", "value": scheduledTasks}))
    if load is not None:
        response = rPatch(url=RESTserver + "scheduler_statistics/", data=json.dumps({"key": "scheduler.load", "value": load}))
    if processedTasks is not None:
        response = rPatch(url=RESTserver + "scheduler_statistics/", data=json.dumps({"key": "scheduler.processedTasks", "value": processedTasks}))


# --------------------------------------------------
# Ontology
# --------------------------------------------------

def getOntoDataArray(DBName, Type):
    response = rGet(url=RESTserver + "EDM/" + str(DBName) + "/" + str(Type))
    return response.json()

def getOntoData(DBName, Type, ID, path):
    if ID == '' or ID is None:
        return None
    url = RESTserver + "EDM/" + str(DBName) + "/" + str(Type) + "/" + str(ID) + "/?path=" + str(path)
    response = rGet(url=url)
    return response.json()


def setOntoData(DBName, Type, ID, path, data):
    url = RESTserver + "EDM/" + str(DBName) + "/" + str(Type) + "/" + str(ID)
    response = rPatch(url=url, data=json.dumps({"path": str(path), "data": data}))
    return response.json()


def createOntoData(DBName, Type, data):
    url = RESTserver + "EDM/" + str(DBName) + "/" + str(Type)
    response = rPost(url=url, data=json.dumps(data))
    return response.json()


def cloneOntoData(DBName, Type, ID, shallow=[]):
    url = RESTserver + "EDM/" + str(DBName) + "/" + str(Type) + "/" + str(ID) + "/clone"
    response = rGet(url=url, params={"shallow": ' '.join(shallow)})
    return response.json()


def getSafeLinks(DBName, Type, ID, paths=[]):
    url = RESTserver + "EDM/" + str(DBName) + "/" + str(Type) + "/" + str(ID) + "/safe-links"
    response = rGet(url=url, params={"paths": ' '.join(paths)})
    return response.json()
