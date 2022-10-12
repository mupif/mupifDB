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
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")

RESTserver = os.environ.get('MUPIFDB_REST_SERVER', "http://127.0.0.1:5000/")

# RESTserver *must* have trailing /, fix if not
if not RESTserver[-1] == '/':
    RESTserver += '/'

RESTserver_new = RESTserver.replace('5000', '8005')


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
    response = requests.get(RESTserver_new + "users/" + str(ip))
    return response.json()


# --------------------------------------------------
# Usecases
# --------------------------------------------------

def getUsecaseRecords():
    if api_type == 'granta':
        return []
    data = []
    response = requests.get(RESTserver_new + "usecases/")
    response_json = response.json()
    if response_json:
        print(response)
        for record in response_json:
            data.append(record)
    return data


def getUsecaseRecord(ucid):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver_new + "usecases/" + ucid)
    response_json = response.json()
    return response_json


def insertUsecaseRecord(ucid, description):
    if api_type == 'granta':
        return None
    response = requests.post(RESTserver_new + "usecases/", data=json.dumps({"ucid": ucid, "description": description}))
    return response.json()


# --------------------------------------------------
# Workflows
# --------------------------------------------------

def getWorkflowRecords():
    if api_type == 'granta':
        return []
    data = []
    response = requests.get(RESTserver_new + "workflows/")
    response_json = response.json()
    for record in response_json:
        data.append(record)
    return data


def getWorkflowRecordsWithUsecase(usecase):
    if api_type == 'granta':
        return []
    data = []
    response = requests.get(RESTserver_new + "usecases/" + str(usecase) + "/workflows")
    response_json = response.json()
    for record in response_json:
        data.append(record)
    return data


def getWorkflowRecord(wid):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver_new + "workflows/" + wid)
    response_json = response.json()
    return response_json


def insertWorkflow(data):
    if api_type == 'granta':
        return None
    response = requests.post(RESTserver_new + "workflows/", data=json.dumps({"entity": data}))
    return response.json()


def updateWorkflow(data):
    if api_type == 'granta':
        return None
    response = requests.patch(RESTserver_new + "workflows/", data=json.dumps({"workflow": data}))
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
        r = requests.get(url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']))
        if r.status_code == 200:
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
                    fid = gmd['value'].split('/')[-1]

            if fid:
                file, filename = getBinaryFileByID(fid)
                workflow['GridFSID'] = fid
                workflow['modulename'] = filename.replace('.py', '')

            return workflow
        return None

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
    r = requests.get(url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']))
    r_json = r.json()
    for inp in r_json:
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
                    'TypeID': 'mupif.DataID.ID_None',
                    'Units': units,  # todo
                    'ValueType': 'Scalar',
                    'Value': None,
                    'FileID': None,
                    'Link': {},
                    'Object': {
                        'ClassName': 'ConstantProperty',
                        'ValueType': 'Scalar',
                        'DataID': 'ID_None',
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
                    'TypeID': 'mupif.DataID.ID_None',
                    'Units': units,  # todo
                    'ValueType': 'Scalar',
                    'Value': None,
                    'FileID': None,
                    'Link': {},
                    'Object': {
                        'ClassName': 'String',
                        'DataID': 'ID_None',
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
                        'TypeID': 'mupif.DataID.ID_None',
                        'Units': '',
                        'ValueType': 'Scalar',
                        'Value': None,
                        'FileID': None,
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
                        'TypeID': 'mupif.DataID.ID_None',
                        'Units': '',
                        'ValueType': 'Scalar',
                        'Value': None,
                        'FileID': None,
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
                        'TypeID': 'mupif.DataID.ID_None',
                        'Units': '',
                        'ValueType': 'Scalar',
                        'Value': None,
                        'FileID': None,
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
    response = requests.get(RESTserver_new + "workflows_history/" + wid + "/" + str(version))
    response_json = response.json()
    return response_json


def insertWorkflowHistory(data):
    if api_type == 'granta':
        return None
    response = requests.post(RESTserver_new + "workflows_history/", data=json.dumps({"entity": data}))
    return response.json()


# --------------------------------------------------
# Executions
# --------------------------------------------------

def getExecutionRecords(workflow_id=None, workflow_version=None, label=None, num_limit=None, status=None):
    if api_type == 'granta':
        url = RESTserver + 'executions/'
        headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        r = requests.get(url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']))
        if r.status_code == 200:
            r_json = r.json()
            res = []
            for ex in r_json:
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
        return []
    data = []
    endpoint_address = RESTserver_new + "executions/?noparam"
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
    response = requests.get(endpoint_address)
    response_json = response.json()
    for record in response_json:
        data.append(record)
    return data


def getExecutionRecord(weid):
    if api_type == 'granta':
        url = RESTserver + 'executions/' + str(weid)
        headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        r = requests.get(url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']))
        if r.status_code == 200:
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
        return None
    response = requests.get(RESTserver_new + "executions/" + str(weid))
    response_json = response.json()
    return response_json


def getScheduledExecutions():
    if api_type == 'granta':
        return []
    return getExecutionRecords(status="Scheduled")


def getPendingExecutions():
    if api_type == 'granta':
        url = RESTserver + 'executions/?status=Ready'
        headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        r = requests.get(url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']))
        if r.status_code == 200:
            r_json = r.json()
            res = []
            for ex in r_json:
                execution = table_structures.extendRecord({}, table_structures.tableExecution)
                execution['_id'] = ex['guid']
                execution['WorkflowID'] = ex['template_guid']
                execution['WorkflowVersion'] = -1
                execution['Status'] = 'Pending'
                execution['Task_ID'] = ''
                res.append(execution)
            return res
        return []
    return getExecutionRecords(status="Pending")


def scheduleExecution(execution_id):
    if api_type == 'granta':
        return None
    response = requests.patch(RESTserver_new + "executions/" + str(execution_id) + "/schedule")
    return response.json()


def setExecutionParameter(execution_id, param, value, val_type="str"):
    if api_type == 'granta':
        return None
    response = requests.patch(RESTserver_new + "executions/" + str(execution_id), data=json.dumps({"key": str(param), "value": value}))
    return response.json()


def setExecutionAttemptsCount(execution_id, val):
    if api_type == 'granta':
        return None
    return setExecutionParameter(execution_id, "Attempts", val, "int")


def _setGrantaExecutionResults(eid, val_list):
    url = RESTserver + 'executions/' + str(eid)
    headers = {'content-type': 'application/json', 'charset': 'UTF-8', 'accept': 'application/json', 'Accept-Charset': 'UTF-8'}
    newdata = {"results": val_list}
    r = requests.patch(url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']), data=json.dumps(newdata))
    return None


def _setGrantaExecutionStatus(eid, val):
    url = RESTserver + 'executions/' + str(eid)
    headers = {'content-type': 'application/json', 'charset': 'UTF-8', 'accept': 'application/json', 'Accept-Charset': 'UTF-8'}
    newdata = {"status": str(val)}
    r = requests.patch(url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']), data=json.dumps(newdata))
    return True


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


def createExecution(wid, version, ip):
    if api_type == 'granta':
        return None
    response = requests.post(RESTserver_new + "executions/create/", data=json.dumps({"wid": str(wid), "version": str(version), "ip": str(ip)}))
    return response.json()


def insertExecution(data):
    if api_type == 'granta':
        return None
    response = requests.post(RESTserver_new + "executions/", data=json.dumps({"entity": data}))
    return response.json()


def getExecutionInputRecord(weid):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver_new + "executions/" + str(weid) + "/inputs/")
    return response.json()


def getExecutionOutputRecord(weid):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver_new + "executions/" + str(weid) + "/outputs/")
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
    response = requests.get(RESTserver_new + "iodata/" + str(iod_id))
    return response.json()


def insertIODataRecord(data):
    if api_type == 'granta':
        return None
    response = requests.post(RESTserver_new + "iodata/", data=json.dumps({"entity": data}))
    return response.json()


def setExecutionInputLink(weid, name, obj_id, link_eid, link_name, link_obj_id):
    if api_type == 'granta':
        return None
    response = requests.patch(RESTserver_new + "executions/" + str(weid) + "/input_item/" + str(name) + "/" + str(obj_id) + "/", data=json.dumps({"link": {"ExecID": link_eid, "Name": link_name, "ObjID": link_obj_id}}))
    return response.json()


def setExecutionInputObject(weid, name, obj_id, object_dict):
    if api_type == 'granta':
        return None
    response = requests.patch(RESTserver_new + "executions/" + str(weid) + "/input_item/" + str(name) + "/" + str(obj_id) + "/", data=json.dumps({"object": object_dict}))
    return response.json()


def setExecutionOutputObject(weid, name, obj_id, object_dict):
    if api_type == 'granta':
        return None
    response = requests.patch(RESTserver_new + "executions/" + str(weid) + "/output_item/" + str(name) + "/" + str(obj_id) + "/", data=json.dumps({"object": object_dict}))
    return response.json()


def getPropertyArrayData(file_id, i_start, i_count):  # may not be used
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver_new + "property_array_data/" + str(file_id) + "/" + str(i_start) + "/" + str(i_count) + "/")
    return response.json()


# --------------------------------------------------
# Files
# --------------------------------------------------

def getBinaryFileByID(fid):
    if api_type == 'granta':
        url = RESTserver.replace('/api/', '/filestore/')
        response = requests.get(url + str(fid), auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']), allow_redirects=True)
        return response.content, response.headers['content-disposition'].split('filename=')[1].replace('"', '')

    response = requests.get(RESTserver_new + "file/" + str(fid))
    d = response.headers['Content-Disposition']
    filename = re.findall("filename=(.+)", d)[0]
    return response.content, filename


def uploadBinaryFile(binary_data):
    if api_type == 'granta':
        url = RESTserver.replace('/api/', '/filestore')
        response = requests.post(url, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']), files={"file": binary_data})
        response_json = response.json()
        return response_json['guid']

    response = requests.post(RESTserver_new + "file/", files={"file": binary_data})
    return response.json()


# --------------------------------------------------
# Stat
# --------------------------------------------------

def getStatus():
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver_new + "status/")
    return response.json()


def getStatScheduler():
    if api_type == 'granta':
        return {"runningTasks": 0, "scheduledTasks": 0, "load": 0, "processedTasks": 0}
    response = requests.get(RESTserver_new + "scheduler_statistics/")
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
        response = session.patch(RESTserver_new + "scheduler_statistics/", data=json.dumps({"key": "scheduler.runningTasks", "value": runningTasks}))
    if scheduledTasks is not None:
        response = session.patch(RESTserver_new + "scheduler_statistics/", data=json.dumps({"key": "scheduler.scheduledTasks", "value": scheduledTasks}))
    if load is not None:
        response = session.patch(RESTserver_new + "scheduler_statistics/", data=json.dumps({"key": "scheduler.load", "value": load}))
    if processedTasks is not None:
        response = session.patch(RESTserver_new + "scheduler_statistics/", data=json.dumps({"key": "scheduler.processedTasks", "value": processedTasks}))
    return


def updateStatScheduler(runningTasks=None, scheduledTasks=None, load=None, processedTasks=None):
    if api_type == 'granta':
        return None
    if runningTasks is not None:
        response = requests.patch(RESTserver_new + "scheduler_statistics/", data=json.dumps({"key": "scheduler.runningTasks", "value": runningTasks}))
    if scheduledTasks is not None:
        response = requests.patch(RESTserver_new + "scheduler_statistics/", data=json.dumps({"key": "scheduler.scheduledTasks", "value": scheduledTasks}))
    if load is not None:
        response = requests.patch(RESTserver_new + "scheduler_statistics/", data=json.dumps({"key": "scheduler.load", "value": load}))
    if processedTasks is not None:
        response = requests.patch(RESTserver_new + "scheduler_statistics/", data=json.dumps({"key": "scheduler.processedTasks", "value": processedTasks}))
    return
