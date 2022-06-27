import requests
from requests.auth import HTTPBasicAuth
import json
import datetime
import sys
import os
import table_structures
import tempfile
import importlib
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")


RESTserver = os.environ.get('MUPIFDB_REST_SERVER', "http://127.0.0.1:5000/")

# RESTserver *must* have trailing /, fix if not
if not RESTserver[-1] == '/':
    RESTserver += '/'


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
    response = requests.get(RESTserver + "main?action=get_user_by_ip&ip=" + str(ip))
    response_json = response.json()
    return response_json['result']


# --------------------------------------------------
# Usecases
# --------------------------------------------------

def getUsecaseRecords():
    if api_type == 'granta':
        return []
    data = []
    response = requests.get(RESTserver + "main?action=get_usecases")
    response_json = response.json()
    for record in response_json['result']:
        data.append(record)
    return data


def getUsecaseRecord(ucid):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=get_usecase&id=" + ucid)
    response_json = response.json()
    return response_json['result']


def insertUsecaseRecord(ucid, description):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=insert_usecase&ucid=" + ucid + "&description=" + description)
    response_json = response.json()
    print(response_json)
    if 'result' in response_json:
        return response_json['result']
    return None


# --------------------------------------------------
# Workflows
# --------------------------------------------------

def getWorkflowRecords():
    if api_type == 'granta':
        return []
    data = []
    response = requests.get(RESTserver + "main?action=get_workflows")
    response_json = response.json()
    for record in response_json['result']:
        data.append(record)
    return data


def getWorkflowRecordsWithUsecase(usecase):
    if api_type == 'granta':
        return []
    data = []
    response = requests.get(RESTserver + "main?action=get_workflows_for_usecase&usecase=" + str(usecase))
    response_json = response.json()
    for record in response_json['result']:
        data.append(record)
    return data


def getWorkflowRecord(wid):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=get_workflow&wid=" + wid)
    response_json = response.json()
    return response_json['result']


def setWorkflowParameter(workflow_id, param, value):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=modify_workflow&wid=" + str(workflow_id) + "&key=" + str(param) + "&value=" + str(value))


def insertWorkflow(data):
    if api_type == 'granta':
        return None
    response = requests.post(RESTserver + "main?action=insert_workflow", data=json.dumps(data))
    response_json = response.json()
    return response_json['result']


def updateWorkflow(data):
    if api_type == 'granta':
        return None
    response = requests.post(RESTserver + "main?action=update_workflow", data=json.dumps(data))
    response_json = response.json()
    return response_json['result']


def fix_json(val):
    import re
    val = re.sub(",[ \t\r\n]+}", "}", val)
    val = re.sub(",[ \t\r\n]+\]", "]", val)
    val
    return val


def getWorkflowRecordGeneral(wid, version):  # todo granta
    if api_type == 'granta':
        url = RESTserver + 'templates/' + str(wid)
        headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        r = requests.get(url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']))
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

                        # todo delete this temporary fix
                        if units == 'degK':
                            units = 'K'

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
    return None


# --------------------------------------------------
# Workflows history
# --------------------------------------------------

def getWorkflowRecordFromHistory(wid, version):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=get_workflow_from_history&wid=" + str(wid) + "&version=" + str(version))
    response_json = response.json()
    print(response_json)
    for record in response_json['result']:
        return record
    return None


def insertWorkflowHistory(data):
    if api_type == 'granta':
        return None
    response = requests.post(RESTserver + "main?action=insert_workflow_history", data=json.dumps(data))
    response_json = response.json()
    return response_json['result']


# --------------------------------------------------
# Executions
# --------------------------------------------------

def getExecutionRecords(workflow_id=None, workflow_version=None, label=None, num_limit=None, status=None):
    if api_type == 'granta':
        return []
    data = []
    endpoint_address = RESTserver + "main?action=get_executions"
    if num_limit is not None:
        endpoint_address += "&num_limit=" + str(num_limit)
    if label is not None:
        endpoint_address += "&label=" + str(label)
    if workflow_id is not None:
        endpoint_address += "&workflow_id=" + str(workflow_id)
    if workflow_version is not None:
        endpoint_address += "&workflow_version=" + str(workflow_version)
    if status is not None:
        endpoint_address += "&status=" + str(status)

    print(endpoint_address)
    response = requests.get(endpoint_address)
    response_json = response.json()
    for record in response_json['result']:
        data.append(record)
    return data


def getExecutionRecord(weid):  # todo granta
    if api_type == 'granta':
        url = RESTserver + 'executions/' + str(weid)
        headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        r = requests.get(url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']))
        r_json = r.json()
        execution = table_structures.extendRecord({}, table_structures.tableExecution)
        execution['_id'] = r_json['guid']
        execution['WorkflowID'] = r_json['template_guid']
        execution['WorkflowVersion'] = -1
        execution['Status'] = 'unknown'
        if r_json['status'] == 'Ready':
            execution['Status'] = 'Pending'
        execution['Task_ID'] = ''
        return execution
    response = requests.get(RESTserver + "main?action=get_execution&id=" + str(weid))
    response_json = response.json()
    return response_json['result']


def getScheduledExecutions():  # todo granta
    if api_type == 'granta':
        return []
    return getExecutionRecords(status="Scheduled")


def getPendingExecutions():  # todo granta
    if api_type == 'granta':
        url = RESTserver + 'executions/?status=Ready'
        headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        r = requests.get(url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']))
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
    return getExecutionRecords(status="Pending")


def scheduleExecution(execution_id):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=schedule_execution&id=" + str(execution_id))
    return response.status_code == 200


def setExecutionParameter(execution_id, param, value, val_type="str"):  # todo granta
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=modify_execution&id=" + str(execution_id) + "&key=" + str(param) + "&value=" + str(value) + "&val_type=" + str(val_type))
    return response.status_code == 200


def setExecutionAttemptsCount(execution_id, val):  # todo granta
    if api_type == 'granta':
        return None
    return setExecutionParameter(execution_id, "Attempts", val, "int")


def _setGrantaExecutionResults(eid, val_list):
    url = RESTserver + 'executions/' + str(eid)
    headers = {'content-type': 'application/json', 'charset': 'UTF-8', 'accept': 'application/json', 'Accept-Charset': 'UTF-8'}
    newdata = {"results": val_list}
    r = requests.patch(url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']), data=json.dumps(newdata))
    # print(r)
    # print(r.text)
    return None


def _setGrantaExecutionStatus(eid, val):
    url = RESTserver + 'executions/' + str(eid)
    headers = {'content-type': 'application/json', 'charset': 'UTF-8', 'accept': 'application/json', 'Accept-Charset': 'UTF-8'}
    newdata = {"status": str(val)}
    r = requests.patch(url, headers=headers, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']), data=json.dumps(newdata))
    return True


def setExecutionStatusScheduled(execution_id):
    if api_type == 'granta':
        return _setGrantaExecutionStatus(execution_id, 'Planned')
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
        return _setGrantaExecutionStatus(execution_id, 'On-going')
    setExecutionParameter(execution_id, "StartDate", str(datetime.datetime.now()))
    return setExecutionParameter(execution_id, "Status", "Running")


def setExecutionStatusFinished(execution_id):
    if api_type == 'granta':
        return _setGrantaExecutionStatus(execution_id, 'Completed')
    setExecutionParameter(execution_id, "EndDate", str(datetime.datetime.now()))
    return setExecutionParameter(execution_id, "Status", "Finished")


def setExecutionStatusFailed(execution_id):
    if api_type == 'granta':
        return _setGrantaExecutionStatus(execution_id, 'Cancelled')

    setExecutionParameter(execution_id, "EndDate", str(datetime.datetime.now()))
    return setExecutionParameter(execution_id, "Status", "Failed")


def insertExecution(workflow_wid, version, ip):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=insert_new_execution&wid=" + str(workflow_wid) + "&version=" + str(version) + "&ip=" + str(ip))
    response_json = response.json()
    return response_json['result']


def insertExecutionRecord(data):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=insert_execution_data", data=json.dumps(data))
    response_json = response.json()
    return response_json['result']


def getExecutionInputRecord(weid):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=get_execution_inputs&id=" + str(weid))
    response_json = response.json()
    return response_json['result']


def getExecutionOutputRecord(weid):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=get_execution_outputs&id=" + str(weid))
    response_json = response.json()
    return response_json['result']


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


def getExecutionInputsCheck(weid):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=get_execution_inputs_check&id=" + str(weid))
    response_json = response.json()
    return response_json['result'] == 'OK'


# --------------------------------------------------
# IO Data
# --------------------------------------------------

def getIODataRecord(iod_id):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=get_iodata&id=" + str(iod_id))
    response_json = response.json()
    return response_json['result']


def insertIODataRecord(data):
    if api_type == 'granta':
        return None
    response = requests.post(RESTserver + "main?action=insert_iodata", data=json.dumps(data))
    response_json = response.json()
    return response_json['result']


def setExecutionInputValue(execution_id, name, value, obj_id):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=set_execution_input&id=" + str(execution_id) + "&name=" + str(name) + "&value=" + str(value) + "&obj_id=" + str(obj_id))
    return response.status_code == 200


def setExecutionInputLink(weid, name, obj_id, link_eid, link_name, link_obj_id):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=set_execution_input_link&id=" + str(weid) + "&name=" + str(name) + "&obj_id=" + str(obj_id) + "&link_eid=" + str(link_eid) + "&link_name=" + str(link_name) + "&link_obj_id=" + str(link_obj_id))
    return response.status_code == 200


def setExecutionInputObject(weid, name, obj_id, object_dict):
    if api_type == 'granta':
        return None
    response = requests.put(RESTserver + "main?action=set_execution_input_object&id=" + str(weid) + "&name=" + str(name) + "&obj_id=" + str(obj_id), json=object_dict)
    return response.status_code == 200


def setExecutionOutputObject(weid, name, obj_id, object_dict):
    if api_type == 'granta':
        return None
    response = requests.put(RESTserver + "main?action=set_execution_output_object&id=" + str(weid) + "&name=" + str(name) + "&obj_id=" + str(obj_id), json=object_dict)
    return response.status_code == 200


def setExecutionOutputValue(weid, name, value, obj_id):
    response = requests.get(RESTserver + "main?action=set_execution_output&id=" + str(weid) + "&name=" + str(name) + "&value=" + str(value) + "&obj_id=" + str(obj_id))
    return response.status_code == 200


def setExecutionOutputFileID(weid, name, fileID, obj_id):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=set_execution_output&id=" + str(weid) + "&name=" + str(name) + "&file_id=" + str(fileID) + "&obj_id=" + str(obj_id))
    return response.status_code == 200


def getExecutionInputValue(weid, name, obj_id):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=get_execution_input&id=" + str(weid) + "&name=" + str(name) + "&obj_id=" + str(obj_id))
    response_json = response.json()
    return response_json['result']


def getExecutionOutputValue(weid, name, obj_id):
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=get_execution_output&id=" + str(weid) + "&name=" + str(name) + "&obj_id=" + str(obj_id))
    response_json = response.json()
    return response_json['result']


def getPropertyArrayData(file_id, i_start, i_count):  # may not be used
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=get_property_array_data&file_id=" + str(file_id) + "&i_start=" + i_start + "&i_count=" + i_count)
    response_json = response.json()
    return response_json['result']


# --------------------------------------------------
# Files
# --------------------------------------------------

def getBinaryFileByID(fid):
    if api_type == 'granta':
        url = RESTserver.replace('/api/', '/filestore/')
        response = requests.get(url + str(fid), auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']), allow_redirects=True)
        return response.content, response.headers['content-disposition'].split('filename=')[1].replace('"', '')

    # name
    response = requests.get(RESTserver + "main?action=get_filename&id=" + str(fid), allow_redirects=True)
    response_json = response.json()
    filename = str(response_json['result'])
    # content
    response = requests.get(RESTserver + "main?action=get_file&id=" + str(fid), allow_redirects=True)
    return response.content, filename


def uploadBinaryFile(binary_data):
    if api_type == 'granta':
        url = RESTserver.replace('/api/', '/filestore')
        response = requests.post(url, auth=HTTPBasicAuth(granta_credentials['username'], granta_credentials['password']), files={"file": binary_data})
        response_json = response.json()
        return response_json['guid']
    response = requests.post(RESTserver + "upload", files={"myfile": binary_data})
    response_json = response.json()
    return response_json['result']


# --------------------------------------------------
# Stat
# --------------------------------------------------

def getStatus():
    if api_type == 'granta':
        return None
    response = requests.get(RESTserver + "main?action=get_status")
    response_json = response.json()
    return response_json['result']


def getStatScheduler():
    if api_type == 'granta':
        return {"runningTasks": 0, "scheduledTasks": 0, "load": 0, "processedTasks": 0}
    response = requests.get(RESTserver + "main?action=get_scheduler_stat")
    response_json = response.json()
    keys = ["runningTasks", "scheduledTasks", "load", "processedTasks"]
    for k in keys:
        if k not in response_json['result']:
            response_json['result'][k] = None
    return response_json['result']

# session is the requests module by default (one-off session for each request) but can be passed 
# a custom requests.Session() object with config such as retries and timeouts.
# This feature is implemented only for setStatsScheduler to cleanly handle scheduler startup.
def setStatScheduler(runningTasks=None, scheduledTasks=None, load=None, processedTasks=None, session=requests):
    if api_type == 'granta':
        return None
    if runningTasks is not None:
        response = session.get(RESTserver + "main?action=set_scheduler_stat&key=scheduler.runningTasks&value=" + str(runningTasks))
    if scheduledTasks is not None:
        response = session.get(RESTserver + "main?action=set_scheduler_stat&key=scheduler.scheduledTasks&value=" + str(scheduledTasks))
    if load is not None:
        response = session.get(RESTserver + "main?action=set_scheduler_stat&key=scheduler.load&value=" + str(load))
    if processedTasks is not None:
        response = session.get(RESTserver + "main?action=set_scheduler_stat&key=scheduler.processedTasks&value=" + str(processedTasks))


def updateStatScheduler(runningTasks=None, scheduledTasks=None, load=None, processedTasks=None):
    if api_type == 'granta':
        return None
    if runningTasks is not None:
        response = requests.get(RESTserver + "main?action=update_scheduler_stat&key=scheduler.runningTasks&value=" + str(runningTasks))
    if scheduledTasks is not None:
        response = requests.get(RESTserver + "main?action=update_scheduler_stat&key=scheduler.scheduledTasks&value=" + str(scheduledTasks))
    if load is not None:
        response = requests.get(RESTserver + "main?action=update_scheduler_stat&key=scheduler.load&value=" + str(load))
    if processedTasks is not None:
        response = requests.get(RESTserver + "main?action=update_scheduler_stat&key=scheduler.processedTasks&value=" + str(processedTasks))
