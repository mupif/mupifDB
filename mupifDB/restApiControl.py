import requests
import json
import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")


RESTserver = os.environ.get('MUPIFDB_REST_SERVER', "http://127.0.0.1:5000/")

# RESTserver *must* have trailing /, fix if not
if not RESTserver[-1] == '/':
    RESTserver += '/'


# --------------------------------------------------
# Users
# --------------------------------------------------

def getUserByIP(ip):
    response = requests.get(RESTserver + "main?action=get_user_by_ip&ip=" + str(ip))
    response_json = response.json()
    return response_json['result']


# --------------------------------------------------
# Usecases
# --------------------------------------------------

def getUsecaseRecords():
    data = []
    response = requests.get(RESTserver + "main?action=get_usecases")
    response_json = response.json()
    for record in response_json['result']:
        data.append(record)
    return data


def getUsecaseRecord(ucid):
    response = requests.get(RESTserver + "main?action=get_usecase&id=" + ucid)
    response_json = response.json()
    return response_json['result']


def insertUsecaseRecord(ucid, description):
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
    data = []
    response = requests.get(RESTserver + "main?action=get_workflows")
    response_json = response.json()
    for record in response_json['result']:
        data.append(record)
    return data


def getWorkflowRecordsWithUsecase(usecase):
    data = []
    response = requests.get(RESTserver + "main?action=get_workflows_for_usecase&usecase=" + str(usecase))
    response_json = response.json()
    for record in response_json['result']:
        data.append(record)
    return data


def getWorkflowRecord(wid):
    response = requests.get(RESTserver + "main?action=get_workflow&wid=" + wid)
    response_json = response.json()
    return response_json['result']


def setWorkflowParameter(workflow_id, param, value):
    response = requests.get(RESTserver + "main?action=modify_workflow&wid=" + str(workflow_id) + "&key=" + str(param) + "&value=" + str(value))


def insertWorkflow(data):
    response = requests.post(RESTserver + "main?action=insert_workflow", data=json.dumps(data))
    response_json = response.json()
    return response_json['result']


def updateWorkflow(data):
    response = requests.post(RESTserver + "main?action=update_workflow", data=json.dumps(data))
    response_json = response.json()
    return response_json['result']


def getWorkflowRecordGeneral(wid, version):
    workflow_newest = getWorkflowRecord(wid)
    if workflow_newest is not None:
        if workflow_newest['Version'] == version or version == -1 or version == None:
            return workflow_newest
    return getWorkflowRecordFromHistory(wid, version)


# --------------------------------------------------
# Workflows history
# --------------------------------------------------

def getWorkflowRecordFromHistory(wid, version):
    response = requests.get(RESTserver + "main?action=get_workflow_from_history&wid=" + str(wid) + "&version=" + str(version))
    response_json = response.json()
    print(response_json)
    for record in response_json['result']:
        return record
    return None


def insertWorkflowHistory(data):
    response = requests.post(RESTserver + "main?action=insert_workflow_history", data=json.dumps(data))
    response_json = response.json()
    return response_json['result']


# --------------------------------------------------
# Executions
# --------------------------------------------------

def getExecutionRecords(workflow_id=None, workflow_version=None, label=None, num_limit=None, status=None):
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


def getExecutionRecord(weid):
    response = requests.get(RESTserver + "main?action=get_execution&id=" + str(weid))
    response_json = response.json()
    return response_json['result']


def getScheduledExecutions():
    return getExecutionRecords(status="Scheduled")


def getPendingExecutions():
    return getExecutionRecords(status="Pending")


def scheduleExecution(execution_id):
    response = requests.get(RESTserver + "main?action=schedule_execution&id=" + str(execution_id))
    return response.status_code == 200


def setExecutionParameter(execution_id, param, value):
    response = requests.get(RESTserver + "main?action=modify_execution&id=" + str(execution_id) + "&key=" + str(param) + "&value=" + str(value))
    return response.status_code == 200


def setExecutionStatusScheduled(execution_id):
    return setExecutionParameter(execution_id, "Status", "Scheduled")


def setExecutionStatusPending(execution_id):
    setExecutionParameter(execution_id, "SubmittedDate", str(datetime.datetime.now()))
    return setExecutionParameter(execution_id, "Status", "Pending")


def setExecutionStatusRunning(execution_id):
    setExecutionParameter(execution_id, "StartDate", str(datetime.datetime.now()))
    return setExecutionParameter(execution_id, "Status", "Running")


def setExecutionStatusFinished(execution_id):
    setExecutionParameter(execution_id, "EndDate", str(datetime.datetime.now()))
    return setExecutionParameter(execution_id, "Status", "Finished")


def setExecutionStatusFailed(execution_id):
    setExecutionParameter(execution_id, "EndDate", str(datetime.datetime.now()))
    return setExecutionParameter(execution_id, "Status", "Failed")


def insertExecution(workflow_wid, version, ip):
    response = requests.get(RESTserver + "main?action=insert_new_execution&wid=" + str(workflow_wid) + "&version=" + str(version) + "&ip=" + str(ip))
    response_json = response.json()
    return response_json['result']


def insertExecutionRecord(data):
    response = requests.get(RESTserver + "main?action=insert_execution_data", data=json.dumps(data))
    response_json = response.json()
    return response_json['result']


def getExecutionInputRecord(weid):
    response = requests.get(RESTserver + "main?action=get_execution_inputs&id=" + str(weid))
    response_json = response.json()
    return response_json['result']


def getExecutionOutputRecord(weid):
    response = requests.get(RESTserver + "main?action=get_execution_outputs&id=" + str(weid))
    response_json = response.json()
    return response_json['result']


def getExecutionInputRecordItem(weid, name, obj_id):
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
    response = requests.get(RESTserver + "main?action=get_execution_inputs_check&id=" + str(weid))
    response_json = response.json()
    return response_json['result'] == 'OK'


# --------------------------------------------------
# IO Data
# --------------------------------------------------

def getIODataRecord(iod_id):
    response = requests.get(RESTserver + "main?action=get_iodata&id=" + str(iod_id))
    response_json = response.json()
    return response_json['result']


def insertIODataRecord(data):
    response = requests.post(RESTserver + "main?action=insert_iodata", data=json.dumps(data))
    response_json = response.json()
    return response_json['result']


def setExecutionInputValue(execution_id, name, value, obj_id):
    response = requests.get(RESTserver + "main?action=set_execution_input&id=" + str(execution_id) + "&name=" + str(name) + "&value=" + str(value) + "&obj_id=" + str(obj_id))
    return response.status_code == 200


def setExecutionInputLink(weid, name, obj_id, link_eid, link_name, link_obj_id):
    response = requests.get(RESTserver + "main?action=set_execution_input_link&id=" + str(weid) + "&name=" + str(name) + "&obj_id=" + str(obj_id) + "&link_eid=" + str(link_eid) + "&link_name=" + str(link_name) + "&link_obj_id=" + str(link_obj_id))
    return response.status_code == 200


def setExecutionInputObject(weid, name, obj_id, object_dict):
    response = requests.put(RESTserver + "main?action=set_execution_input_object&id=" + str(weid) + "&name=" + str(name) + "&obj_id=" + str(obj_id), json=object_dict)
    return response.status_code == 200


def setExecutionOutputObject(weid, name, obj_id, object_dict):
    response = requests.put(RESTserver + "main?action=set_execution_output_object&id=" + str(weid) + "&name=" + str(name) + "&obj_id=" + str(obj_id), json=object_dict)
    return response.status_code == 200


def setExecutionOutputValue(weid, name, value, obj_id):
    response = requests.get(RESTserver + "main?action=set_execution_output&id=" + str(weid) + "&name=" + str(name) + "&value=" + str(value) + "&obj_id=" + str(obj_id))
    return response.status_code == 200


def setExecutionOutputFileID(weid, name, fileID, obj_id):
    response = requests.get(RESTserver + "main?action=set_execution_output&id=" + str(weid) + "&name=" + str(name) + "&file_id=" + str(fileID) + "&obj_id=" + str(obj_id))
    return response.status_code == 200


def getExecutionInputValue(weid, name, obj_id):
    response = requests.get(RESTserver + "main?action=get_execution_input&id=" + str(weid) + "&name=" + str(name) + "&obj_id=" + str(obj_id))
    response_json = response.json()
    return response_json['result']


def getExecutionOutputValue(weid, name, obj_id):
    response = requests.get(RESTserver + "main?action=get_execution_output&id=" + str(weid) + "&name=" + str(name) + "&obj_id=" + str(obj_id))
    response_json = response.json()
    return response_json['result']


# --------------------------------------------------
# Files
# --------------------------------------------------

def getBinaryFileContentByID(fid):
    response = requests.get(RESTserver + "main?action=get_file&id=" + str(fid), allow_redirects=True)
    return response.content


def getFileNameByID(fid):
    response = requests.get(RESTserver + "main?action=get_filename&id=" + str(fid), allow_redirects=True)
    response_json = response.json()
    return str(response_json['result'])


def uploadBinaryFileContent(binary_data):  # todo
    response = requests.post(RESTserver + "upload", files={"myfile": binary_data})
    response_json = response.json()
    return response_json['result']


# --------------------------------------------------
# Stat
# --------------------------------------------------

def getStatus():
    response = requests.get(RESTserver + "main?action=get_status")
    response_json = response.json()
    return response_json['result']


def getStatScheduler():
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
    if runningTasks is not None:
        response = session.get(RESTserver + "main?action=set_scheduler_stat&key=scheduler.runningTasks&value=" + str(runningTasks))
    if scheduledTasks is not None:
        response = session.get(RESTserver + "main?action=set_scheduler_stat&key=scheduler.scheduledTasks&value=" + str(scheduledTasks))
    if load is not None:
        response = session.get(RESTserver + "main?action=set_scheduler_stat&key=scheduler.load&value=" + str(load))
    if processedTasks is not None:
        response = session.get(RESTserver + "main?action=set_scheduler_stat&key=scheduler.processedTasks&value=" + str(processedTasks))


def updateStatScheduler(runningTasks=None, scheduledTasks=None, load=None, processedTasks=None):
    if runningTasks is not None:
        response = requests.get(RESTserver + "main?action=update_scheduler_stat&key=scheduler.runningTasks&value=" + str(runningTasks))
    if scheduledTasks is not None:
        response = requests.get(RESTserver + "main?action=update_scheduler_stat&key=scheduler.scheduledTasks&value=" + str(scheduledTasks))
    if load is not None:
        response = requests.get(RESTserver + "main?action=update_scheduler_stat&key=scheduler.load&value=" + str(load))
    if processedTasks is not None:
        response = requests.get(RESTserver + "main?action=update_scheduler_stat&key=scheduler.processedTasks&value=" + str(processedTasks))
