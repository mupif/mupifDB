import requests
import json
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")


RESTserver = os.environ.get('MUPIFDB_REST_SERVER', "http://127.0.0.1:5000/")

# RESTserver *must* have trailing /, fix if not
if not RESTserver[-1] == '/':
    RESTserver += '/'


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
    for record in response_json['result']:
        return record
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
    response = requests.get(RESTserver + "main?action=get_workflow_with_usecase&usecase=" + str(usecase))
    response_json = response.json()
    for record in response_json['result']:
        data.append(record)
    return data


def getWorkflowRecord(wid):
    response = requests.get(RESTserver + "main?action=get_workflow&wid=" + wid)
    response_json = response.json()
    for record in response_json['result']:
        return record
    return None


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

def getExecutionRecords():
    data = []
    response = requests.get(RESTserver + "main?action=get_executions")
    response_json = response.json()
    for record in response_json['result']:
        data.append(record)
    return data


def getExecutionRecord(weid):
    response = requests.get(RESTserver + "main?action=get_execution&id=" + str(weid))
    response_json = response.json()
    for record in response_json['result']:
        return record
    return None


def getScheduledExecutions():
    data = []
    response = requests.get(RESTserver + "main?action=get_executions_with_status&status=Scheduled")
    response_json = response.json()
    for record in response_json['result']:
        data.append(record)
    return data


def getPendingExecutions():
    data = []
    response = requests.get(RESTserver + "main?action=get_executions_with_status&status=Pending")
    response_json = response.json()
    for record in response_json['result']:
        data.append(record)
    return data


def scheduleExecution(execution_id):
    response = requests.get(RESTserver + "main?action=schedule_execution&id=" + str(execution_id))
    return response.status_code == 200


def setExecutionParameter(execution_id, param, value):
    response = requests.get(RESTserver + "main?action=modify_execution&id=" + str(execution_id) + "&key=" + str(param) + "&value=" + str(value))
    return response.status_code == 200


def setExecutionStatusScheduled(execution_id):
    return setExecutionParameter(execution_id, "Status", "Scheduled")


def setExecutionStatusPending(execution_id):
    return setExecutionParameter(execution_id, "Status", "Pending")


def setExecutionStatusRunning(execution_id):
    return setExecutionParameter(execution_id, "Status", "Running")


def setExecutionStatusFinished(execution_id, log_id):
    return setExecutionParameter(execution_id, "Status", "Finished")


def setExecutionStatusFailed(execution_id, log_id):
    return setExecutionParameter(execution_id, "Status", "Failed")


def insertExecution(workflow_wid):
    response = requests.get(RESTserver + "main?action=insert_execution_all&wid=" + str(workflow_wid))
    response_json = response.json()
    return response_json['result']


def insertExecutionRecord(data):
    response = requests.get(RESTserver + "main?action=insert_execution", data=json.dumps(data))
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


# --------------------------------------------------
# IO Data
# --------------------------------------------------

def getIODataRecord(iod_id):
    response = requests.get(RESTserver + "main?action=get_iodata&id=" + str(iod_id))
    response_json = response.json()
    for record in response_json['result']:
        return record
    return None


def insertIODataRecord(data):
    response = requests.post(RESTserver + "main?action=insert_iodata", data=json.dumps(data))
    response_json = response.json()
    return response_json['result']


def setIOProperty(iod_id, name, attribute, value, obj_id):
    response = requests.get(RESTserver + "main?action=modify_iodata&id=" + str(iod_id) + "&name=" + str(name) + "&attribute=" + str(attribute) + "&value=" + str(value) + "&obj_id=" + str(obj_id))
    return response.status_code == 200


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


def uploadBinaryFileContentAndZip(binary_data):  # todo
    response = requests.post(RESTserver + "upload_and_zip", files={"myfile": binary_data})
    response_json = response.json()
    return response_json['result']


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


def setStatScheduler(runningTasks=None, scheduledTasks=None, load=None, processedTasks=None):
    if runningTasks is not None:
        response = requests.get(RESTserver + "main?action=set_scheduler_stat&key=scheduler.runningTasks&value=" + str(runningTasks))
    if scheduledTasks is not None:
        response = requests.get(RESTserver + "main?action=set_scheduler_stat&key=scheduler.scheduledTasks&value=" + str(scheduledTasks))
    if load is not None:
        response = requests.get(RESTserver + "main?action=set_scheduler_stat&key=scheduler.load&value=" + str(load))
    if processedTasks is not None:
        response = requests.get(RESTserver + "main?action=set_scheduler_stat&key=scheduler.processedTasks&value=" + str(processedTasks))
