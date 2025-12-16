import pydantic
import json
import datetime
import re
import requests
from .. import models
from .client_util import *
from rich import print_json
from rich.pretty import pprint
from typing import List, Optional, Tuple

thisDir = os.path.dirname(os.path.abspath(__file__))

# --------------------------------------------------
# Authentication
# --------------------------------------------------

API_CREDENTIALS_FILE = os.environ.get('API_CREDENTIALS_FILE', None)

api_credentials = {'username': '', 'password': ''}
if API_CREDENTIALS_FILE is not None and os.path.exists(API_CREDENTIALS_FILE):
    with open(API_CREDENTIALS_FILE) as json_data_file:
        credentials = json.load(json_data_file)
        api_credentials = {'username': credentials['username'], 'password': credentials['password']}

bearer_token: dict[str,Any] = {}
bearer_token_expires_at: float = 0

def getAuthToken() -> dict[str,Any] | None:
    if API_CREDENTIALS_FILE is None or (api_credentials['username']=='' and api_credentials['password']==''):
        return None

    global bearer_token
    global bearer_token_expires_at
    time_now = datetime.datetime.now()
    time_secs = time_now.timestamp()
    if time_secs > bearer_token_expires_at - 10:
        try:
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept-Charset': 'UTF-8',
                'Accept': 'application/json',
            }
            response = rPost(f"api/login", data={
                "grant_type": "password",
                "username": api_credentials['username'],
                "password": api_credentials['password'],
            }, headers=headers)
            # print(response)
            bearer_token = response
            bearer_token_expires_at = 0
            if 'expires_at' in bearer_token:
                expiration = bearer_token['expires_at']
                dt_expiration = datetime.datetime.fromisoformat(expiration)
                bearer_token_expires_at = dt_expiration.timestamp()
        except Exception as e:
            print(f"Error obtaining auth token: {e}")
            return None

    elif time_secs > bearer_token_expires_at - 15*60:
        try:
            headers={
                'Content-Type': 'application/json',
                'Accept-Charset': 'UTF-8',
                'Accept': 'application/json',
                'Authorization': f"Bearer {bearer_token['access_token']}"
            }
            response = rPost(f"api/refresh_token", headers=headers)
            # print(response)
            bearer_token = response
            bearer_token_expires_at = 0
            if 'expires_at' in bearer_token:
                expiration = bearer_token['expires_at']
                dt_expiration = datetime.datetime.fromisoformat(expiration)
                bearer_token_expires_at = dt_expiration.timestamp()
        except Exception as e:
            print(f"Error refreshing auth token: {e}")
            return None

    return bearer_token

def getRequestHeaders(content_type: str | None = None) -> dict[str,str]:
    headers = {'Accept-Charset': 'UTF-8'}
    if content_type is not None:
        headers['Content-type'] = content_type
    token = getAuthToken()
    if token is not None:
        headers['Authorization'] = f"Bearer {token['access_token']}"
    return headers


# --------------------------------------------------
# Usecases
# --------------------------------------------------

def getUsecaseRecords():
    return [models.UseCase_Model.model_validate(rec) for rec in rGet("api/usecases", headers=getRequestHeaders())['collection']]

def getUsecaseRecord(ucid):
    return rGet(f"api/usecases/{ucid}", headers=getRequestHeaders())['entity']

def insertUsecaseRecord(ucid, description):
    return rPost("api/usecases/", data=json.dumps({"ucid": ucid, "Description": description}), headers=getRequestHeaders())


# --------------------------------------------------
# Workflows
# --------------------------------------------------

def getWorkflowRecords() -> List[models.Workflow_Model]:
    return [models.Workflow_Model.model_validate(record) for record in rGet("workflows", headers=getRequestHeaders())['collection']]

def getWorkflowRecordsWithUsecase(usecase) -> List[models.Workflow_Model]:
    return [models.Workflow_Model.model_validate(record) for record in rGet(f"usecases/{usecase}/workflows", headers=getRequestHeaders())['collection']]

pydantic.validate_call(validate_return=True)
def getWorkflowRecord(wid, version: int) -> models.Workflow_Model:
    return models.Workflow_Model.model_validate(rGet(f"workflows/{wid}/version/{version}", headers=getRequestHeaders())['entity'])

pydantic.validate_call(validate_return=True)  # todo delete
def updateWorkflow(wf: models.Workflow_Model) -> models.Workflow_Model:
    return models.Workflow_Model.model_validate(rPatch("workflows/", data=wf.model_dump_json(), headers=getRequestHeaders())['entity'])

def postWorkflowFiles(usecaseid, path_workflow, paths_additional):
    files = {}
    if path_workflow is None or not os.path.exists(path_workflow):
        print(f"Error: Workflow file not found at {path_workflow}")
        return None
    files['workflow_file'] = (
        os.path.basename(path_workflow),
        open(path_workflow, 'rb'),
        'application/octet-stream'  # may be 'text/x-python'
    )

    additional_files_for_request = []
    for i, file_path in enumerate(paths_additional):
        if file_path and os.path.exists(file_path):
            filename = os.path.basename(file_path)
            mime_type = 'application/octet-stream'
            if filename.endswith('.txt'):
                mime_type = 'text/plain'
            elif filename.endswith('.json'):
                mime_type = 'application/json'
            elif filename.endswith('.py'):
                mime_type = 'text/x-python'
            elif filename.endswith('.md'):
                mime_type = 'text/markdown'

            additional_files_for_request.append(
                (filename, open(file_path, 'rb'), mime_type)
            )
        elif file_path:
            print(f"Warning: Additional file {file_path} not found, skipping.")

    if additional_files_for_request:
        files['additional_files'] = additional_files_for_request

    try:
        response = rPost(f"usecases/{usecaseid}/workflows", files=files, headers=getRequestHeaders())
        return response['wid']

    except requests.exceptions.ConnectionError as e:
        print(f"Error: Could not connect to FastAPI server. {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")



# --------------------------------------------------
# Executions
# --------------------------------------------------

pydantic.validate_call(validate_return=True)
def getExecutionRecords(workflow_id: str|None=None, workflow_version: int|None=None, label: str|None=None, num_limit: int|None=None, status: str|None=None) -> List[models.WorkflowExecution_Model]:
    query = "executions/?noparam"
    if workflow_version is not None and workflow_version<0: workflow_version=None
    for n,a in [('num_limit',num_limit),('label',label),('workflow_id',workflow_id),('workflow_version',workflow_version),('status',status)]:
        if a is not None: query += f"&{n}={str(a)}"
    return [models.WorkflowExecution_Model.model_validate(record) for record in rGet(query, headers=getRequestHeaders(), timeout=15)['collection']]


pydantic.validate_call(validate_return=True)
def getExecutionRecord(weid: str) -> models.WorkflowExecution_Model:
    return models.WorkflowExecution_Model.model_validate(rGet(f"executions/{weid}", headers=getRequestHeaders())['entity'])

def getScheduledExecutions(num_limit: int|None=None):
    return getExecutionRecords(status="Scheduled", num_limit=num_limit)

def getPendingExecutions(num_limit: int|None=None):
    return getExecutionRecords(status="Pending", num_limit=num_limit)

def scheduleExecution(execution_id: str):
    return rPatch(f"executions/{execution_id}/schedule", headers=getRequestHeaders())

def setExecutionParameter(execution_id: str, param: str, value: Any, val_type="str"):
    return rPatch(f"executions/{execution_id}/set_param", data=json.dumps({"key": str(param), "value": value}), headers=getRequestHeaders())

def setExecutionOntoBaseObjectID(execution_id, name, value):
    return rPatch(f"executions/{execution_id}/set_onto_base_object_id/", data=json.dumps({"name": str(name), "value": value}), headers=getRequestHeaders())

def setExecutionOntoBaseObjectIDMultiple(execution_id, data):
    return rPatch(f"executions/{execution_id}/set_onto_base_object_id_multiple/", data=json.dumps({"data": data}), headers=getRequestHeaders())

def setExecutionOntoBaseObjectIDs(execution_id, name, value):
    return rPatch(f"executions/{execution_id}/set_onto_base_object_ids/", data=json.dumps({"name": str(name), "value": value}), headers=getRequestHeaders())

def setExecutionAttemptsCount(execution_id, val):
    return setExecutionParameter(execution_id, "Attempts", str(val), "int")

def setExecutionStatus(execution_id: str, status: models.ExecutionStatus_Literal, revertPending=False):
    if status=='Created': setExecutionParameter(execution_id, "SubmittedDate", str(datetime.datetime.now()))
    elif status=='Pending' and not revertPending:
        setExecutionAttemptsCount(execution_id, 0)
    return setExecutionParameter(execution_id, "Status", status)

def createExecution(wid: str, version: int, ip: str, no_onto=False):
    wec=models.WorkflowExecutionCreate_Model(wid=wid,version=version,ip=ip,no_onto=no_onto)
    return rPost("executions/create/", data=wec.model_dump_json(), headers=getRequestHeaders())

pydantic.validate_call(validate_return=True)
def insertExecution(m: models.WorkflowExecution_Model):
    return rPost("executions/", data=m.model_dump_json(), headers=getRequestHeaders())

def getExecutionInputRecord(weid) -> List[models.IODataRecordItem_Model]:
    return [models.IODataRecordItem_Model.model_validate(record) for record in rGet(f"executions/{weid}/inputs/", headers=getRequestHeaders())]

def getExecutionOutputRecord(weid) -> List[models.IODataRecordItem_Model]:
    return [models.IODataRecordItem_Model.model_validate(record) for record in rGet(f"executions/{weid}/outputs/", headers=getRequestHeaders())]

def getExecutionInputRecordItem(weid, name, obj_id):
    io_data = getExecutionInputRecord(weid)
    for elem in io_data:
        if elem.Name == name and elem.ObjID == obj_id:
            return elem


def getExecutionOutputRecordItem(weid, name, obj_id):
    io_data = getExecutionOutputRecord(weid)
    for elem in io_data:
        if elem.Name == name and elem.ObjID == obj_id:
            return elem


# --------------------------------------------------
# IO Data
# --------------------------------------------------
pydantic.validate_call(validate_return=True)
def getIODataRecord(iod_id: str):
    return models.IODataRecord_Model.model_validate(rGet(f"iodata/{iod_id}", headers=getRequestHeaders()))

pydantic.validate_call(validate_return=True)
def insertIODataRecord(data: models.IODataRecord_Model):
    return rPost("iodata/", data=data.model_dump_json(), headers=getRequestHeaders())

def setExecutionInputLink(weid, name, obj_id, link_eid, link_name, link_obj_id):
    return rPatch(f"executions/{weid}/input_item/{name}/{obj_id}/", data=json.dumps({"link": {"ExecID": link_eid, "Name": link_name, "ObjID": link_obj_id}}), headers=getRequestHeaders())

# TODO: validate input
def setExecutionInputObject(weid, name, obj_id, object_dict):
    return rPatch(f"executions/{weid}/input_item/{name}/{obj_id}/", data=json.dumps({"object": object_dict}), headers=getRequestHeaders())

# TODO: validate input
def setExecutionOutputObject(weid, name, obj_id, object_dict):
    return rPatch(f"executions/{weid}/output_item/{name}/{obj_id}/", data=json.dumps({"object": object_dict}), headers=getRequestHeaders())

def getPropertyArrayData(file_id, i_start, i_count):  # may not be used
    return rGet(f"property_array_data/{file_id}/{i_start}/{i_count}/", headers=getRequestHeaders())


# --------------------------------------------------
# Files
# --------------------------------------------------
def getBinaryFileByID(fid) -> Tuple[bytes,str]:
    response = rGetRaw(f"file/{fid}", headers=getRequestHeaders())
    d = response.headers['Content-Disposition']
    filename = re.findall("filename=(.+)", d)[0]
    return response.content, filename

def uploadBinaryFile(binary_data) -> str:
    return rPost("file/", files={"file": binary_data}, headers=getRequestHeaders())


# --------------------------------------------------
# Stat
# --------------------------------------------------
def getStatus():
    return rGet("status/", headers=getRequestHeaders())

def getExecutionStatistics() -> models.MupifDBStatus_Model.ExecutionStatistics_Model:
    return models.MupifDBStatus_Model.ExecutionStatistics_Model.model_validate(rGet("execution_statistics/", headers=getRequestHeaders()))

def getStatScheduler():
    return models.MupifDBStatus_Model.Stat_Model.SchedulerStat_Model.model_validate(rGet("scheduler_statistics/", headers=getRequestHeaders()))

# # session is the requests module by default (one-off session for each request) but can be passed
# # a custom requests.Session() object with config such as retries and timeouts.
# # This feature is implemented only for setStatsScheduler to cleanly handle scheduler startup.
# def setStatScheduler(runningTasks=None, scheduledTasks=None, load=None, processedTasks=None, session: Any=requests):
#     if runningTasks is not None:
#         rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.runningTasks", "value": runningTasks}))
#     if scheduledTasks is not None:
#         rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.scheduledTasks", "value": scheduledTasks}))
#     if load is not None:
#         rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.load", "value": load}))
#     if processedTasks is not None:
#         rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.processedTasks", "value": processedTasks}))

# NOTE: session arg is discarded
def setStatScheduler(*args, session = None, **kw):
    return updateStatScheduler(*args,**kw)

def updateStatScheduler(runningTasks=None, scheduledTasks=None, load=None, processedTasks=None):
    if runningTasks is not None:
        rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.runningTasks", "value": runningTasks}), headers=getRequestHeaders())
    if scheduledTasks is not None:
        rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.scheduledTasks", "value": scheduledTasks}), headers=getRequestHeaders())
    if load is not None:
        rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.load", "value": load}), headers=getRequestHeaders())
    if processedTasks is not None:
        rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.processedTasks", "value": processedTasks}), headers=getRequestHeaders())


# --------------------------------------------------
# Settings
# --------------------------------------------------

def getSettings(maybe_init_db: bool=False):
    if maybe_init_db: rGet("database/maybe_init", headers=getRequestHeaders())
    return rGet("settings", headers=getRequestHeaders())


# --------------------------------------------------
# EDM
# --------------------------------------------------

def getEDMDataArray(DBName, Type):
    response = rGet(f"EDM/{DBName}/{Type}", headers=getRequestHeaders())
    return response.json()

def getEDMData(DBName, Type, ID, path):
    if ID == '' or ID is None: return None
    return rGet(f"EDM/{DBName}/{Type}/{ID}/?path={path}", headers=getRequestHeaders())


def setEDMData(DBName, Type, ID, path, data):
    return rPatch(f"EDM/{DBName}/{Type}/{ID}", data=json.dumps({"path": str(path), "data": data}), headers=getRequestHeaders())


def createEDMData(DBName, Type, data):
    return rPost(f"EDM/{DBName}/Type", data=json.dumps(data), headers=getRequestHeaders())


def cloneEDMData(DBName, Type, ID, shallow=[]):
    return rGet(f"EDM/{DBName}/{Type}/{ID}/clone", params={"shallow": ' '.join(shallow)}, headers=getRequestHeaders())


def getSafeLinks(DBName, Type, ID, paths=[]):
    return rGet(f"EDM/{DBName}/{Type}/{ID}/safe-links", params={"paths": ' '.join(paths)}, headers=getRequestHeaders())


def getEDMEntityIDs(DBName, Type, filter=None):
    return rPut(f"EDM/{DBName}/{Type}/find", data=json.dumps({"filter": (filter if filter else {})}), headers=getRequestHeaders())


def uploadEDMBinaryFile(DBName, binary_data) -> str:
    return rPost(f"EDM/{DBName}/blob/upload", files={"blob": binary_data}, headers=getRequestHeaders())


def getEDMBinaryFileByID(DBName, fid) -> Tuple[bytes,str]:
    response = rGetRaw(f"EDM/{DBName}/blob/{fid}", headers=getRequestHeaders())
    d = response.headers['Content-Disposition']
    filename = re.findall("filename=(.+)", d)[0]
    return response.content, filename

