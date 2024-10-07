import pydantic
import json
import datetime
import re
import requests
from .. import models
from .client_util import *
from rich import print_json
from typing import List,Optional

@pydantic.validate_call
def getUsecaseRecords():
    return [models.UseCase_Model.model_validate(rec) for rec in rGet("usecases/").json()]

def getUsecaseRecord(ucid):
    response = rGet(f"usecases/{ucid}")
    return response.json()

def insertUsecaseRecord(ucid, description):
    response = rPost("usecases/", data=json.dumps({"ucid": ucid, "description": description}))
    return response.json()


# --------------------------------------------------
# Workflows
# --------------------------------------------------

def getWorkflowRecords():
    response = rGet("workflows/")
    return [models.Workflow_Model.model_validate(record) for record in response.json()]

def getWorkflowRecordsWithUsecase(usecase):
    data = []
    response = rGet(f"usecases/{usecase}/workflows")
    for record in response.json():
        data.append(record)
    return data

@pydantic.validate_call
def getWorkflowRecord(wid: str) -> models.Workflow_Model|None:
    response = rGet(f"workflows/{wid}")
    if response.json() is None: return None
    # print(response)
    #if response.json() is None: return None
    print_json(data=response.json())
    return models.Workflow_Model.model_validate(response.json())

@pydantic.validate_call
def insertWorkflow(wf: models.Workflow_Model):
    # print_json(data=wf.model_dump())
    response = rPost("workflows/", data=json.dumps({"entity": wf.model_dump()}))
    return response.json()

@pydantic.validate_call
def updateWorkflow(wf: models.Workflow_Model):
    response = rPatch("workflows/", data=json.dumps({"entity": wf.model_dump()}))
    return response.json()


@pydantic.validate_call
def getWorkflowRecordGeneral(wid, version: int) -> models.Workflow_Model:
    workflow_newest = getWorkflowRecord(wid)
    if workflow_newest is not None:
        if workflow_newest.Version == version or version is None: # == -1 or version == None:
            return workflow_newest
    return getWorkflowRecordFromHistory(wid, version)


# --------------------------------------------------
# Workflows history
# --------------------------------------------------
def getWorkflowRecordFromHistory(wid, version) -> models.Workflow_Model: 
    response = rGet(f"workflows_history/{wid}/{version}")
    return models.Workflow_Model.model_validate(response.json())

@pydantic.validate_call
def insertWorkflowHistory(wf: models.Workflow_Model):
    response = rPost("workflows_history/", data=json.dumps({"entity": wf.model_dump()}))
    return response.json()


# --------------------------------------------------
# Executions
# --------------------------------------------------

@pydantic.validate_call
def getExecutionRecords(workflow_id: str|None=None, workflow_version: int|None=None, label: str|None=None, num_limit: int|None=None, status: str|None=None) -> List[models.WorkflowExecutionRecord_Model]:
    query = "executions/?noparam"
    for n,a in [('num_limit',num_limit),('label',label),('workflow_id',workflow_id),('workflow_version',workflow_version),('status',status)]:
        if a is not None: query += f"&{n}={str(a)}"
    response = rGet(query)
    return [models.WorkflowExecutionRecord_Model.model_validate(record) for record in response.json()]


@pydantic.validate_call
def getExecutionRecord(weid: str) -> models.WorkflowExecutionRecord_Model:
    response = rGet(f"executions/{weid}")
    return models.WorkflowExecutionRecord_Model.model_validate(response.json())

def getScheduledExecutions(num_limit=None):
    return getExecutionRecords(status="Scheduled", num_limit=num_limit)

def getPendingExecutions(num_limit=None):
    return getExecutionRecords(status="Pending", num_limit=num_limit)

def scheduleExecution(execution_id):
    response = rPatch(f"executions/{execution_id}/schedule")
    return response.json()

def setExecutionParameter(execution_id, param, value, val_type="str"):
    response = rPatch(f"executions/{execution_id}", data=json.dumps({"key": str(param), "value": value}))
    return response.json()

def setExecutionOntoBaseObjectID(execution_id, name, value):
    response = rPatch(f"executions/{execution_id}/set_onto_base_object_id/", data=json.dumps({"name": str(name), "value": value}))
    if response.status_code == 200:
        return response.json()
    return None

def setExecutionOntoBaseObjectIDMultiple(execution_id, data):
    response = rPatch(f"executions/{execution_id}/set_onto_base_object_id_multiple/", data=json.dumps({"data": data}))
    if response.status_code == 200:
        return response.json()
    return None

def setExecutionOntoBaseObjectIDs(execution_id, name, value):
    response = rPatch(f"executions/{execution_id}/set_onto_base_object_ids/", data=json.dumps({"name": str(name), "value": value}))
    if response.status_code == 200:
        return response.json()
    return None

def setExecutionAttemptsCount(execution_id, val):
    return setExecutionParameter(execution_id, "Attempts", str(val), "int")


def setExecutionStatusScheduled(execution_id):
    return setExecutionParameter(execution_id, "Status", "Scheduled")

def setExecutionStatusCreated(execution_id):  # only reverted
    setExecutionParameter(execution_id, "SubmittedDate", "")
    return setExecutionParameter(execution_id, "Status", "Created")


def setExecutionStatusPending(execution_id, reverted=False):
    if reverted:
        pass
        # setExecutionParameter(execution_id, "StartDate", "")
    else:
        setExecutionParameter(execution_id, "SubmittedDate", str(datetime.datetime.now()))
        setExecutionAttemptsCount(execution_id, 0)
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

def createExecution(wid: str, version: int, ip: str, no_onto=False):
    wec=models.WorkflowExecutionCreate_Model(wid=wid,version=version,ip=ip,no_onto=no_onto)
    #print_json(data=wec.model_dump())
    response = rPost("executions/create/", data=wec.model_dump_json())
    return response.json()

def insertExecution(data):
    response = rPost("executions/", data=json.dumps({"entity": data}))
    return response.json()

def getExecutionInputRecord(weid):
    response = rGet(f"executions/{weid}/inputs/")
    return response.json()

def getExecutionOutputRecord(weid):
    response = rGet(f"executions/{weid}/outputs/")
    #print_json(data=response.json())
    return [models.IODataRecordItem_Model.model_validate(record) for record in response.json()]

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
@pydantic.validate_call
def getIODataRecord(iod_id: str):
    response = rGet(f"iodata/{iod_id}")
    return [models.IODataRecordItem_Model.model_validate(i) for i in response.json()]

def insertIODataRecord(data: models.IODataRecord_Model):
    response = rPost("iodata/", data=json.dumps({"entity": data.model_dump()}))
    return response.json()

def setExecutionInputLink(weid, name, obj_id, link_eid, link_name, link_obj_id):
    response = rPatch(f"executions/{weid}/input_item/{name}/{obj_id}/", data=json.dumps({"link": {"ExecID": link_eid, "Name": link_name, "ObjID": link_obj_id}}))
    return response.json()

def setExecutionInputObject(weid, name, obj_id, object_dict):
    response = rPatch(f"executions/{weid}/input_item/{name}/{obj_id}/", data=json.dumps({"object": object_dict}))
    return response.json()

def setExecutionOutputObject(weid, name, obj_id, object_dict):
    response = rPatch(f"executions/{weid}/input_item/{name}/{obj_id}/", data=json.dumps({"object": object_dict}))
    return response.json()

def getPropertyArrayData(file_id, i_start, i_count):  # may not be used
    response = rGet(f"property_array_data/{file_id}/{i_start}/{i_count}/")
    return response.json()


# --------------------------------------------------
# Files
# --------------------------------------------------
def getBinaryFileByID(fid):
    response = rGet(f"file/{fid}")
    d = response.headers['Content-Disposition']
    filename = re.findall("filename=(.+)", d)[0]
    return response.content, filename

def uploadBinaryFile(binary_data):
    response = rPost("file/", files={"file": binary_data})
    return response.json()


# --------------------------------------------------
# Stat
# --------------------------------------------------
def getStatus():
    response = rGet("status/")
    return response.json()

def getExecutionStatistics():
    response = rGet("execution_statistics/")
    return response.json()

def getStatScheduler():
    response = rGet("scheduler_statistics/")
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
    if runningTasks is not None:
        response = rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.runningTasks", "value": runningTasks}))
    if scheduledTasks is not None:
        response = rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.scheduledTasks", "value": scheduledTasks}))
    if load is not None:
        response = rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.load", "value": load}))
    if processedTasks is not None:
        response = rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.processedTasks", "value": processedTasks}))

def updateStatScheduler(runningTasks=None, scheduledTasks=None, load=None, processedTasks=None):
    if runningTasks is not None:
        response = rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.runningTasks", "value": runningTasks}))
    if scheduledTasks is not None:
        response = rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.scheduledTasks", "value": scheduledTasks}))
    if load is not None:
        response = rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.load", "value": load}))
    if processedTasks is not None:
        response = rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.processedTasks", "value": processedTasks}))


# --------------------------------------------------
# Settings
# --------------------------------------------------

def getSettings():
    response = rGet("settings")
    return response.json()


# --------------------------------------------------
# EDM
# --------------------------------------------------

def getEDMDataArray(DBName, Type):
    response = rGet(f"EDM/{DBName}/{Type}")
    return response.json()

def getEDMData(DBName, Type, ID, path):
    if ID == '' or ID is None:
        return None
    response = rGet(f"EDM/{DBName}/{Type}/{ID}/?path={path}")
    return response.json()


def setEDMData(DBName, Type, ID, path, data):
    response = rPatch(f"EDM/{DBName}/{Type}/{ID}", data=json.dumps({"path": str(path), "data": data}))
    return response.json()


def createEDMData(DBName, Type, data):
    response = rPost(f"EDM/{DBName}/Type", data=json.dumps(data))
    return response.json()


def cloneEDMData(DBName, Type, ID, shallow=[]):
    response = rGet(f"EDM/{DBName}/{Type}/{ID}/clone", params={"shallow": ' '.join(shallow)})
    return response.json()


def getSafeLinks(DBName, Type, ID, paths=[]):
    response = rGet(f"EDM/{DBName}/{Type}/{ID}/safe-links", params={"paths": ' '.join(paths)})
    return response.json()


def getEDMEntityIDs(DBName, Type, filter=None):
    response = rPut(f"EDM/{DBName}/{Type}/find", data=json.dumps({"filter": (filter if filter else {})}))
    return response.json()


def uploadEDMBinaryFile(DBName, binary_data):
    response = rPost(f"EDM/{DBName}/blob/upload", files={"blob": binary_data})
    return response.json()


def getEDMBinaryFileByID(DBName, fid):
    response = rGet(f"EDM/{DBName}/blob/{fid}")
    d = response.headers['Content-Disposition']
    filename = re.findall("filename=(.+)", d)[0]
    return response.content, filename

