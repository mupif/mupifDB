import pydantic
import json
import datetime
import re
import requests
from .. import models
from .client_util import *
from rich import print_json
from rich.pretty import pprint
from typing import List,Optional


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

def getWorkflowRecords() -> List[models.Workflow_Model]:
    response = rGet("workflows/")
    return [models.Workflow_Model.model_validate(record) for record in response.json()]

def getWorkflowRecordsWithUsecase(usecase) -> List[models.Workflow_Model]:
    response = rGet(f"usecases/{usecase}/workflows")
    return [models.Workflow_Model.model_validate(record) for record in response.json()]

@pydantic.validate_call
def getWorkflowRecord(wid: str) -> models.Workflow_Model:
    response = rGet(f"workflows/{wid}")
    return models.Workflow_Model.model_validate(response.json())

@pydantic.validate_call
def insertWorkflow(wf: models.Workflow_Model):
    response = rPost("workflows/", data=wf.model_dump_json())
    return response.json()

@pydantic.validate_call
def updateWorkflow(wf: models.Workflow_Model) -> models.Workflow_Model:
    response = rPatch("workflows/", data=wf.model_dump_json())
    return models.Workflow_Model.model_validate(response.json())


@pydantic.validate_call
def getWorkflowRecordGeneral(wid, version: int) -> models.Workflow_Model:
    workflow_newest = getWorkflowRecord(wid)
    if workflow_newest is not None:
        if workflow_newest.Version == version or version == -1:
            return workflow_newest
    return getWorkflowRecordFromHistory(wid, version)


# --------------------------------------------------
# Workflows history
# --------------------------------------------------
@pydantic.validate_call
def getWorkflowRecordFromHistory(wid: str, version: int) -> models.Workflow_Model:
    response = rGet(f"workflows_history/{wid}/{version}")
    return models.Workflow_Model.model_validate(response.json())

@pydantic.validate_call
def insertWorkflowHistory(wf: models.Workflow_Model):
    response = rPost("workflows_history/", data=wf.model_dump_json())
    return response.json()


# --------------------------------------------------
# Executions
# --------------------------------------------------

@pydantic.validate_call
def getExecutionRecords(workflow_id: str|None=None, workflow_version: int|None=None, label: str|None=None, num_limit: int|None=None, status: str|None=None) -> List[models.WorkflowExecution_Model]:
    query = "executions/?noparam"
    if workflow_version is not None and workflow_version<0: workflow_version=None
    for n,a in [('num_limit',num_limit),('label',label),('workflow_id',workflow_id),('workflow_version',workflow_version),('status',status)]:
        if a is not None: query += f"&{n}={str(a)}"
    response = rGet(query)
    return [models.WorkflowExecution_Model.model_validate(record) for record in response.json()]


@pydantic.validate_call
def getExecutionRecord(weid: str) -> models.WorkflowExecution_Model:
    response = rGet(f"executions/{weid}")
    return models.WorkflowExecution_Model.model_validate(response.json())

def getScheduledExecutions(num_limit: int|None=None):
    return getExecutionRecords(status="Scheduled", num_limit=num_limit)

def getPendingExecutions(num_limit: int|None=None):
    return getExecutionRecords(status="Pending", num_limit=num_limit)

def scheduleExecution(execution_id: str):
    response = rPatch(f"executions/{execution_id}/schedule")
    return response.json()

def setExecutionParameter(execution_id: str, param: str, value: Any, val_type="str"):
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


def setExecutionStatus(execution_id: str, status: models.ExecutionStatus_Literal, revertPending=False):
    if status=='Created': setExecutionParameter(execution_id, "SubmittedDate", str(datetime.datetime.now()))
    elif status=='Pending' and not revertPending:
        setExecutionParameter(execution_id, "SubmittedDate", str(datetime.datetime.now()))
        setExecutionAttemptsCount(execution_id, 0)
    elif status=='Running':
        setExecutionParameter(execution_id, "StartDate", str(datetime.datetime.now()))
    elif status in ('Finished','Failed'):
        setExecutionParameter(execution_id, "EndDate", str(datetime.datetime.now()))
    return setExecutionParameter(execution_id, "Status", status)


def createExecution(wid: str, version: int, ip: str, no_onto=False):
    wec=models.WorkflowExecutionCreate_Model(wid=wid,version=version,ip=ip,no_onto=no_onto)
    response = rPost("executions/create/", data=wec.model_dump_json())

    return response.json()

@pydantic.validate_call
def insertExecution(m: models.WorkflowExecution_Model):
    response = rPost("executions/", data=m.model_dump_json())
    return response.json()

def getExecutionInputRecord(weid) -> List[models.IODataRecordItem_Model]:
    response = rGet(f"executions/{weid}/inputs/")
    return [models.IODataRecordItem_Model.model_validate(record) for record in response.json()]

def getExecutionOutputRecord(weid) -> List[models.IODataRecordItem_Model]:
    response = rGet(f"executions/{weid}/outputs/")
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
    return models.IODataRecord_Model.model_validate(response.json())

@pydantic.validate_call
def insertIODataRecord(data: models.IODataRecord_Model):
    response = rPost("iodata/", data=data.model_dump_json())
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

def getExecutionStatistics() -> models.MupifDBStatus_Model.ExecutionStatistics_Model:
    response = rGet("execution_statistics/")
    return models.MupifDBStatus_Model.ExecutionStatistics_Model.model_validate(response.json())

def getStatScheduler():
    return models.MupifDBStatus_Model.Stat_Model.SchedulerStat_Model.model_validate(rGet("scheduler_statistics/").json())

# session is the requests module by default (one-off session for each request) but can be passed 
# a custom requests.Session() object with config such as retries and timeouts.
# This feature is implemented only for setStatsScheduler to cleanly handle scheduler startup.
def setStatScheduler(runningTasks=None, scheduledTasks=None, load=None, processedTasks=None, session: Any=requests):
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

def getSettings(maybe_init_db: bool=False):
    if maybe_init_db: rGet("database/maybe_init")
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

