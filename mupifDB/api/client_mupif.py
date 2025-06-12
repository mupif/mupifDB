import pydantic
import json
import datetime
import re
import requests
from .. import models
from .client_util import *
from rich import print_json
from rich.pretty import pprint
from typing import List,Optional,Tuple


def getUsecaseRecords():
    return [models.UseCase_Model.model_validate(rec) for rec in rGet("usecases/")]

def getUsecaseRecord(ucid):
    return rGet(f"usecases/{ucid}")

def insertUsecaseRecord(ucid, description):
    return rPost("usecases/", data=json.dumps({"ucid": ucid, "description": description}))


# --------------------------------------------------
# Workflows
# --------------------------------------------------

def getWorkflowRecords() -> List[models.Workflow_Model]:
    return [models.Workflow_Model.model_validate(record) for record in rGet("workflows/")]

def getWorkflowRecordsWithUsecase(usecase) -> List[models.Workflow_Model]:
    return [models.Workflow_Model.model_validate(record) for record in rGet(f"usecases/{usecase}/workflows")]

pydantic.validate_call(validate_return=True)
def getWorkflowRecord(wid, version: int) -> models.Workflow_Model:
    return models.Workflow_Model.model_validate(rGet(f"workflows/{wid}/version/{version}"))

pydantic.validate_call(validate_return=True)  # todo delete
def insertWorkflow(wf: models.Workflow_Model):
    return rPost("workflows/", data=wf.model_dump_json())

pydantic.validate_call(validate_return=True)  # todo delete
def updateWorkflow(wf: models.Workflow_Model) -> models.Workflow_Model:
    return models.Workflow_Model.model_validate(rPatch("workflows/", data=wf.model_dump_json()))

pydantic.validate_call(validate_return=True)  # todo delete
def insertWorkflowHistory(wf: models.Workflow_Model):
    return rPost("workflows_history/", data=wf.model_dump_json())



# --------------------------------------------------
# Executions
# --------------------------------------------------

pydantic.validate_call(validate_return=True)
def getExecutionRecords(workflow_id: str|None=None, workflow_version: int|None=None, label: str|None=None, num_limit: int|None=None, status: str|None=None) -> List[models.WorkflowExecution_Model]:
    query = "executions/?noparam"
    if workflow_version is not None and workflow_version<0: workflow_version=None
    for n,a in [('num_limit',num_limit),('label',label),('workflow_id',workflow_id),('workflow_version',workflow_version),('status',status)]:
        if a is not None: query += f"&{n}={str(a)}"
    return [models.WorkflowExecution_Model.model_validate(record) for record in rGet(query, timeout=15)]


pydantic.validate_call(validate_return=True)
def getExecutionRecord(weid: str) -> models.WorkflowExecution_Model:
    return models.WorkflowExecution_Model.model_validate(rGet(f"executions/{weid}"))

def getScheduledExecutions(num_limit: int|None=None):
    return getExecutionRecords(status="Scheduled", num_limit=num_limit)

def getPendingExecutions(num_limit: int|None=None):
    return getExecutionRecords(status="Pending", num_limit=num_limit)

def scheduleExecution(execution_id: str):
    return rPatch(f"executions/{execution_id}/schedule")

def setExecutionParameter(execution_id: str, param: str, value: Any, val_type="str"):
    return rPatch(f"executions/{execution_id}", data=json.dumps({"key": str(param), "value": value}))

def setExecutionOntoBaseObjectID(execution_id, name, value):
    return rPatch(f"executions/{execution_id}/set_onto_base_object_id/", data=json.dumps({"name": str(name), "value": value}))

def setExecutionOntoBaseObjectIDMultiple(execution_id, data):
    return rPatch(f"executions/{execution_id}/set_onto_base_object_id_multiple/", data=json.dumps({"data": data}))

def setExecutionOntoBaseObjectIDs(execution_id, name, value):
    return rPatch(f"executions/{execution_id}/set_onto_base_object_ids/", data=json.dumps({"name": str(name), "value": value}))

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
    return rPost("executions/create/", data=wec.model_dump_json())

pydantic.validate_call(validate_return=True)
def insertExecution(m: models.WorkflowExecution_Model):
    return rPost("executions/", data=m.model_dump_json())

def getExecutionInputRecord(weid) -> List[models.IODataRecordItem_Model]:
    return [models.IODataRecordItem_Model.model_validate(record) for record in rGet(f"executions/{weid}/inputs/")]

def getExecutionOutputRecord(weid) -> List[models.IODataRecordItem_Model]:
    return [models.IODataRecordItem_Model.model_validate(record) for record in rGet(f"executions/{weid}/outputs/")]

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
    return models.IODataRecord_Model.model_validate(rGet(f"iodata/{iod_id}"))

pydantic.validate_call(validate_return=True)
def insertIODataRecord(data: models.IODataRecord_Model):
    return rPost("iodata/", data=data.model_dump_json())

def setExecutionInputLink(weid, name, obj_id, link_eid, link_name, link_obj_id):
    return rPatch(f"executions/{weid}/input_item/{name}/{obj_id}/", data=json.dumps({"link": {"ExecID": link_eid, "Name": link_name, "ObjID": link_obj_id}}))

# TODO: validate input
def setExecutionInputObject(weid, name, obj_id, object_dict):
    return rPatch(f"executions/{weid}/input_item/{name}/{obj_id}/", data=json.dumps({"object": object_dict}))

# TODO: validate input
def setExecutionOutputObject(weid, name, obj_id, object_dict):
    return rPatch(f"executions/{weid}/output_item/{name}/{obj_id}/", data=json.dumps({"object": object_dict}))

def getPropertyArrayData(file_id, i_start, i_count):  # may not be used
    return rGet(f"property_array_data/{file_id}/{i_start}/{i_count}/")


# --------------------------------------------------
# Files
# --------------------------------------------------
def getBinaryFileByID(fid) -> Tuple[bytes,str]:
    response = rGetRaw(f"file/{fid}")
    d = response.headers['Content-Disposition']
    filename = re.findall("filename=(.+)", d)[0]
    return response.content, filename

def uploadBinaryFile(binary_data) -> str:
    return rPost("file/", files={"file": binary_data})


# --------------------------------------------------
# Stat
# --------------------------------------------------
def getStatus():
    return rGet("status/")

def getExecutionStatistics() -> models.MupifDBStatus_Model.ExecutionStatistics_Model:
    return models.MupifDBStatus_Model.ExecutionStatistics_Model.model_validate(rGet("execution_statistics/"))

def getStatScheduler():
    return models.MupifDBStatus_Model.Stat_Model.SchedulerStat_Model.model_validate(rGet("scheduler_statistics/"))

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
        rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.runningTasks", "value": runningTasks}))
    if scheduledTasks is not None:
        rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.scheduledTasks", "value": scheduledTasks}))
    if load is not None:
        rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.load", "value": load}))
    if processedTasks is not None:
        rPatch("scheduler_statistics/", data=json.dumps({"key": "scheduler.processedTasks", "value": processedTasks}))


# --------------------------------------------------
# Settings
# --------------------------------------------------

def getSettings(maybe_init_db: bool=False):
    if maybe_init_db: rGet("database/maybe_init")
    return rGet("settings")


# --------------------------------------------------
# EDM
# --------------------------------------------------

def getEDMDataArray(DBName, Type):
    response = rGet(f"EDM/{DBName}/{Type}")
    return response.json()

def getEDMData(DBName, Type, ID, path):
    if ID == '' or ID is None: return None
    return rGet(f"EDM/{DBName}/{Type}/{ID}/?path={path}")


def setEDMData(DBName, Type, ID, path, data):
    return rPatch(f"EDM/{DBName}/{Type}/{ID}", data=json.dumps({"path": str(path), "data": data}))


def createEDMData(DBName, Type, data):
    return rPost(f"EDM/{DBName}/Type", data=json.dumps(data))


def cloneEDMData(DBName, Type, ID, shallow=[]):
    return rGet(f"EDM/{DBName}/{Type}/{ID}/clone", params={"shallow": ' '.join(shallow)})


def getSafeLinks(DBName, Type, ID, paths=[]):
    return rGet(f"EDM/{DBName}/{Type}/{ID}/safe-links", params={"paths": ' '.join(paths)})


def getEDMEntityIDs(DBName, Type, filter=None):
    return rPut(f"EDM/{DBName}/{Type}/find", data=json.dumps({"filter": (filter if filter else {})}))


def uploadEDMBinaryFile(DBName, binary_data) -> str:
    return rPost(f"EDM/{DBName}/blob/upload", files={"blob": binary_data})


def getEDMBinaryFileByID(DBName, fid) -> Tuple[bytes,str]:
    response = rGetRaw(f"EDM/{DBName}/blob/{fid}")
    d = response.headers['Content-Disposition']
    filename = re.findall("filename=(.+)", d)[0]
    return response.content, filename

