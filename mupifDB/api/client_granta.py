import tempfile
import json
import sys
import os
import logging
import importlib
import datetime
from typing import List,Optional,Literal,Any
from typing_extensions import ParamSpec

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from requests.auth import HTTPBasicAuth

from .client_util import rGet, rPost, rPatch, rPut, rDelete, api_type
from .. import table_structures, models

granta_credentials = {'username': '', 'password': ''}
# RESTserver = 'https://musicode.grantami.com/musicode/api/'
with open("/var/lib/mupif/persistent/granta_api_login.json") as json_data_file:
    credentials = json.load(json_data_file)
    granta_credentials = {'username': credentials['username'], 'password': credentials['password']}

bearer_token: dict[str,Any] = {}
bearer_token_expires_at: float = 0

def getAuthToken() -> dict[str,Any]:
    global bearer_token
    global bearer_token_expires_at
    time_now = datetime.datetime.now()
    time_secs = time_now.timestamp()
    if time_secs > bearer_token_expires_at - 10:
        URL = 'https://auth.musicode.cloud/realms/musicode/protocol/openid-connect/token'
        CLIENT_ID = granta_credentials['username']
        CLIENT_SECRET = granta_credentials['password']
        client = BackendApplicationClient(client_id=CLIENT_ID)
        oauth = OAuth2Session(client=client)
        auth = HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
        bearer_token = oauth.fetch_token(token_url=URL, auth=auth)
        bearer_token_expires_at = bearer_token['expires_at']
    return bearer_token

def getGrantaHeaders(set: bool=False) -> dict[str,str]:
    return {'content-type': 'application/json', 'Accept-Charset': 'UTF-8', 'Authorization': f'Bearer {getAuthToken()["access_token"]}'}|({'charset': 'UTF-8', 'accept': 'application/json',} if set else {})


def getUsercaseRecords(): return []
def getUsecaseRecord(ucid: str): return None
def insertUsecaseRecord(ucid, description): return None
# workflow
def getWorkflowRecords(): return[]
def getWorkflowRecordsWithUsecase(usecase): return[]
def getWorkflowRecord(wid: str): return None
def insertWorkflow(wf: models.Workflow_Model): return None
def updateWorkflow(wf: models.Workflow_Model): return None
def getWorkflowRecordGeneral(wid, version: int): return _getGrantaWorkflowRecordGeneral(wid,version)
def getWorkflowRecordFromHistory(wid, version): return None
def insertWorkflowHistory(wf: models.Workflow_Model): return None
# execution
def getExecutionRecords(*args, **kw): return _getGrantaExecutionRecords(*args,**kw)
def getExecutionRecord(weid:str): return _getGrantaExecutionRecord(weid)
def getScheduledExecutions(num_limit=None): return []
def getPendingExecutions(num_limit=None): return _getGrantaPendingExecutions(num_limit)
def scheduleExecution(execution_id): return None
def setExecutionParameter(*args,**kw): _setGrantaExecutionParameter(*args,**kw)
def setExecutionOntoBaseObjectID(*args,**kw): return None
def setExecutionOntoBaseObjectIDMultiple(*args,**kw): return None
def setExecutionOntoBaseObjectIDs(*args,**kw): return None
def setExecutionAttemptsCount(*args,**kw): return None
def setExecutionStatusScheduled(execution_id): return _setGrantaExecutionStatus(execution_id,'Scheduled')
def setExecutionStatusCreated(execution_id): return None
def setExecutionStatusPending(execution_id): return _setGrantaExecutionStatus(execution_id,'Ready')
def setExecutionStatusRunning(execution_id): return _setGrantaExecutionStatus(execution_id,'Running')
def setExecutionStatusFinished(execution_id): return _setGrantaExecutionStatus(execution_id,'Completed, to be reviewed')
def setExecutionStatusFailed(execution_id): return _setGrantaExecutionStatus(execution_id,'Failed')
def createExecution(*args,**kw): return None
def insertExecution(*args,**kw): return None
def getExecutionInputRecord(weid): return None
def getExecutionOutputRecord(weid): return None
def getExecutionInputRecordItem(*args,**kw): return None
def getExecutionOutputRecordItem(*args,**kw): return None
# i/o
def getIODataRecord(*args,**kw): return None
def insertIODataRecord(*args,**kw): return None
def setExecutionInputLink(*args,**kw): return None
def setExecutionInputObject(*args,**kw): return None
def setExecutionOutputObject(*args,**kw): return None
def getPropertyArrayData(*args,**kw): return None
def getBinaryFileByID(fid): return _getGrantaBinaryFileByID(fid)
def uploadBinaryFile(binary_data): return _uploadGrantaBinaryFile(binary_data)
# stats
def getStatus(): return None
def getExecutionStatistics(): return {'totalExecutions': 0,'finishedExecutions': 0,'failedExecutions': 0,'createdExecutions': 0,'pendingExecutions': 0,'scheduledExecutions': 0,'runningExecutions': 0}
def getStatScheduler(): return {"runningTasks": 0, "scheduledTasks": 0, "load": 0, "processedTasks": 0}
def setStatScheduler(*args,**kw): return None
def updateStatScheduler(*args,**kw): return None
def getSettings(): return {}


def fix_json(val: str) -> str:

    import re
    val = re.sub(r",[ \t\r\n]+}", "}", val)
    val = re.sub(r",[ \t\r\n]+\]", "]", val)
    val = val.replace("False", "false").replace("True", "true")
    val
    return val


def _getGrantaWorkflowRecordGeneral(wid, version: int):
    r = rGet(f"templates/{wid}", headers=getGrantaHeaders())
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
            # print(md)
            workflow['metadata'] = md
            workflow['classname'] = md['ClassName']
            workflow['modulename'] = md['ModuleName']

        if gmd['name'] == 'workflow python file':
            fid = gmd['value']['url'].split('/')[-1]

    if fid:
        file, filename = _getGrantaBinaryFileByID(fid)
        workflow['GridFSID'] = fid
        workflow['modulename'] = filename.replace('.py', '')

    return workflow


def _getGrantaWorkflowMetadataFromDatabase(wid):
    workflow_record = _getGrantaWorkflowRecordGeneral(wid, -1)
    return workflow_record.get('metadata', {})


def _getGrantaWorkflowMetadataFromFile(wid, key=None):
    workflow_record = _getGrantaWorkflowRecordGeneral(wid, -1)
    if workflow_record.get('GridFSID', None) is not None:
        with tempfile.TemporaryDirectory(dir='/tmp', prefix='mupifDB') as tempDir:
            try:
                fc, fn = _getGrantaBinaryFileByID(workflow_record['GridFSID'])
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

    r = rGet(f"executions/{eid}/inputs", headers=getGrantaHeaders())

    for inp in r.json():
        if name == inp['name']:
            if inp['type'] == 'float':
                # fint units first :(
                execution_record = _getGrantaExecutionRecord(eid)
                w_inputs = _getGrantaWorkflowMetadataFromDatabase(execution_record['WorkflowID']).get('Inputs', [])
                units = ''
                data_id = 'Unknown'
                valueType = 'Unknown'
                for w_i in w_inputs:
                    if w_i['Name'] == name:
                        units = w_i.get('Units', 'Unknown')
                        data_id = w_i.get('Type_ID', 'Unknown')
                        valueType = w_i.get('ValueType', 'Scalar')
                return {
                    'Compulsory': True,
                    'Description': '',
                    'Name': inp['name'],
                    'ObjID': inp['name'],
                    'Type': 'mupif.Property',
                    'TypeID': data_id,
                    'Units': units,  # todo
                    'ValueType': valueType,
                    'Link': {},
                    'Object': {
                        'ClassName': 'ConstantProperty',
                        'ValueType': valueType,
                        'DataID': data_id.replace('mupif.DataID.', ''),
                        'Unit': units,  # todo
                        'Value': inp['value'],
                        'Time': None
                    }
                }

            if inp['type'] == 'str':
                # fint units first :(
                execution_record = _getGrantaExecutionRecord(eid)
                w_inputs = _getGrantaWorkflowMetadataFromDatabase(execution_record['WorkflowID']).get('Inputs', [])
                units = ''
                data_id = 'Unknown'
                valueType = 'Unknown'
                for w_i in w_inputs:
                    if w_i['Name'] == name:
                        units = w_i.get('Units', 'Unknown')
                        data_id = w_i.get('Type_ID', 'Unknown')
                        valueType = w_i.get('ValueType', 'Scalar')
                return {
                    'Compulsory': True,
                    'Description': '',
                    'Name': inp['name'],
                    'ObjID': inp['name'],
                    'Type': 'mupif.String',
                    'TypeID': data_id,
                    'Units': units,  # todo
                    'ValueType': valueType,
                    'Link': {},
                    'Object': {
                        'ClassName': 'String',
                        'DataID': data_id.replace('mupif.DataID.', ''),
                        'Value': inp['value'],
                    'ValueType': valueType
                    }
                }

            if inp['type'] == 'hyperlink':
                execution_record = _getGrantaExecutionRecord(eid)
                w_inputs = _getGrantaWorkflowMetadataFromDatabase(execution_record['WorkflowID']).get('Inputs', [])
                units = ''
                data_id = 'Unknown'
                valueType = 'Unknown'
                obj_type = ''
                for w_i in w_inputs:
                    if w_i['Name'] == name:
                        units = w_i.get('Units', 'Unknown')
                        data_id = w_i.get('Type_ID', 'Unknown')
                        valueType = w_i.get('ValueType', 'Scalar')
                        obj_type = w_i['Type']

                if obj_type == 'mupif.HeavyStruct':
                    return {
                        'Compulsory': True,
                        'Description': '',
                        'Name': inp['name'],
                        'ObjID': inp['name'],
                        'Type': 'mupif.HeavyStruct',
                        'TypeID': data_id,
                        'Units': '',
                        'ValueType': valueType,
                        'Link': {},
                        'Object': {
                            'FileID': inp['value']['url'].split('/')[-1]
                        }
                    }

                if obj_type == 'mupif.PyroFile':
                    return {
                        'Compulsory': True,
                        'Description': '',
                        'Name': inp['name'],
                        'ObjID': inp['name'],
                        'Type': 'mupif.PyroFile',
                        'TypeID': data_id,
                        'Units': '',
                        'ValueType': valueType,
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
                        'TypeID': data_id,
                        'Units': '',
                        'ValueType': valueType,
                        'Link': {},
                        'Object': {
                            'FileID': inp['value'].split('/')[-1]
                        }
                    }
    return None


def _getGrantaExecutionRecords(workflow_id=None, workflow_version=None, label=None, num_limit=None, status=None): #  -> List[models.WorkflowExecution_Model]: 
    assert api_type == 'granta'
    r = rGet("executions/", headers=getGrantaHeaders())
    res = []
    for ex in r.json():
        execution = table_structures.extendRecord({}, table_structures.tableExecution)
        execution['_id'] = ex['guid']
        execution['WorkflowID'] = ex['template_guid']
        execution['WorkflowVersion'] = -1
        execution['Status'] = {'Ready':'Pending','On-going':'Running','Completed':'Finished','Completed, to be reviewed':'Finished','Completed & reviewed':'Finished','Cancelled':'Failed'}.get(ex['status'],ex['status'])
        execution['Task_ID'] = ''
        res.append(execution)
    return res


def _getGrantaExecutionRecord(weid: str):
    assert api_type == 'granta'
    r = rGet(f"executions/{weid}", headers=getGrantaHeaders())
    r_json = r.json()
    execution = table_structures.extendRecord({}, table_structures.tableExecution)
    execution['_id'] = r_json['guid']
    execution['WorkflowID'] = r_json['template_guid']
    execution['WorkflowVersion'] = -1
    execution['Status'] = 'unknown'

    execution['Status'] = {'Ready':'Pending','On-going':'Running','Completed':'Finished','Completed, to be reviewed':'Finished','Completed & reviewed':'Finished','Cancelled':'Failed'}.get(r_json['status'],r_json['status'])

    execution['Task_ID'] = ''
    return execution

def _setGrantaExecutionParameter(execution_id, param, value, val_type="str"):
    assert api_type == 'granta'
    if param == 'ExecutionLog':
        token = getAuthToken()
        headers = {'content-type': 'application/json', 'charset': 'UTF-8', 'accept': 'application/json', 'Accept-Charset': 'UTF-8', 'Authorization': f'Bearer {token["access_token"]}'}
        newdata = {"logs": {"url": "https://musicode.grantami.com/musicode/filestore/%s" % str(value), "description": None}}
        r = rPatch(f"executions/{execution_id}", headers=headers, data=json.dumps(newdata))
        if r.status_code == 200:
            return True
    return None


def _getGrantaPendingExecutions(num_limit=None):
    assert api_type == 'granta'
    r = rGet("executions/?status=Ready", headers=getGrantaHeaders())
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


def _setGrantaExecutionResults(eid, val_list):
    newdata = {"results": val_list}
    r = rPatch(f"executions/{eid}", headers=getGrantaHeaders(set=True), data=json.dumps(newdata))
    if r.status_code == 200:
        return True
    return False


def _setGrantaExecutionStatus(eid, val):
    token = getAuthToken()
    headers = {'content-type': 'application/json', 'charset': 'UTF-8', 'accept': 'application/json', 'Accept-Charset': 'UTF-8', 'Authorization': f'Bearer {token["access_token"]}'}
    newdata = {"status": str(val)}
    r = rPatch(f"executions/{eid}", headers=getGrantaHeaders(set=True), data=json.dumps(newdata))
    if r.status_code == 200:
        return True
    return False


def _getGrantaBinaryFileByID(fid):
    assert api_type == 'granta'
    # this is .../filestore instead of ..../api, so the ../filestore should do the trick
    response = rGet(f"../filestore/{fid}", headers={'Authorization': f'Bearer {getAuthToken()["access_token"]}'}, allow_redirects=True)
    return response.content, response.headers['content-disposition'].split('filename=')[1].replace('"', '')

def _uploadGrantaBinaryFile(binary_data):
    assert api_type == 'granta'
    response = rPost("../filestore", headers={'Authorization': f'Bearer {getAuthToken()["access_token"]}'}, files={"file": binary_data})
    return response.json()['guid']

