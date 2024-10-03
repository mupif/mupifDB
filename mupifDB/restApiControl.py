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

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")

import pydantic
from mupifDB import models
from typing import List,Optional,Literal

from . import _granta

from rich import print_json
from rich.pretty import pprint


#

def Request(*, method, url, headers=None, auth=None, data=None, timeout=None, files={}, params={}, allow_redirects=True):
    if method == 'get':
        response = requests.get(url=url, timeout=timeout, headers=headers, auth=auth, data=data, params=params, allow_redirects=allow_redirects)
    elif method == 'post':
        response = requests.post(url=url, timeout=timeout, headers=headers, auth=auth, data=data, files=files, allow_redirects=allow_redirects)
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
        raise Exception(f'API returned code {response.status_code}, message {response.reason} ({url=}, {method=}, {data=}, {auth=}, {timeout=}, {files=}, {params=}, {allow_redirects=}')
    return None

def rGet(*, url, headers=None, auth=None, timeout=100, params={}, allow_redirects=True):
    return Request(method='get', url=url, headers=headers, auth=auth, timeout=timeout, params=params, allow_redirects=allow_redirects)

def rPost(*, url, headers=None, auth=None, data=None, timeout=100, files={}):
    return Request(method='post', url=url, headers=headers, auth=auth, timeout=timeout, data=data, files=files)

def rPatch(*, url, headers=None, auth=None, data=None, timeout=100):
    return Request(method='patch', url=url, headers=headers, auth=auth, timeout=timeout, data=data)

def rPut(*, url, headers=None, auth=None, data=None, timeout=100):
    return Request(method='put', url=url, headers=headers, auth=auth, timeout=timeout, data=data)

def rDelete(*, url, headers=None, auth=None, timeout=100):
    return Request(method='delete', url=url, headers=headers, auth=auth)

#

RESTserver = os.environ.get('MUPIFDB_REST_SERVER', "http://127.0.0.1:8005/")
RESTserver = RESTserver.replace('5000', '8005')

# RESTserver *must* have trailing /, fix if not
if not RESTserver[-1] == '/':
    RESTserver += '/'

RESTserverMuPIF = RESTserver

def setRESTserver(r):
    'Used in tests to set RESTserver after import'
    global RESTserver
    global RESTserverMuPIF
    RESTserver=RESTserverMuPIF=r+'/'

granta_credentials = {'username': '', 'password': ''}

bearer_token = None
bearer_token_expires_at = 0

api_type = os.environ.get('MUPIFDB_REST_SERVER_TYPE', "mupif")
if api_type == 'granta':
    RESTserver = 'https://musicode.grantami.com/musicode/api/'

    with open("/var/lib/mupif/persistent/granta_api_login.json") as json_data_file:
        credentials = json.load(json_data_file)
        granta_credentials = {'username': credentials['username'], 'password': credentials['password']}
        username = credentials['username']
        password = credentials['password']
        server = credentials['server']


def getAuthToken():
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

# with granta, turns functions into stub (if ret is not callable) or a proxy (if ret is callable)
# without granta, transparently call the function underneath
def if_granta(ret):
    def granta_decorator(func):
        def inner(*args,**kw):
            if callable(ret): return ret(*args,**kw)
            return ret
        return inner
    def noop_decorator(func):
        def noop(*args,**kw): return func(*args,**kw)
        return noop
    return (granta_decorator if api_type=='granta' else noop_decorator)

# --------------------------------------------------
# Users
# --------------------------------------------------

# def getUserByIP(ip):
#     if api_type == 'granta':
#         return None
#     response = rGet(url=RESTserver + "users/" + str(ip))
#     return response.json()


# --------------------------------------------------
# Usecases
# --------------------------------------------------


@if_granta([])
def getUsecaseRecords():
    data = []
    response = rGet(url=RESTserver + "usecases/")
    for record in response.json():
        data.append(record)
    return data

@if_granta(None)
def getUsecaseRecord(ucid):
    response = rGet(url=RESTserver + "usecases/" + ucid)
    return response.json()

@if_granta(None)
def insertUsecaseRecord(ucid, description):
    response = rPost(url=RESTserver + "usecases/", data=json.dumps({"ucid": ucid, "description": description}))
    return response.json()


# --------------------------------------------------
# Workflows
# --------------------------------------------------

@if_granta([])
def getWorkflowRecords():
    response = rGet(url=RESTserver + "workflows/")
    return [models.Workflow_Model.model_validate(record) for record in response.json()]

@if_granta([])
def getWorkflowRecordsWithUsecase(usecase):
    data = []
    response = rGet(url=RESTserver + "usecases/" + str(usecase) + "/workflows")
    for record in response.json():
        data.append(record)
    return data

@if_granta(None)
@pydantic.validate_call
def getWorkflowRecord(wid: str) -> models.Workflow_Model|None:
    response = rGet(url=RESTserver + "workflows/" + wid)
    print(response)
    if response.json() is None: return None
    #print_json(data=response.json())
    return models.Workflow_Model.model_validate(response.json())

@if_granta(None)
@pydantic.validate_call
def insertWorkflow(wf: models.Workflow_Model):
    #print_json(data=wf.model_dump())
    response = rPost(url=RESTserver + "workflows/", data=json.dumps({"entity": wf.model_dump()}))
    return response.json()

@if_granta(None)
@pydantic.validate_call
def updateWorkflow(wf: models.Workflow_Model):
    response = rPatch(url=RESTserver + "workflows/", data=json.dumps({"entity": wf.model_dump()}))
    return response.json()


@if_granta(_granta._getGrantaWorkflowRecordGeneral)
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
@if_granta(None)
def getWorkflowRecordFromHistory(wid, version) -> models.Workflow_Model: 
    response = rGet(url=RESTserver + "workflows_history/" + wid + "/" + str(version))
    return models.Workflow_Model.model_validate(response.json())

@if_granta(None)
@pydantic.validate_call
def insertWorkflowHistory(wf: models.Workflow_Model):
    response = rPost(url=RESTserver + "workflows_history/", data=json.dumps({"entity": wf.model_dump()}))
    return response.json()


# --------------------------------------------------
# Executions
# --------------------------------------------------

@if_granta(_granta._getGrantaExecutionRecords)
@pydantic.validate_call
def getExecutionRecords(workflow_id=None, workflow_version=None, label=None, num_limit=None, status=None) -> List[models.WorkflowExecution_Model]: 
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
    return [models.WorkflowExecution_Model.model_validate(record) for record in response.json()]


@if_granta(_granta._getGrantaExecutionRecord)
@pydantic.validate_call
def getExecutionRecord(weid: str) -> models.WorkflowExecution_Model:
    response = rGet(url=RESTserver + "executions/" + weid)
    return models.WorkflowExecution_Model.model_validate(response.json())


@if_granta([])
def getScheduledExecutions(num_limit=None):
    return getExecutionRecords(status="Scheduled", num_limit=num_limit)

@if_granta(_granta.getGrantaPendingExecutions)
def getPendingExecutions(num_limit=None):
    return getExecutionRecords(status="Pending", num_limit=num_limit)

@if_granta(None)
def scheduleExecution(execution_id):
    response = rPatch(url=RESTserver + "executions/" + str(execution_id) + "/schedule")
    return response.json()

@if_granta(_granta._setGrantaExecutionParameter)
def setExecutionParameter(execution_id, param, value, val_type="str"):
    response = rPatch(url=RESTserver + "executions/" + str(execution_id), data=json.dumps({"key": str(param), "value": value}))
    return response.json()

@if_granta(None)
def setExecutionOntoBaseObjectID(execution_id, name, value):
    response = rPatch(url=RESTserver + "executions/" + str(execution_id) + "/set_onto_base_object_id/", data=json.dumps({"name": str(name), "value": value}))
    if response.status_code == 200:
        return response.json()
    return None

@if_granta(None)
def setExecutionOntoBaseObjectIDMultiple(execution_id, data):
    response = rPatch(url=RESTserver + "executions/" + str(execution_id) + "/set_onto_base_object_id_multiple/", data=json.dumps({"data": data}))
    if response.status_code == 200:
        return response.json()
    return None

@if_granta(None)
def setExecutionOntoBaseObjectIDs(execution_id, name, value):
    response = rPatch(url=RESTserver + "executions/" + str(execution_id) + "/set_onto_base_object_ids/", data=json.dumps({"name": str(name), "value": value}))
    if response.status_code == 200:
        return response.json()
    return None

@if_granta(None)
def setExecutionAttemptsCount(execution_id, val):
    return setExecutionParameter(execution_id, "Attempts", str(val), "int")


@if_granta(lambda eid: _granta._setGrantaExecutionStatus(eid,'Scheduled'))
def setExecutionStatusScheduled(execution_id):
    return setExecutionParameter(execution_id, "Status", "Scheduled")

@if_granta(None)
def setExecutionStatusCreated(execution_id):  # only reverted
    setExecutionParameter(execution_id, "SubmittedDate", "")
    return setExecutionParameter(execution_id, "Status", "Created")


@if_granta(lambda eid: _granta._setGrantaExecutionStatus(eid,'Ready'))
def setExecutionStatusPending(execution_id, reverted=False):
    if reverted:
        pass
        # setExecutionParameter(execution_id, "StartDate", "")
    else:
        setExecutionParameter(execution_id, "SubmittedDate", str(datetime.datetime.now()))
        setExecutionAttemptsCount(execution_id, 0)
    return setExecutionParameter(execution_id, "Status", "Pending")


@if_granta(lambda eid: _granta._setGrantaExecutionStatus(eid,'Running'))
def setExecutionStatusRunning(execution_id):
    setExecutionParameter(execution_id, "StartDate", str(datetime.datetime.now()))
    return setExecutionParameter(execution_id, "Status", "Running")


@if_granta(lambda eid: _granta._setGrantaExecutionStatus(eid,'Completed, to be reviewed'))
def setExecutionStatusFinished(execution_id):
    setExecutionParameter(execution_id, "EndDate", str(datetime.datetime.now()))
    return setExecutionParameter(execution_id, "Status", "Finished")


@if_granta(lambda eid: _granta._setGrantaExecutionStatus(eid,'Failed'))
def setExecutionStatusFailed(execution_id):
    setExecutionParameter(execution_id, "EndDate", str(datetime.datetime.now()))
    return setExecutionParameter(execution_id, "Status", "Failed")

@if_granta(None)
def createExecution(wid: str, version: int, ip: str, no_onto=False):
    wec=models.WorkflowExecutionCreate_Model(wid=wid,version=version,ip=ip,no_onto=no_onto)
    #print_json(data=wec.model_dump())
    response = rPost(url=RESTserver + "executions/create/", data=wec.model_dump_json())
    return response.json()

@if_granta(None)
def insertExecution(data):
    response = rPost(url=RESTserver + "executions/", data=json.dumps({"entity": data}))
    return response.json()

@if_granta(None)
def getExecutionInputRecord(weid):
    response = rGet(url=RESTserver + "executions/" + str(weid) + "/inputs/")
    return response.json()

@if_granta(None)
def getExecutionOutputRecord(weid):
    response = rGet(url=RESTserver + "executions/" + str(weid) + "/outputs/")
    #print_json(data=response.json())
    return [models.Workflow_Model.IOCard_Model.Output_Model.model_validate(record) for record in response.json()]

@if_granta(None)
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


# --------------------------------------------------
# IO Data
# --------------------------------------------------
@if_granta(None)
def getIODataRecord(iod_id):
    response = rGet(url=RESTserver + "iodata/" + str(iod_id))
    return response.json()

@if_granta(None)
def insertIODataRecord(data):
    response = rPost(url=RESTserver + "iodata/", data=json.dumps({"entity": data}))
    return response.json()


@if_granta(None)
def setExecutionInputLink(weid, name, obj_id, link_eid, link_name, link_obj_id):
    response = rPatch(url=RESTserver + "executions/" + str(weid) + "/input_item/" + str(name) + "/" + str(obj_id) + "/", data=json.dumps({"link": {"ExecID": link_eid, "Name": link_name, "ObjID": link_obj_id}}))
    return response.json()


@if_granta(None)
def setExecutionInputObject(weid, name, obj_id, object_dict):
    response = rPatch(url=RESTserver + "executions/" + str(weid) + "/input_item/" + str(name) + "/" + str(obj_id) + "/", data=json.dumps({"object": object_dict}))
    return response.json()


@if_granta(None)
def setExecutionOutputObject(weid, name, obj_id, object_dict):
    response = rPatch(url=RESTserver + "executions/" + str(weid) + "/output_item/" + str(name) + "/" + str(obj_id) + "/", data=json.dumps({"object": object_dict}))
    return response.json()


@if_granta(None)
def getPropertyArrayData(file_id, i_start, i_count):  # may not be used
    response = rGet(url=RESTserver + "property_array_data/" + str(file_id) + "/" + str(i_start) + "/" + str(i_count) + "/")
    return response.json()


# --------------------------------------------------
# Files
# --------------------------------------------------
@if_granta(_granta._getGrantaBinaryFileByID)
def getBinaryFileByID(fid):
    if api_type == 'granta':
        url = RESTserver.replace('/api/', '/filestore/')
        token = getAuthToken()
        headers = {'Authorization': f'Bearer {token["access_token"]}'}
        response = rGet(url=url + str(fid), headers=headers, allow_redirects=True)
        return response.content, response.headers['content-disposition'].split('filename=')[1].replace('"', '')

    response = rGet(url=RESTserver + "file/" + str(fid))
    d = response.headers['Content-Disposition']
    filename = re.findall("filename=(.+)", d)[0]
    return response.content, filename

@if_granta(_granta._uploadGrantaBinaryFile)
def uploadBinaryFile(binary_data):
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
    return
    # re-assemble the dictionary
    data = dict(name=name,levelno=levelno,pathname=pathname,lineno=lineno,created=created,**kw)
    data['msg'] = data['msg'] % data['args']
    del data['args']
    # pprint(data)
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
@if_granta(None)
def getStatus():
    response = rGet(url=RESTserver + "status/")
    return response.json()

@if_granta({'totalExecutions': 0,'finishedExecutions': 0,'failedExecutions': 0,'createdExecutions': 0,'pendingExecutions': 0,'scheduledExecutions': 0,'runningExecutions': 0})
def getExecutionStatistics():
    response = rGet(url=RESTserver + "execution_statistics/")
    return response.json()

@if_granta({"runningTasks": 0, "scheduledTasks": 0, "load": 0, "processedTasks": 0})
def getStatScheduler():
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
@if_granta(None)
def setStatScheduler(runningTasks=None, scheduledTasks=None, load=None, processedTasks=None, session=requests):
    if runningTasks is not None:
        response = session.patch(RESTserver + "scheduler_statistics/", data=json.dumps({"key": "scheduler.runningTasks", "value": runningTasks}))
    if scheduledTasks is not None:
        response = session.patch(RESTserver + "scheduler_statistics/", data=json.dumps({"key": "scheduler.scheduledTasks", "value": scheduledTasks}))
    if load is not None:
        response = session.patch(RESTserver + "scheduler_statistics/", data=json.dumps({"key": "scheduler.load", "value": load}))
    if processedTasks is not None:
        response = session.patch(RESTserver + "scheduler_statistics/", data=json.dumps({"key": "scheduler.processedTasks", "value": processedTasks}))


@if_granta(None)
def updateStatScheduler(runningTasks=None, scheduledTasks=None, load=None, processedTasks=None):
    if runningTasks is not None:
        response = rPatch(url=RESTserver + "scheduler_statistics/", data=json.dumps({"key": "scheduler.runningTasks", "value": runningTasks}))
    if scheduledTasks is not None:
        response = rPatch(url=RESTserver + "scheduler_statistics/", data=json.dumps({"key": "scheduler.scheduledTasks", "value": scheduledTasks}))
    if load is not None:
        response = rPatch(url=RESTserver + "scheduler_statistics/", data=json.dumps({"key": "scheduler.load", "value": load}))
    if processedTasks is not None:
        response = rPatch(url=RESTserver + "scheduler_statistics/", data=json.dumps({"key": "scheduler.processedTasks", "value": processedTasks}))


# --------------------------------------------------
# Settings
# --------------------------------------------------

@if_granta(dict)
def getSettings():
    response = rGet(url=RESTserver + "settings")
    return response.json()


# --------------------------------------------------
# EDM
# --------------------------------------------------

def getEDMDataArray(DBName, Type):
    response = rGet(url=RESTserver + "EDM/" + str(DBName) + "/" + str(Type))
    return response.json()

def getEDMData(DBName, Type, ID, path):
    if ID == '' or ID is None:
        return None
    url = RESTserver + "EDM/" + str(DBName) + "/" + str(Type) + "/" + str(ID) + "/?path=" + str(path)
    response = rGet(url=url)
    return response.json()


def setEDMData(DBName, Type, ID, path, data):
    url = RESTserver + "EDM/" + str(DBName) + "/" + str(Type) + "/" + str(ID)
    response = rPatch(url=url, data=json.dumps({"path": str(path), "data": data}))
    return response.json()


def createEDMData(DBName, Type, data):
    url = RESTserver + "EDM/" + str(DBName) + "/" + str(Type)
    response = rPost(url=url, data=json.dumps(data))
    return response.json()


def cloneEDMData(DBName, Type, ID, shallow=[]):
    url = RESTserver + "EDM/" + str(DBName) + "/" + str(Type) + "/" + str(ID) + "/clone"
    response = rGet(url=url, params={"shallow": ' '.join(shallow)})
    return response.json()


def getSafeLinks(DBName, Type, ID, paths=[]):
    url = RESTserver + "EDM/" + str(DBName) + "/" + str(Type) + "/" + str(ID) + "/safe-links"
    response = rGet(url=url, params={"paths": ' '.join(paths)})
    return response.json()


def getEDMEntityIDs(DBName, Type, filter=None):
    url = RESTserver + "EDM/" + str(DBName) + "/" + str(Type) + "/find"
    if filter:
        response = rPut(url=url, data=json.dumps({"filter": filter}))
    else:
        response = rPut(url=url, data=json.dumps({"filter": {}}))
    return response.json()


def uploadEDMBinaryFile(DBName, binary_data):
    response = rPost(url=RESTserver + "EDM/" + str(DBName) + "/blob/upload", files={"blob": binary_data})
    return response.json()


def getEDMBinaryFileByID(DBName, fid):
    response = rGet(url=RESTserver + "EDM/" + str(DBName) + "/blob/" + str(fid))
    d = response.headers['Content-Disposition']
    filename = re.findall("filename=(.+)", d)[0]
    return response.content, filename
