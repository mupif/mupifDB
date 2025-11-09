import os
import requests
from requests.models import Response
import logging
import json
from typing import TypeVar,Any,Callable,Optional,Dict,List,Literal
from rich import print_json
log = logging.getLogger(__name__)


RESTserver = os.environ.get('MUPIFDB_REST_SERVER', "http://127.0.0.1:8005/")
RESTserver = RESTserver.replace('5000', '8005')

# RESTserver *must* have trailing /, fix if not
if not RESTserver[-1] == '/':
    RESTserver += '/'

RESTserverMuPIF = RESTserver

def setRESTserver(r: str) -> None:
    'Used in tests to set RESTserver after import'
    global RESTserver, RestServerMuPIF
    RESTserver=RESTserverMuPIF=r+'/'

class NotFoundResponse(Exception):
    """
    Custom exception being raised if the API reports 404 exception (not found).
    """
    pass

def _check(resp: Response):
    msg=f'{resp.request.method} {resp.request.url}, status {resp.status_code} ({resp.reason}): {resp.text}'
    (log.info if (200<=resp.status_code<300 and resp.status_code!=404) else log.error)(msg)
    if 200 <= resp.status_code <= 299: return resp
    elif resp.status_code==404: raise NotFoundResponse(msg)
    elif resp.status_code==422: # Unprocessable entity
        log.error(100*'*'+'\nUnprocessable entity\n'+100*'*')
        txt=json.loads(resp.text)
        print(txt['message'])
        try:
            import ast
            print_json(data=ast.literal_eval(txt['message']))
        except: print('(not renderable as JSON)')
        # print error to log, continue to raise exception below
    raise RuntimeError(f'Error: {resp.request.method} {resp.request.url}, status {resp.status_code} ({resp.reason}): {resp.text}.')

_defaultTimeout=4


def rGetRaw(path, *, headers=None, auth=None, timeout=_defaultTimeout, params={}, allow_redirects=True):  # type: ignore
    return _check(requests.get(url=RESTserver+path, timeout=timeout, headers=headers, auth=auth, params=params, allow_redirects=allow_redirects))
def rPostRaw(path, *, headers=None, auth=None, data=None, timeout=_defaultTimeout, files={}, allow_redirects=True): # type: ignore
    return _check(requests.post(url=RESTserver+path, timeout=timeout, headers=headers, auth=auth, data=data, files=files, allow_redirects=allow_redirects))
def rPatchRaw(path, *, headers=None, auth=None, data=None, timeout=_defaultTimeout):
    return _check(requests.patch(url=RESTserver+path, timeout=timeout, headers=headers, auth=auth, data=data))
def rPutRaw(path, *, headers=None, auth=None, data=None, timeout=_defaultTimeout): # type: ignore
    return _check(requests.put(url=RESTserver+path, timeout=timeout, headers=headers, auth=auth, data=data))
def rDeleteRaw(path, *, headers=None, auth=None, timeout=_defaultTimeout): # type: ignore
    return _check(requests.delete(url=RESTserver+path, timeout=timeout, headers=headers, auth=auth))

def rGet(*args,**kw): return rGetRaw(*args,**kw).json()
def rPost(*args,**kw): return rPostRaw(*args,**kw).json()
def rPatch(*args,**kw): return rPatchRaw(*args,**kw).json()
def rPut(*args,**kw): return rPutRaw(*args,**kw).json()
def rDelete(*args,**kw): return rDeleteRaw(*args,**kw).json()

def logMessage(*,name,levelno,pathname,lineno,created,**kw):
    '''
    from client_mupif import *

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
        response = rPost("logs/", data=json.dumps(data))
    finally:
        logging.disable(previous_level)
    return response.json()


# if 0:
#     # this is not used, and maybe never will
#
#     from .. import models
#     import pydantic
#
#     # https://stackoverflow.com/a/68284497
#     from abc import ABCMeta, abstractmethod
#
#     # decorate all methods as class methods, abstract, and validated by pydantic
#     class Interface(ABCMeta):
#         def __new__(metacls, name, bases, classdict):
#             for attr, value in classdict.items():
#                 if callable(value):
#                     classdict[attr] = abstractmethod(pydantic.validate_call(staticmethod(value)))
#             return super().__new__(ABCMeta, name, bases, classdict)
#
#     # linters don't know those methods are static, so warning everywhere.
#     class Client_ABC(Interface):
#         def getUsercaseRecords() -> List[models.UseCase_Model]: ...
#         def getUsecaseRecord(ucid: str) -> models.UseCase_Model: ...
#         def insertUsecaseRecord(ucid: str, description: str): ...
#         def getWorkflowRecords() -> List[models.Workflow_Model]: ...
#         def getWorkflowRecordsWithUsecase(usecase) -> List[models.Workflow_Model]: ...
#         def updateWorkflow(wf: models.Workflow_Model) -> models.Workflow_Model: ...
#         def getWorkflowRecord(wid, version: int) -> models.Workflow_Model: ...
#         def getExecutionRecords(workflow_id: str|None=None, workflow_version: int|None=None, label: str|None=None, num_limit: int|None=None, status: str|None=None) -> List[models.WorkflowExecution_Model]: ...
#         def getExecutionRecord(weid: str) -> models.WorkflowExecution_Model: ...
#         def getScheduledExecutions(num_limit=None): ...
#         def getPendingExecutions(num_limit=None): ...
#         def scheduleExecution(execution_id): ...
#         def setExecutionParameter(execution_id, param, value, val_type="str"): ...
#         def setExecutionOntoBaseObjectID(execution_id, name, value): ...
#         def setExecutionOntoBaseObjectIDMultiple(execution_id, data): ...
#         def setExecutionOntoBaseObjectIDs(execution_id, name, value): ...
#         def setExecutionAttemptsCount(execution_id, val): ...
#         def setExecutionStatus(execution_id: str, status: Literal=['Scheduled','Created','Pending','Running','Finished','Failed']): ...
#         def createExecution(wid: str, version: int, ip: str, no_onto=False): ...
#         def insertExecution(m: models.WorkflowExecution_Model): ...
#         def getExecutionInputRecord(weid) -> List[models.IODataRecordItem_Model]: ...
#         def getExecutionOutputRecord(weid) -> List[models.IODataRecordItem_Model]: ...
#         def getExecutionInputRecordItem(weid, name, obj_id): ...
#         def getExecutionOutputRecordItem(weid, name, obj_id): ...
#         def getIODataRecord(iod_id: str) ->  models.IODataRecord_Model: ...
#         def insertIODataRecord(data: models.IODataRecord_Model): ...
#         def setExecutionInputLink(weid, name, obj_id, link_eid, link_name, link_obj_id): ...
#         def setExecutionInputObject(weid, name, obj_id, object_dict): ...
#         def setExecutionOutputObject(weid, name, obj_id, object_dict): ...
#         def getPropertyArrayData(file_id, i_start, i_count):  ... # may not be used
#         def getBinaryFileByID(fid): ...
#         def uploadBinaryFile(binary_data): ...
#         def getStatus(): ...
#         def getExecutionStatistics() -> models.MupifDBStatus_Model.ExecutionStatistics_Model: ...
#         def getStatScheduler() : ...
#         def setStatScheduler(runningTasks=None, scheduledTasks=None, load=None, processedTasks=None, session=requests): ...
#         def updateStatScheduler(runningTasks=None, scheduledTasks=None, load=None, processedTasks=None): ...
#         def getSettings(maybe_init_db: bool=False): ...
