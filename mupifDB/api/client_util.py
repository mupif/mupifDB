import contextlib
import contextvars
import os
import time
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

_defaultTimeout=(4.0, 12.0)

class NotFoundResponse(Exception):
    """
    Custom exception being raised if the API reports 404 exception (not found).
    """
    pass


class ConflictResponse(Exception):
    """
    The API reports 409 (conflict): a lease condition failed (see mupifDB.api.leases).

    E.g. the execution was claimed by another scheduler, or the lease the request was made under
    is not current any more (taken over): the caller must not retry, but drop what it was doing.
    Distinct from errors which may go away by retrying (5xx, API unreachable).
    """
    pass


#: lease the requests of the current thread are made under (``<weid>:<scheduler_id>:<epoch>``),
#: sent as the header ``X-MupifDB-Lease``; see :func:`leaseContext` and :func:`currentLease`
_lease: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar('mupifDBLease', default=None)


@contextlib.contextmanager
def leaseContext(lease: Optional[str]):
    """
    Make the requests of the current thread (and its context) carry the given lease.

    The scheduler runs each execution in a job thread under the execution's lease; every write the
    thread makes to the execution is then checked by the API against the current lease (fencing).
    """
    token = _lease.set(lease)
    try:
        yield
    finally:
        _lease.reset(token)


def currentLease() -> Optional[str]:
    """
    Return the lease requests are made under: set by :func:`leaseContext`, else the environment
    variable ``MUPIFDB_LEASE`` (set by the scheduler for the workflow execution script), else None.
    """
    return _lease.get() or os.environ.get('MUPIFDB_LEASE') or None

def _check(resp: Response):
    msg = f'{resp.request.method} {resp.request.url}, status {resp.status_code} ({resp.reason}): {resp.text}'
    (log.info if (200 <= resp.status_code < 300 and resp.status_code != 404) else log.error)(msg)
    if 200 <= resp.status_code <= 299:
        return resp
    elif resp.status_code == 404:
        raise NotFoundResponse(msg)
    elif resp.status_code == 409:
        raise ConflictResponse(msg)
    elif resp.status_code == 422:  # Unprocessable entity
        log.error(100 * '*' + '\nUnprocessable entity\n' + 100 * '*')
        txt = json.loads(resp.text)
        print(txt['message'])
        try:
            import ast
            print_json(data=ast.literal_eval(txt['message']))
        except Exception:
            print('(not renderable as JSON)')
    raise RuntimeError(f'Error: {resp.request.method} {resp.request.url}, status {resp.status_code} ({resp.reason}): {resp.text}.')

#: Called when the REST API rejects a request with 401 although it carried an access token (e.g.
#: the API restarted with a new JWT key, or the session was removed): drops the cached token, logs
#: in again and returns the new ``Authorization`` header, or None if that is not possible. Set by
#: client_mupif, which owns the credentials.
onUnauthorized: Optional[Callable[[], Optional[Dict[str, str]]]] = None


def _retrySeconds() -> float:
    """
    How long requests failing with a connection error (REST API unreachable) are retried, in seconds.

    ``MUPIFDB_RETRY_SECONDS``, set by the scheduler for the workflow execution script to ``inf``
    (retried until the REST API answers): an outage of the REST API, however long, then does not
    lose the results of a run. The scheduler bounds the wait -- it stops the workflow when its lease
    was taken over or its run time is exceeded, and the script ends with the scheduler.
    0 (not set): no retries (the scheduler has its own).
    """
    try:
        return float(os.environ.get('MUPIFDB_RETRY_SECONDS', '0') or 0)
    except ValueError:
        return 0.


def _request(method: str, path: str, *, headers=None, files=None, **kw) -> Response:
    """
    Send one request to the REST API; on 401, log in again once and repeat the request.

    :param method: HTTP method name (``get``, ``post``, ...)
    :param path: path relative to :data:`RESTserver`
    :param headers: request headers; only requests with an ``Authorization`` header are repeated
    :param files: files to upload (rewound before a repeated request)
    :return: the response, checked by :func:`_check`
    """
    def send():
        window = _retrySeconds()
        deadline, pause = time.monotonic() + window, 2.
        while True:
            try:
                return getattr(requests, method)(url=RESTserver + path, headers=headers, **({'files': files} if files is not None else {}), **kw)
            except (requests.ConnectionError, requests.Timeout) as e:
                if time.monotonic() + pause > deadline:
                    raise
                log.warning(f'{method.upper()} {path}: REST API unreachable ({e}); retrying in {pause:.0f} s')
                time.sleep(pause)
                pause = min(pause * 2, 30.)
                for f in (files or {}).values():
                    if hasattr(f, 'seek'):
                        f.seek(0)
    resp = send()
    if resp.status_code == 401 and headers and 'Authorization' in headers and onUnauthorized is not None:
        newAuth = onUnauthorized()
        if newAuth:
            log.warning(f'{method.upper()} {path}: access token rejected (401), repeating the request after logging in again')
            headers = dict(headers, **newAuth)
            for f in (files or {}).values():
                if hasattr(f, 'seek'):
                    f.seek(0)
            resp = send()
    return _check(resp)

def rGetRaw(path, *, headers=None, auth=None, timeout=_defaultTimeout, params={}, allow_redirects=True):  # type: ignore
    return _request('get', path, timeout=timeout, headers=headers, auth=auth, params=params, allow_redirects=allow_redirects)

def rPostRaw(path, *, headers=None, auth=None, data=None, json=None, timeout=_defaultTimeout, files={}, allow_redirects=True):  # type: ignore
    return _request('post', path, timeout=timeout, headers=headers, auth=auth, data=data, json=json, files=files, allow_redirects=allow_redirects)

def rPatchRaw(path, *, headers=None, auth=None, data=None, json=None, timeout=_defaultTimeout):
    return _request('patch', path, timeout=timeout, headers=headers, auth=auth, data=data, json=json)

def rPutRaw(path, *, headers=None, auth=None, data=None, json=None, timeout=_defaultTimeout):  # type: ignore
    return _request('put', path, timeout=timeout, headers=headers, auth=auth, data=data, json=json)

def rDeleteRaw(path, *, headers=None, auth=None, timeout=_defaultTimeout):  # type: ignore
    return _request('delete', path, timeout=timeout, headers=headers, auth=auth)

def rGet(*args, **kw): return rGetRaw(*args, **kw).json()
def rPost(*args, **kw): return rPostRaw(*args, **kw).json()
def rPatch(*args, **kw): return rPatchRaw(*args, **kw).json()
def rPut(*args, **kw): return rPutRaw(*args, **kw).json()
def rDelete(*args, **kw): return rDeleteRaw(*args, **kw).json()

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
#         def createExecution(wid: str, version: int, ip: str, no_edm=False): ...
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
