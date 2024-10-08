import os
import requests
from requests.models import Response
import logging
import json
from typing import TypeVar,Any,Callable,Optional,Dict
from rich import print_json
log=logging.getLogger(__name__)


api_type = os.environ.get('MUPIFDB_REST_SERVER_TYPE', "mupif")

if api_type=='granta':
    RESTserver = 'https://musicode.grantami.com/musicode/api/'
else:
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

def _check(resp: Response) -> Response:
    (log.info if 200<=resp.status_code<300 else log.error)(f'{resp.request.method} {resp.request.url}, status {resp.status_code} ({resp.reason}): {resp.text}')
    if resp.status_code==422: # Unprocessable entity
        log.error(100*'*'+'\nUnprocessable entity\n'+100*'*')
        txt=json.loads(resp.text)
        print(txt['message'])
        try:
            import ast
            print_json(data=ast.literal_eval(txt['message']))
        except: print('(not renderable as JSON)')
    #try:
    #    txt=json.loads(resp.text)
    #    print_json(txt['message'])
    #    #if isinstance(B:=resp.request.body,bytes): print_json(B.decode('utf-8'))
    #    #elif isinstance(B,str): print_json(B)
    #    #else: print_json("null")
    #except: pass
    if 200 <= resp.status_code <= 299: return resp
    raise RuntimeError(f'Error: {resp.request.method} {resp.request.url}, status {resp.status_code} ({resp.reason}): {resp.text}.')

_defaultTimeout=4

OStr=Optional[str]

def rGet(path, *, headers=None, auth=None, timeout=_defaultTimeout, params={}, allow_redirects=True) -> requests.Response:  # type: ignore
    return _check(requests.get(url=RESTserver+path, timeout=timeout, headers=headers, auth=auth, params=params, allow_redirects=allow_redirects))

def rPost(path, *, headers=None, auth=None, data=None, timeout=_defaultTimeout, files={}, allow_redirects=True): # type: ignore
    return _check(requests.post(url=RESTserver+path, timeout=timeout, headers=headers, auth=auth, data=data, files=files, allow_redirects=allow_redirects))

def rPatch(path, *, headers=None, auth=None, data=None, timeout=_defaultTimeout):
    return _check(requests.patch(url=RESTserver+path, timeout=timeout, headers=headers, auth=auth, data=data))

def rPut(path, *, headers=None, auth=None, data=None, timeout=_defaultTimeout): # type: ignore
    return _check(requests.put(url=RESTserver+path, timeout=timeout, headers=headers, auth=auth, data=data))

def rDelete(path, *, headers=None, auth=None, timeout=_defaultTimeout): # type: ignore
    return _check(requests.delete(url=RESTserver+path, timeout=timeout, headers=headers, auth=auth))






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
        response = rPost("logs/", data=json.dumps({"entity": data}))
    finally:
        logging.disable(previous_level)
    return response.json()

