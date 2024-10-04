import os
import requests
from rich.pretty import pprint
import logging
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

def setRESTserver(r):
    'Used in tests to set RESTserver after import'
    global RESTserver, RestServerMuPIF
    RESTserver=RESTserverMuPIF=r+'/'


def check_response(func):
    def inner(path,**kwargs):
        # log.error(f'{func.__name__.upper()} {RESTserver}{path}: {kwargs}')
        response=func(path,**kwargs)
        # log.error(f'{response}: {response.status_code} {response.reason} {response.text}')
        if 200 <= response.status_code <= 299: return response
        raise RuntimeError(f'Error: {func.__name__.upper()} {RESTserver}{path}, status {response.status_code} ({response.reason}): {response.text}.\n{pprint(kwargs)}')
    return inner

_defaultTimeout=10

@check_response
def rGet(path, *, headers=None, auth=None, timeout=_defaultTimeout, params={}, allow_redirects=True):
    return requests.get(url=RESTserver+path, timeout=timeout, headers=headers, auth=auth, params=params, allow_redirects=allow_redirects)

@check_response
def rPost(path, *, headers=None, auth=None, data=None, timeout=_defaultTimeout, files={}, allow_redirects=True):
    return requests.post(url=RESTserver+path, timeout=timeout, headers=headers, auth=auth, data=data, files=files, allow_redirects=allow_redirects)

@check_response
def rPatch(path, *, headers=None, auth=None, data=None, timeout=_defaultTimeout):
    return requests.patch(url=RESTserver+path, timeout=timeout, headers=headers, auth=auth, data=data)

@check_response
def rPut(path, *, headers=None, auth=None, data=None, timeout=_defaultTimeout):
    return requests.put(url=RESTserver+path, timeout=timeout, headers=headers, auth=auth, data=data)

@check_response
def rDelete(path, *, headers=None, auth=None, timeout=_defaultTimeout):
    return requests.delete(url=RESTserver+path, timeout=timeout, headers=headers, auth=auth)



