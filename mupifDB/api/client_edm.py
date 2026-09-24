from mupifDB.api.client_mupif import getRequestHeaders
from .client_util import *
import json
import re
from typing import Any, Tuple
import pydantic

def _dump(data: Any) -> Any:
    if isinstance(data, pydantic.BaseModel):
        # Exclude None values and drop 'id' if creating a brand new entity
        d = data.model_dump(mode='json', exclude_none=True)
        if 'id' in d and (d['id'] is None or d['id'] == ''):
            d.pop('id', None)
        return d
    if isinstance(data, list):
        return [_dump(item) for item in data]
    if isinstance(data, dict):
        return {k: _dump(v) for k, v in data.items()}
    return data

def getEDMDataArray(DBName, Type):
    return rGet(f"api/EDM/{DBName}/{Type}", headers=getRequestHeaders())

def getEDMData(DBName, Type, ID, path):
    if ID == '' or ID is None:
        return None
    return rGet(f"api/EDM/{DBName}/{Type}/{ID}/?path={path}", headers=getRequestHeaders())

def setEDMData(DBName, Type, ID, path, data):
    return rPatch(f"api/EDM/{DBName}/{Type}/{ID}", json={"path": str(path), "data": _dump(data)}, headers=getRequestHeaders())

def createEDMData(DBName, Type, data):
    payload = _dump(data)
    if isinstance(payload, dict):
        payload.pop('id', None)
        payload.pop('ids', None)
        payload.pop('_id', None)
    return rPost(f"api/EDM/{DBName}/{Type}", json=payload, headers=getRequestHeaders())

def cloneEDMData(DBName, Type, ID, shallow=[]):
    return rGet(f"api/EDM/{DBName}/{Type}/{ID}/clone", params={"shallow": ' '.join(shallow)}, headers=getRequestHeaders())

def getSafeLinks(DBName, Type, ID, paths=[]):
    return rGet(f"api/EDM/{DBName}/{Type}/{ID}/safe-links", params={"paths": ' '.join(paths)}, headers=getRequestHeaders())

def getEDMEntityIDs(DBName, Type, filter=None):
    return rPut(f"api/EDM/{DBName}/{Type}/find", json={"filter": (filter if filter else {})}, headers=getRequestHeaders())

def uploadEDMBinaryFile(DBName, binary_data):
    return rPost(f"api/EDM/{DBName}/blob/upload", files={"blob": binary_data}, headers=getRequestHeaders())

def getEDMBinaryFileByID(DBName, fid) -> Tuple[bytes, str]:
    response = rGetRaw(f"api/EDM/{DBName}/blob/{fid}", headers=getRequestHeaders())
    d = response.headers['Content-Disposition']
    filename = re.findall("filename=(.+)", d)[0]
    return response.content, filename