from mupifDB.api.client_mupif import getRequestHeaders
from .client_util import *
import json
import re

def getEDMDataArray(DBName, Type):
    return rGet(f"api/EDM/{DBName}/{Type}", headers=getRequestHeaders())

def getEDMData(DBName, Type, ID, path):
    if ID == '' or ID is None:
        return None
    return rGet(f"api/EDM/{DBName}/{Type}/{ID}/?path={path}", headers=getRequestHeaders())


def setEDMData(DBName, Type, ID, path, data):
    return rPatch(f"api/EDM/{DBName}/{Type}/{ID}", data=json.dumps({"path": str(path), "data": data}), headers=getRequestHeaders())


def createEDMData(DBName, Type, data):
    return rPost(f"api/EDM/{DBName}/Type", data=json.dumps(data), headers=getRequestHeaders())


def cloneEDMData(DBName, Type, ID, shallow=[]):
    return rGet(f"api/EDM/{DBName}/{Type}/{ID}/clone", params={"shallow": ' '.join(shallow)}, headers=getRequestHeaders())


def getSafeLinks(DBName, Type, ID, paths=[]):
    return rGet(f"api/EDM/{DBName}/{Type}/{ID}/safe-links", params={"paths": ' '.join(paths)}, headers=getRequestHeaders())


def getEDMEntityIDs(DBName, Type, filter=None):
    return rPut(f"api/EDM/{DBName}/{Type}/find", data=json.dumps({"filter": (filter if filter else {})}), headers=getRequestHeaders())


def uploadEDMBinaryFile(DBName, binary_data):
    return rPost(f"api/EDM/{DBName}/blob/upload", files={"blob": binary_data}, headers=getRequestHeaders())


def getEDMBinaryFileByID(DBName, fid):
    response = rGetRaw(f"api/EDM/{DBName}/blob/{fid}", headers=getRequestHeaders())
    d = response.headers['Content-Disposition']
    filename = re.findall("filename=(.+)", d)[0]
    return response.content, filename

