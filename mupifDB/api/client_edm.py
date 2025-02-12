from .client_util import *
import json
import re

def getEDMDataArray(DBName, Type):
    response = rGet(f"EDM/{DBName}/{Type}")
    return response.json()

def getEDMData(DBName, Type, ID, path):
    if ID == '' or ID is None:
        return None
    return rGet(f"EDM/{DBName}/{Type}/{ID}/?path={path}")


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

