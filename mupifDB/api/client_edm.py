from .client_util import *
import json
import re

def getEDMDataArray(DBName, Type):
    return rGet(f"EDM/{DBName}/{Type}")

def getEDMData(DBName, Type, ID, path):
    if ID == '' or ID is None:
        return None
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


def uploadEDMBinaryFile(DBName, binary_data):
    return rPost(f"EDM/{DBName}/blob/upload", files={"blob": binary_data})


def getEDMBinaryFileByID(DBName, fid):
    response = rGetRaw(f"EDM/{DBName}/blob/{fid}")
    d = response.headers['Content-Disposition']
    filename = re.findall("filename=(.+)", d)[0]
    return response.content, filename

