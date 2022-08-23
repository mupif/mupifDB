from fastapi import FastAPI, File, UploadFile, Depends
from fastapi.responses import FileResponse
from pymongo import MongoClient
import tempfile
import gridfs
import io
import bson
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")

import table_structures


client = MongoClient()
db = client.MuPIF

app = FastAPI()


def fix_id(record):
    if record:
        if '_id' in record:
            record['_id'] = str(record['_id'])
    return record


@app.get("/")
def read_root():
    return {"MuPIF": "API"}


# --------------------------------------------------
# Users
# --------------------------------------------------

@app.get("/user/{user_ip}")
def get_user(user_ip: str):
    table = db.Users
    res = table.find_one({'IP': user_ip})
    if res:
        return res
    return None


# --------------------------------------------------
# Usecases
# --------------------------------------------------

@app.get("/usecases/")
def get_usecases():
    table = db.UseCases
    output = []
    res = table.find()
    if res:
        for s in res:
            output.append(table_structures.extendRecord(fix_id(s), table_structures.tableUseCase))
        return output
    return []


@app.get("/usecase/{uid}")
def get_usecase(uid: str):
    return {"result": uid}


# --------------------------------------------------
# Workflows
# --------------------------------------------------

@app.get("/workflows/")
def get_workflows():
    table = db.Workflows
    output = []
    res = table.find()
    if res:
        for s in res:
            output.append(table_structures.extendRecord(fix_id(s), table_structures.tableWorkflow))
        return output
    return []


@app.get("/workflow/{workflow_id}")
def get_workflow(workflow_id: str):
    table = db.Workflows
    res = table.find_one({"wid": workflow_id})
    if res:
        return table_structures.extendRecord(fix_id(res), table_structures.tableWorkflow)
    return None


# --------------------------------------------------
# Executions
# --------------------------------------------------

@app.get("/executions/")
def get_executions():
    table = db.WorkflowExecutions
    output = []
    res = table.find()
    if res:
        for s in res:
            output.append(table_structures.extendRecord(fix_id(s), table_structures.tableExecution))
        return output
    return []


@app.get("/execution/{uid}")
def get_execution(uid: str):
    table = db.WorkflowExecutions
    res = table.find_one({"_id": uid})
    if res:
        return table_structures.extendRecord(fix_id(res), table_structures.tableExecution)
    return None


# --------------------------------------------------
# Files
# --------------------------------------------------

async def get_temp_dir():
    tdir = tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB')
    try:
        yield tdir.name
    finally:
        del tdir


@app.get("/file/{uid}")
def get_file(uid: str, tdir=Depends(get_temp_dir)):
    fs = gridfs.GridFS(db)
    foundfile = fs.get(bson.objectid.ObjectId(uid))
    wfile = io.BytesIO(foundfile.read())
    fn = foundfile.filename
    fullpath = tdir + '/' + fn
    with open(fullpath, "wb") as f:
        f.write(wfile.read())
        f.close()
        return FileResponse(path=fullpath, headers={"Content-Disposition": "attachment; filename=" + fn})


@app.post("/file/")
def upload_file(file: UploadFile):
    if file:
        fs = gridfs.GridFS(db)
        sourceID = fs.put(file.file, filename=file.filename)
        return str(sourceID)
    return None













