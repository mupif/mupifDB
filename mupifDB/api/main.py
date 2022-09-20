from fastapi import FastAPI, File, UploadFile, Depends, Body
from fastapi.responses import FileResponse
from pymongo import MongoClient
import tempfile
import gridfs
import io
import bson
from typing import Union
from pydantic import BaseModel
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")

import table_structures


client = MongoClient()
db = client.MuPIF


tags_metadata = [
    {
        "name": "Users",
    },
    {
        "name": "Usecases",
    },
    {
        "name": "Workflows",
    },
    {
        "name": "Executions",
    },
    {
        "name": "Files",
    },
]


app = FastAPI(openapi_tags=tags_metadata)


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

@app.get("/users/{user_ip}", tags=["Users"])
def get_user(user_ip: str):
    res = db.Users.find_one({'IP': user_ip})
    if res:
        return res
    return None


# --------------------------------------------------
# Usecases
# --------------------------------------------------

@app.get("/usecases/", tags=["Usecases"])
def get_usecases():
    output = []
    res = db.UseCases.find()
    if res:
        for s in res:
            output.append(table_structures.extendRecord(fix_id(s), table_structures.tableUseCase))
        return output
    return []


@app.get("/usecases/{uid}", tags=["Usecases"])
def get_usecase(uid: str):
    res = db.UseCases.find_one({"ucid": uid})
    if res is not None:
        return table_structures.extendRecord(res, table_structures.tableUseCase)
    return None


@app.get("/usecases/{uid}/workflows", tags=["Usecases"])
def get_usecase_workflows(uid: str):
    output = []
    res = db.Workflows.find({"UseCase": uid})
    if res:
        for s in res:
            output.append(table_structures.extendRecord(fix_id(s), table_structures.tableWorkflow))
        return output
    return []


class M_UseCase(BaseModel):
    ucid: str
    description: str


@app.post("/usecases/", tags=["Usecases"])
def post_usecase(data: M_UseCase):
    res = db.UseCases.insert_one({"ucid": data.ucid, "Description": data.description})
    return str(res.inserted_id)


# --------------------------------------------------
# Workflows
# --------------------------------------------------

@app.get("/workflows/", tags=["Workflows"])
def get_workflows():
    output = []
    res = db.Workflows.find()
    if res:
        for s in res:
            output.append(table_structures.extendRecord(fix_id(s), table_structures.tableWorkflow))
        return output
    return []


@app.get("/workflows/{workflow_id}", tags=["Workflows"])
def get_workflow(workflow_id: str):
    res = db.Workflows.find_one({"wid": workflow_id})
    if res:
        return table_structures.extendRecord(fix_id(res), table_structures.tableWorkflow)
    return None


# --------------------------------------------------
# Workflows history
# --------------------------------------------------

@app.get("/workflows_history/{workflow_id}/{workflow_version}", tags=["Workflows"])
def get_workflow_history(workflow_id: str, workflow_version: int):
    res = db.WorkflowsHistory.find_one({"wid": workflow_id, "Version": workflow_version})
    if res:
        return table_structures.extendRecord(fix_id(res), table_structures.tableWorkflow)
    return None


# --------------------------------------------------
# Executions
# --------------------------------------------------

@app.get("/executions/", tags=["Executions"])
def get_executions(status: str = "", workflow_version: int = 0, workflow_id: str = "", num_limit: int = 0, label: str = ""):
    output = []
    filtering = {}
    if status:
        filtering["Status"] = status
    if workflow_version:
        filtering["WorkflowVersion"] = workflow_version
    if workflow_id:
        filtering["WorkflowID"] = workflow_id
    if label:
        filtering["label"] = label
    if num_limit == 0:
        num_limit = 999999
    res = db.WorkflowExecutions.find(filtering).sort('CreatedDate', 1).limit(num_limit)
    if res:
        for s in res:
            output.append(table_structures.extendRecord(fix_id(s), table_structures.tableExecution))
        return output
    return []


@app.get("/executions/{uid}", tags=["Executions"])
def get_execution(uid: str):
    res = db.WorkflowExecutions.find_one({"_id": bson.objectid.ObjectId(uid)})
    if res:
        return table_structures.extendRecord(fix_id(res), table_structures.tableExecution)
    return None


@app.get("/executions/{uid}/inputs/", tags=["Executions"])
def get_execution_inputs(uid: str):
    res = db.WorkflowExecutions.find_one({"_id": bson.objectid.ObjectId(uid)})
    if res:
        if res.get('Inputs', None) is not None:
            inp = db.IOData.find_one({'_id': bson.objectid.ObjectId(res['Inputs'])})
            return inp.get('DataSet', None)
        return table_structures.extendRecord(fix_id(res), table_structures.tableExecution)
    return None


@app.get("/executions/{uid}/outputs/", tags=["Executions"])
def get_execution_outputs(uid: str):
    res = db.WorkflowExecutions.find_one({"_id": bson.objectid.ObjectId(uid)})
    if res:
        if res.get('Outputs', None) is not None:
            inp = db.IOData.find_one({'_id': bson.objectid.ObjectId(res['Outputs'])})
            return inp.get('DataSet', None)
        return table_structures.extendRecord(fix_id(res), table_structures.tableExecution)
    return None


def get_execution_io_item(uid, name, obj_id, inout):
    table = db.WorkflowExecutions
    wi = table.find_one({"_id": bson.objectid.ObjectId(uid)})
    data = db.IOData.find_one({'_id': bson.objectid.ObjectId(wi[inout])})
    for elem in data['DataSet']:
        if elem.get('Name', None) == name and elem.get('ObjID', '') == obj_id:
            return elem
    return None


@app.get("/executions/{uid}/input_item/{name}/{obj_id}/", tags=["Executions"])
def get_execution_input_item(uid: str, name: str, obj_id: str):
    return get_execution_io_item(uid, name, obj_id, 'Inputs')


@app.get("/executions/{uid}/output_item/{name}/{obj_id}/", tags=["Executions"])
def get_execution_output_item(uid: str, name: str, obj_id: str):
    return get_execution_io_item(uid, name, obj_id, 'Outputs')


class M_ModifyExecution(BaseModel):
    key: str
    value: str


@app.patch("/executions/{uid}/modify", tags=["Executions"])
def modify_execution(uid: str, data: M_ModifyExecution):
    db.WorkflowExecutions.update_one({'_id': bson.objectid.ObjectId(uid)}, {"$set": {data.key: data.value}})
    return get_execution(uid)


@app.patch("/executions/{uid}/schedule", tags=["Executions"])
def schedule_execution(uid: str):
    execution_record = get_execution(uid)
    if execution_record['Status'] == 'Created':
        data = type('', (), {})()
        data.key = "Status"
        data.value = "Pending"
        return modify_execution(uid, data)
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


@app.get("/file/{uid}", tags=["Files"])
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


@app.post("/file/", tags=["Files"])
def upload_file(file: UploadFile):
    if file:
        fs = gridfs.GridFS(db)
        sourceID = fs.put(file.file, filename=file.filename)
        return str(sourceID)
    return None













