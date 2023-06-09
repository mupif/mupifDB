from fastapi import FastAPI, UploadFile, Depends
from fastapi.responses import FileResponse
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
import tempfile
import gridfs
import typing
import io
import bson
import psutil
from pymongo import ReturnDocument
from pydantic import BaseModel
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
import mupifDB
import mupif as mp
import Pyro5.api


import table_structures


client = MongoClient("mongodb://localhost:27017")
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
        "name": "IOData",
    },
    {
        "name": "Files",
    },
    {
        "name": "Logs",
    },
    {
        "name": "Stats",
    },
    {
        "name": "Additional",
    },
    {
        "name": "User Interface",
    },
]


app = FastAPI(openapi_tags=tags_metadata)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# import and initialize EDM
if 1:
    import mupifDB.api.edm as edm
    edm.initializeEdm(client)
    app.include_router(edm.dms3.router)



def fix_id(record):
    if record:
        if '_id' in record:
            record['_id'] = str(record['_id'])
    return record


# --------------------------------------------------
# Default
# --------------------------------------------------

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
        return fix_id(res)
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
        return table_structures.extendRecord(fix_id(res), table_structures.tableUseCase)
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


class M_Dict(BaseModel):
    entity: dict


@app.patch("/workflows/", tags=["Workflows"])
def update_workflow(data: M_Dict):
    res = db.Workflows.find_one_and_update({'wid': data.entity['wid']}, {'$set': data.entity}, return_document=ReturnDocument.AFTER)
    return table_structures.extendRecord(fix_id(res), table_structures.tableWorkflow)


@app.post("/workflows/", tags=["Workflows"])
def insert_workflow(data: M_Dict):
    res = db.Workflows.insert_one(data.entity)
    return str(res.inserted_id)


@app.post("/workflows_history/", tags=["Workflows"])
def insert_workflow_history(data: M_Dict):
    res = db.WorkflowsHistory.insert_one(data.entity)
    return str(res.inserted_id)


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
    res = db.WorkflowExecutions.find(filtering).limit(num_limit)  # .sort('CreatedDate', 1)
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


class M_WorkflowExecutionAddSpec(BaseModel):
    wid: str
    version: str
    ip: str
    no_onto: bool


@app.post("/executions/create/", tags=["Executions"])
def create_execution(data: M_WorkflowExecutionAddSpec):
    c = mupifDB.workflowmanager.WorkflowExecutionContext.create(workflowID=data.wid, workflowVer=int(data.version), requestedBy='', ip=data.ip, no_onto=bool(data.no_onto))
    return str(c.executionID)


@app.post("/executions/", tags=["Executions"])
def insert_execution(data: M_Dict):
    res = db.WorkflowExecutions.insert_one(data.entity)
    return str(res.inserted_id)


@app.get("/executions/{uid}/inputs/", tags=["Executions"])
def get_execution_inputs(uid: str):
    res = db.WorkflowExecutions.find_one({"_id": bson.objectid.ObjectId(uid)})
    if res:
        if res.get('Inputs', None) is not None:
            inp = db.IOData.find_one({'_id': bson.objectid.ObjectId(res['Inputs'])})
            return inp.get('DataSet', None)
    return None


@app.get("/executions/{uid}/outputs/", tags=["Executions"])
def get_execution_outputs(uid: str):
    res = db.WorkflowExecutions.find_one({"_id": bson.objectid.ObjectId(uid)})
    if res:
        if res.get('Outputs', None) is not None:
            inp = db.IOData.find_one({'_id': bson.objectid.ObjectId(res['Outputs'])})
            return inp.get('DataSet', None)
    return None


def get_execution_io_item(uid, name, obj_id, inout):
    we = db.WorkflowExecutions.find_one({"_id": bson.objectid.ObjectId(uid)})
    data = db.IOData.find_one({'_id': bson.objectid.ObjectId(we[inout])})
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


@app.get("/executions/{uid}/input_item/{name}//", tags=["Executions"])
def _get_execution_input_item(uid: str, name: str):
    return get_execution_io_item(uid, name, '', 'Inputs')


@app.get("/executions/{uid}/output_item/{name}//", tags=["Executions"])
def _get_execution_output_item(uid: str, name: str):
    return get_execution_io_item(uid, name, '', 'Outputs')


class M_IOData_link(BaseModel):
    ExecID: str
    Name: str
    ObjID: str


class M_IODataSetContainer(BaseModel):
    link: typing.Optional[dict] = None
    object: typing.Optional[dict] = None


def set_execution_io_item(uid, name, obj_id, inout, data_container):
    we = db.WorkflowExecutions.find_one({"_id": bson.objectid.ObjectId(uid)})
    if (we.get('Status', '') == 'Created' and inout == 'Inputs') or (we.get('Status', '') == 'Running' and inout == 'Outputs'):
        id_condition = {'_id': bson.objectid.ObjectId(we[inout])}
        if data_container.link is not None and inout == 'Inputs':
            res = db.IOData.update_one(id_condition, {'$set': {"DataSet.$[r].Link": data_container.link}}, array_filters=[{"r.Name": name, "r.ObjID": str(obj_id)}])
            return res.matched_count > 0
        if data_container.object is not None:
            res = db.IOData.update_one(id_condition, {'$set': {"DataSet.$[r].Object": data_container.object}}, array_filters=[{"r.Name": name, "r.ObjID": str(obj_id)}])
            return res.matched_count > 0
    return False


@app.patch("/executions/{uid}/input_item/{name}/{obj_id}/", tags=["Executions"])
def set_execution_input_item(uid: str, name: str, obj_id: str, data: M_IODataSetContainer):
    return set_execution_io_item(uid, name, obj_id, 'Inputs', data)


@app.patch("/executions/{uid}/output_item/{name}/{obj_id}/", tags=["Executions"])
def set_execution_output_item(uid: str, name: str, obj_id: str, data: M_IODataSetContainer):
    return set_execution_io_item(uid, name, obj_id, 'Outputs', data)


@app.patch("/executions/{uid}/input_item/{name}//", tags=["Executions"])
def _set_execution_input_item(uid: str, name: str, data: M_IODataSetContainer):
    return set_execution_io_item(uid, name, '', 'Inputs', data)


@app.patch("/executions/{uid}/output_item/{name}//", tags=["Executions"])
def _set_execution_output_item(uid: str, name: str, data: M_IODataSetContainer):
    return set_execution_io_item(uid, name, '', 'Outputs', data)


class M_ModifyExecutionOntoBaseObjectID(BaseModel):
    name: str
    value: str


@app.patch("/executions/{uid}/set_onto_base_object_id/", tags=["Executions"])
def modify_execution(uid: str, data: M_ModifyExecutionOntoBaseObjectID):
    db.WorkflowExecutions.update_one({'_id': bson.objectid.ObjectId(uid), "EDMMapping.Name": data.name}, {"$set": {"EDMMapping.$.id": data.value}})
    return get_execution(uid)


class M_ModifyExecutionOntoBaseObjectIDMultiple(BaseModel):
    data: list[dict]


@app.patch("/executions/{uid}/set_onto_base_object_id_multiple/", tags=["Executions"])
def modify_execution(uid: str, data: M_ModifyExecutionOntoBaseObjectIDMultiple):
    for d in data.data:
        db.WorkflowExecutions.update_one({'_id': bson.objectid.ObjectId(uid), "EDMMapping.Name": d['name']}, {"$set": {"EDMMapping.$.id": d['value']}})
    return get_execution(uid)


class M_ModifyExecutionOntoBaseObjectIDs(BaseModel):
    name: str
    value: list[str]


@app.patch("/executions/{uid}/set_onto_base_object_ids/", tags=["Executions"])
def modify_execution(uid: str, data: M_ModifyExecutionOntoBaseObjectIDs):
    db.WorkflowExecutions.update_one({'_id': bson.objectid.ObjectId(uid), "EDMMapping.Name": data.name}, {"$set": {"EDMMapping.$.ids": data.value}})
    return get_execution(uid)


class M_ModifyExecution(BaseModel):
    key: str
    value: str


@app.patch("/executions/{uid}", tags=["Executions"])
def modify_execution(uid: str, data: M_ModifyExecution):
    db.WorkflowExecutions.update_one({'_id': bson.objectid.ObjectId(uid)}, {"$set": {data.key: data.value}})
    return get_execution(uid)


@app.patch("/executions/{uid}/schedule", tags=["Executions"])
def schedule_execution(uid: str):
    execution_record = get_execution(uid)
    if execution_record['Status'] == 'Created' or True:
        data = type('', (), {})()
        data.key = "Status"
        data.value = "Pending"
        return modify_execution(uid, data)
    return None


# --------------------------------------------------
# IOData
# --------------------------------------------------

@app.get("/iodata/{uid}", tags=["IOData"])
def get_execution_iodata(uid: str):
    res = db.IOData.find_one({'_id': bson.objectid.ObjectId(uid)})
    return fix_id(res)
    # return res.get('DataSet', None)


@app.post("/iodata/", tags=["IOData"])
def insert_execution_iodata(data: M_Dict):
    res = db.IOData.insert_one(data.entity)
    return str(res.inserted_id)


# @app.patch("/iodata/", tags=["IOData"])
# def set_execution_iodata(data: M_Dict):
#     res = db.IOData.insert_one(data.entity)
#     return str(res.inserted_id)


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


@app.get("/property_array_data/{fid}/{i_start}/{i_count}/", tags=["Additional"])
def get_property_array_data(fid: str, i_start: int, i_count: int):
    pfile, fn = mupifDB.restApiControl.getBinaryFileByID(fid)
    with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
        full_path = tempDir + "/file.h5"
        f = open(full_path, 'wb')
        f.write(pfile)
        f.close()
        prop = mp.ConstantProperty.loadHdf5(full_path)
        propval = prop.getValue()
        tot_elems = propval.shape[0]
        id_start = int(i_start)
        id_num = int(i_count)
        if id_num <= 0:
            id_num = tot_elems
        id_end = id_start + id_num
        sub_propval = propval[id_start:id_end]
        return sub_propval.tolist()


@app.get("/field_as_vtu/{fid}", tags=["Additional"])
def get_property_array_data(fid: str, tdir=Depends(get_temp_dir)):
    pfile, fn = mupifDB.restApiControl.getBinaryFileByID(fid)
    full_path = tdir + "/file.h5"
    f = open(full_path, 'wb')
    f.write(pfile)
    f.close()
    field = mp.Field.makeFromHdf5(fileName=full_path)[0]
    full_path_vtu = tdir+'/file.vtu'
    field.toMeshioMesh().write(full_path_vtu)
    return FileResponse(path=full_path_vtu, headers={"Content-Disposition": "attachment; filename=file.vtu"})


# --------------------------------------------------
# Stats
# --------------------------------------------------

@app.post("/logs/", tags=["Logs"])
def insert_log(data: M_Dict):
    res = db.Logs.insert_one(data.entity)
    return str(res.inserted_id)


# --------------------------------------------------
# Stats
# --------------------------------------------------

@app.get("/status/", tags=["Stats"])
def get_status():
    mupifDBStatus = 'OK'
    schedulerStatus = 'OK'

    pidfile = 'mupifDB_scheduler_pidfile'
    if not os.path.exists(pidfile):
        schedulerStatus = 'Failed'
    else:
        with open(pidfile, "r") as f:
            try:
                pid = int(f.read())
            except (OSError, ValueError):
                schedulerStatus = 'Failed'

        if not psutil.pid_exists(pid):
            schedulerStatus = 'Failed'

    # get some scheduler stats
    stat = mupifDB.schedulerstat.getGlobalStat()
    schedulerstat = db.Stat.find_one()['scheduler']
    return {'mupifDBStatus': mupifDBStatus, 'schedulerStatus': schedulerStatus, 'totalStat': stat, 'schedulerStat': schedulerstat}


@app.get("/execution_statistics/", tags=["Stats"])
def get_execution_statistics():
    res = client.MuPIF.WorkflowExecutions.aggregate([
        {"$group": {"_id": "$Status", "count": {"$sum": 1}}}
    ])
    vals = {}
    tot = 0
    for r in res:
        vals[r['_id']] = r['count']
        tot += r['count']

    output = {}
    output['totalExecutions'] = tot
    output['finishedExecutions'] = vals.get('Finished', 0)
    output['failedExecutions'] = vals.get('Failed', 0)
    output['createdExecutions'] = vals.get('Created', 0)
    output['pendingExecutions'] = vals.get('Pending', 0)
    output['scheduledExecutions'] = vals.get('Scheduled', 0)
    output['runningExecutions'] = vals.get('Running', 0)
    return output


@app.get("/scheduler_statistics/", tags=["Stats"])
def get_scheduler_statistics():
    table = db.Stat
    output = {}
    for s in table.find():
        keys = ["runningTasks", "scheduledTasks", "load", "processedTasks"]
        for k in keys:
            if k in s["scheduler"]:
                output[k] = s["scheduler"][k]
        break
    return output


class M_ModifyStatistics(BaseModel):
    key: str
    value: int


@app.patch("/scheduler_statistics/", tags=["Stats"])
def set_scheduler_statistics(data: M_ModifyStatistics):
    if data.key in ["scheduler.runningTasks", "scheduler.scheduledTasks", "scheduler.load", "scheduler.processedTasks"]:
        res = db.Stat.update_one({}, {"$set": {data.key: int(data.value)}})
        return True
    return False


@app.get("/status2/", tags=["Stats"])
def get_status2():
    ns = None
    try:
        ns = mp.pyroutil.connectNameserver();
        nameserverStatus = 'OK'
    except:
        nameserverStatus = 'Failed'
    
    # get Scheduler status
    schedulerStatus = 'Failed'
    query = ns.yplookup(meta_any={"type:scheduler"})
    try:
        for name, (uri, metadata) in query.items():
            s = Pyro5.api.Proxy(uri)
            st = s.getStatistics()
            schedulerStatus = 'OK'
    except Exception as e:
        print(str(e))
    
    # get DMS status
    if (client):
        DMSStatus = 'OK'
    else:
        DMSStatus = 'Failed'
    
    return {'nameserver': nameserverStatus, 'dms': DMSStatus, 'scheduler': schedulerStatus, 'name':os.environ["MUPIF_VPN_NAME"]}

@app.get("/scheduler-status2/", tags=["Stats"])
def get_scheduler_status2():
    ns = mp.pyroutil.connectNameserver();
    return mp.monitor.schedulerInfo(ns)

@app.get("/ns-status2/", tags=["Stats"])
def get_ns_status2():
    ns = mp.pyroutil.connectNameserver();
    return mp.monitor.nsInfo(ns)

@app.get("/vpn-status2/", tags=["Stats"])
def get_vpn_status2():
    return mp.monitor.vpnInfo(hidePriv=false);



@app.get("/UI/", response_class=HTMLResponse, tags=["User Interface"])
def ui():
    f = open('../ui/app.html', 'r')
    content = f.read()
    f.close()
    return HTMLResponse(content=content, status_code=200)


@app.get("/UI/{file_path:path}", tags=["User Interface"])
def get_ui_file(file_path: str):
    try:
        if file_path.find('..') == -1:
            f = open('../ui/'+file_path, 'r')
            content = f.read()
            f.close()
            return HTMLResponse(content=content, status_code=200)
    except:
        pass
    print(file_path + " not found")
    return None


class M_FindParams(BaseModel):
    filter: dict


@app.put("/EDM/{db}/{type}/find", tags=["EDM"])
def edm_find(db: str, type: str, data: M_FindParams):
    res = client[db][type].find(data.filter)
    ids = [str(r["_id"]) for r in res]
    return ids


if __name__ == '__main__':
    import uvicorn
    uvicorn.run('main:app', host='0.0.0.0', port=8080, reload=True)
