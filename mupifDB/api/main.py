import logging
logging.basicConfig()
log=logging.getLogger('mupifDB.api.server')
import argparse, sys
_parser=argparse.ArgumentParser()
_parser.add_argument('--export-openapi',default='',metavar='FILE')
cmdline_opts,_=_parser.parse_known_args() # don't error on other args, such as --log-level, consumed by uvicorn.run
if __name__ == '__main__' and not cmdline_opts.export_openapi:
    print('SERVING')
    import uvicorn
    import os
    host=os.environ.get('MUPIFDB_RESTAPI_HOST','0.0.0.0')
    port=int(os.environ.get('MUPIFDB_RESTAPI_PORT','8005'))
    uvicorn.run('main:app', host=host, port=port, reload=True, log_config=None)

import time

from fastapi import FastAPI, UploadFile, Depends, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import fastapi, fastapi.exceptions
from pymongo import MongoClient
import tempfile
import gridfs
import typing
import io
import bson, bson.objectid
import psutil
from pymongo import ReturnDocument
from pydantic import BaseModel
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
import mupifDB
import mupif as mp
import mupif.pyroutil
import Pyro5.api
import pydantic
import json
from typing import Any,List,Literal
from rich import print_json
from rich.pretty import pprint

# shorthands for common exceptions
def NotFoundError(detail): return HTTPException(status_code=404,detail=detail)


from mupifDB import models

client = MongoClient("mongodb://localhost:"+os.environ.get('MUPIFDB_MONGODB_PORT','27017'))
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
        "name": "Settings",
    },
    {
        "name": "User Interface",
    },
]


app = FastAPI(openapi_tags=tags_metadata)


@app.exception_handler(fastapi.exceptions.RequestValidationError)
async def validation_exception_handler(request: fastapi.Request, exc: fastapi.exceptions.RequestValidationError):
    exc_str = f'{exc}'.replace('\n', ' ').replace('   ', ' ')
    log.error(f'{request}: {exc_str}')
    content = {'status_code': 422, 'message': exc_str, 'data': None}
    return fastapi.responses.JSONResponse(content=content, status_code=fastapi.status.HTTP_422_UNPROCESSABLE_ENTITY)


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
    # when imported at readthedocs, don't try to connect to the DB (no DB running there)
    if 'MUPIFDB_DRY_RUN' not in os.environ and not cmdline_opts.export_openapi:
        edm.initializeEdm(client)
    else: log.info('MUPIFDB_DRY_RUN / --generate-openapi active, not initializing EDM DB connection.')
    app.include_router(edm.dms3.router)



# --------------------------------------------------
# Default
# --------------------------------------------------

@app.get("/")
def read_root():
    return {"MuPIF": "API"}


# --------------------------------------------------
# Users
# --------------------------------------------------

# @app.get("/users/{user_ip}", tags=["Users"])
# def get_user(user_ip: str):
#     res = db.Users.find_one({'IP': user_ip})
#     if res:
#         return fix_id(res)
#     return None


# --------------------------------------------------
# Usecases
# --------------------------------------------------

@app.get("/usecases/", tags=["Usecases"])
def get_usecases() -> List[models.UseCase_Model]:
    res = db.UseCases.find()
    return [models.UseCase_Model.model_validate(r) for r in res]


@app.get("/usecases/{uid}", tags=["Usecases"])
def get_usecase(uid: str) -> models.UseCase_Model:
    res = db.UseCases.find_one({"ucid": uid})
    if res is None: raise NotFoundError(f'Database reports no workflow with ucid={uid}.')
    return models.UseCase_Model.model_validate(res)


@app.get("/usecases/{uid}/workflows", tags=["Usecases"])
def get_usecase_workflows(uid: str) -> List[models.Workflow_Model]:
    res = db.Workflows.find({"UseCase": uid})
    return [models.Workflow_Model.model_validate(r) for r in res]


@app.post("/usecases/", tags=["Usecases"])
def post_usecase(uc: models.UseCase_Model) -> str:
    res = db.UseCases.insert_one(uc.model_dump_db())
    return str(res.inserted_id)


# --------------------------------------------------
# Workflows
# --------------------------------------------------

@app.get("/workflows/", tags=["Workflows"])
def get_workflows() -> List[models.Workflow_Model]:
    res = db.Workflows.find()
    return [models.Workflow_Model.model_validate(r) for r in res]

@app.get("/workflows/{workflow_id}", tags=["Workflows"])
def get_workflow(workflow_id: str) -> models.Workflow_Model:
    res = db.Workflows.find_one({"wid": workflow_id})
    if res is None: raise NotFoundError(f'Database reports no workflow with wid={workflow_id}.')
    return models.Workflow_Model.model_validate(res)

@app.patch("/workflows/", tags=["Workflows"])
def update_workflow(wf: models.Workflow_Model) -> models.Workflow_Model:
    res = db.Workflows.find_one_and_update({'wid': wf.wid}, {'$set': wf.model_dump_db()}, return_document=ReturnDocument.AFTER)
    return models.Workflow_Model.model_validate(res)


@app.post("/workflows/", tags=["Workflows"])
def insert_workflow(wf: models.Workflow_Model) -> str:
    res = db.Workflows.insert_one(wf.model_dump_db())
    return str(res.inserted_id)


@app.post("/workflows_history/", tags=["Workflows"])
def insert_workflow_history(wf: models.Workflow_Model) -> str:
    res = db.WorkflowsHistory.insert_one(wf.model_dump_db())
    return str(res.inserted_id)


# --------------------------------------------------
# Workflows history
# --------------------------------------------------

@app.get("/workflows_history/{workflow_id}/{workflow_version}", tags=["Workflows"])
def get_workflow_history(workflow_id: str, workflow_version: int) -> models.Workflow_Model:
    # print(f'AAA: {workflow_id=} {workflow_version=}')
    res = db.WorkflowsHistory.find_one({"wid": workflow_id, "Version": workflow_version})
    if res is None: raise NotFoundError(f'Database reports no workflow with wid={workflow_id} and Version={workflow_version}.')
    return models.Workflow_Model.model_validate(res)

# --------------------------------------------------
# Executions
# --------------------------------------------------

@app.get("/executions/", tags=["Executions"])
def get_executions(status: str = "", workflow_version: int = 0, workflow_id: str = "", num_limit: int = 0, label: str = "") -> List[models.WorkflowExecution_Model]:
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
    #print(200*'!')
    #pprint(filtering)
    res = db.WorkflowExecutions.find(filtering).sort('SubmittedDate', 1).limit(num_limit)
    # pprint(res)
    return [models.WorkflowExecution_Model.model_validate(r) for r in res]


@app.get("/executions/{uid}", tags=["Executions"])
def get_execution(uid: str) -> models.WorkflowExecution_Model:
    res = db.WorkflowExecutions.find_one({"_id": bson.objectid.ObjectId(uid)})
    if res is None: raise NotFoundError(f'Database reports no execution with uid={uid}.')
    return models.WorkflowExecution_Model.model_validate(res)


# FIXME: how is this different from get_execution??
@app.get("/edm_execution/{uid}", tags=["Executions"])
def get_edm_execution_uid(uid: str) -> models.WorkflowExecution_Model:
    #res = db.WorkflowExecutions.find_one({"_id": bson.objectid.ObjectId(uid)})
    #if res is None: raise NotFoundError(f'Database reports no edm_execution with uid={uid}.')
    #obj=models.WorkflowExecution_Model.model_validate(res)
    #return obj
    return get_execution(uid)

@app.get("/edm_execution/{uid}/{entity}/{iotype}", tags=["Executions"])
def get_edm_execution_uid_entity_iotype(uid: str, entity: str, iotype: Literal['input','output']) -> List[str]:
    obj=get_edm_execution_uid(uid)
    for m in obj.EDMMapping:
        T='input' if (m.createFrom or m.createNew) else 'output'
        if T==iotype and m.EDMEntity==entity:
            if m.id is not None: return [m.id]
            elif m.ids is not None: return m.ids
            else: return []
    return []


@app.post("/executions/create/", tags=["Executions"])
def create_execution(wec: models.WorkflowExecutionCreate_Model) -> str:
    c = mupifDB.workflowmanager.WorkflowExecutionContext.create(workflowID=wec.wid, workflowVer=wec.version, requestedBy='', ip=wec.ip, no_onto=wec.no_onto)
    return str(c.executionID)


@app.post("/executions/", tags=["Executions"])
def insert_execution(data: models.WorkflowExecution_Model) -> str:
    res = db.WorkflowExecutions.insert_one(data.model_dump_db())
    return str(res.inserted_id)

@app.get("/executions/{uid}/inputs/", tags=["Executions"])
def get_execution_inputs(uid: str) -> List[models.IODataRecordItem_Model]:
    ex = get_execution(uid)
    if ex.Inputs: return models.IODataRecord_Model.model_validate(db.IOData.find_one({'_id': bson.objectid.ObjectId(ex.Inputs)})).DataSet
    return []


@app.get("/executions/{uid}/outputs/", tags=["Executions"])
def get_execution_outputs(uid: str) -> List[models.IODataRecordItem_Model]:
    ex = get_execution(uid)
    if ex.Outputs: return models.IODataRecord_Model.model_validate(db.IOData.find_one({'_id': bson.objectid.ObjectId(ex.Outputs)})).DataSet
    return []

@app.get("/executions/{uid}/livelog/{num}", tags=["Executions"])
def get_execution_livelog(uid: str, num: int) -> List[str]:
    ex = get_execution(uid)
    if ex.loggerURI is not None:
        import Pyro5.api
        import serpent
        import pickle
        fmt=logging.Formatter(fmt='%(asctime)s %(levelname)s %(filename)s:%(lineno)s %(message)s')
        proxy=Pyro5.api.Proxy(ex.loggerURI)
        proxy._pyroTimeout=5
        ll=proxy.tail(num,raw=True)
        if isinstance(ll,dict): ll=serpent.tobytes(ll)
        ll=pickle.loads(ll)                                  # type: ignore
        return [fmt.format(rec) for rec in ll]
    # perhaps raise exception instead?
    return []


def get_execution_io_item(uid: str, name, obj_id: str, inputs: bool) -> models.IODataRecordItem_Model:
    ex = get_execution(uid)
    data = models.IODataRecord_Model.model_validate(db.IOData.find_one({'_id': bson.objectid.ObjectId(ex.Inputs if inputs else ex.Outputs)}))
    for elem in data.DataSet:
        if elem.Name == name and elem.ObjID == obj_id:
            return elem
    raise NotFoundError('Execution weid={uid}, {"inputs" it inputs else "outputs"}: no element with {name=} & {obj_id=}.')


@app.get("/executions/{uid}/input_item/{name}/{obj_id}/", tags=["Executions"])
def get_execution_input_item(uid: str, name: str, obj_id: str) -> models.IODataRecordItem_Model:
    return get_execution_io_item(uid, name, obj_id, inputs=True)


@app.get("/executions/{uid}/output_item/{name}/{obj_id}/", tags=["Executions"])
def get_execution_output_item(uid: str, name: str, obj_id: str) -> models.IODataRecordItem_Model:
    return get_execution_io_item(uid, name, obj_id, inputs=False)


@app.get("/executions/{uid}/input_item/{name}//", tags=["Executions"])
def _get_execution_input_item(uid: str, name: str) -> models.IODataRecordItem_Model:
    return get_execution_io_item(uid, name, '', inputs=True)


@app.get("/executions/{uid}/output_item/{name}//", tags=["Executions"])
def _get_execution_output_item(uid: str, name: str) -> models.IODataRecordItem_Model:
    return get_execution_io_item(uid, name, '', inputs=False)



class M_IODataSetContainer(BaseModel):
    link: typing.Optional[dict] = None
    object: typing.Optional[dict] = None

# FIXME: validation
def set_execution_io_item(uid: str, name: str, obj_id: str, inputs: bool, data_container):
    we = get_execution(uid)
    if (we.Status == 'Created' and inputs==True) or (we.Status == 'Running' and inputs==False):
        id_condition = {'_id': bson.objectid.ObjectId(we.Inputs if inputs else we.Outputs)}
        if data_container.link is not None and inputs==True:
            res = db.IOData.update_one(id_condition, {'$set': {"DataSet.$[r].Link": data_container.link}}, array_filters=[{"r.Name": name, "r.ObjID": str(obj_id)}])
            return res.matched_count > 0
        if data_container.object is not None:
            res = db.IOData.update_one(id_condition, {'$set': {"DataSet.$[r].Object": data_container.object}}, array_filters=[{"r.Name": name, "r.ObjID": str(obj_id)}])
            return res.matched_count > 0
    return False


@app.patch("/executions/{uid}/input_item/{name}/{obj_id}/", tags=["Executions"])
def set_execution_input_item(uid: str, name: str, obj_id: str, data: M_IODataSetContainer):
    return set_execution_io_item(uid, name, obj_id, True, data)


@app.patch("/executions/{uid}/output_item/{name}/{obj_id}/", tags=["Executions"])
def set_execution_output_item(uid: str, name: str, obj_id: str, data: M_IODataSetContainer):
    return set_execution_io_item(uid, name, obj_id, False, data)


@app.patch("/executions/{uid}/input_item/{name}//", tags=["Executions"])
def _set_execution_input_item(uid: str, name: str, data: M_IODataSetContainer):
    return set_execution_io_item(uid, name, '', True, data)


@app.patch("/executions/{uid}/output_item/{name}//", tags=["Executions"])
def _set_execution_output_item(uid: str, name: str, data: M_IODataSetContainer):
    return set_execution_io_item(uid, name, '', False, data)


class M_ModifyExecutionOntoBaseObjectID(BaseModel):
    name: str
    value: str

@app.patch("/executions/{uid}/set_onto_base_object_id/", tags=["Executions"])
def modify_execution_id(uid: str, data: M_ModifyExecutionOntoBaseObjectID):
    db.WorkflowExecutions.update_one({'_id': bson.objectid.ObjectId(uid), "EDMMapping.Name": data.name}, {"$set": {"EDMMapping.$.id": data.value}})
    return get_execution(uid)

class M_ModifyExecutionOntoBaseObjectIDMultiple(BaseModel):
    data: list[dict]

@app.patch("/executions/{uid}/set_onto_base_object_id_multiple/", tags=["Executions"])
def modify_execution_id_multiple(uid: str, data: List[M_ModifyExecutionOntoBaseObjectID]):
    for d in data:
        db.WorkflowExecutions.update_one({'_id': bson.objectid.ObjectId(uid), "EDMMapping.Name": d.name}, {"$set": {"EDMMapping.$.id": d.value}})
    return get_execution(uid)


class M_ModifyExecutionOntoBaseObjectIDs(BaseModel):
    name: str
    value: list[str]

@app.patch("/executions/{uid}/set_onto_base_object_ids/", tags=["Executions"])
def modify_execution_ids(uid: str, data: M_ModifyExecutionOntoBaseObjectIDs):
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
    if execution_record.Status == 'Created' or True:
        data = type('', (), {})()
        mod=M_ModifyExecution(key = "Status",value = "Pending")
        return modify_execution(uid, mod)
    return None


# --------------------------------------------------
# IOData
# --------------------------------------------------

@app.get("/iodata/{uid}", tags=["IOData"])
def get_execution_iodata(uid: str) -> models.IODataRecord_Model:
    res = db.IOData.find_one({'_id': bson.objectid.ObjectId(uid)})
    if res is None: raise NotFoundError(f'Database reports no iodata with uid={uid}.')
    return models.IODataRecord_Model.model_validate(res)

# TODO: pass and store parent data as well
@app.post("/iodata/", tags=["IOData"])
def insert_execution_iodata(data: models.IODataRecord_Model):
    res = db.IOData.insert_one(data.model_dump_db())
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
    return StreamingResponse(wfile, headers={"Content-Disposition": "attachment; filename=" + fn})

# TODO: store parent as metadata, validate the fs.files record as well
@app.post("/file/", tags=["Files"])
def upload_file(file: UploadFile):
    if file:
        fs = gridfs.GridFS(db)
        sourceID = fs.put(file.file, filename=file.filename)
        return str(sourceID)
    return None


@app.get("/property_array_data/{fid}/{i_start}/{i_count}/", tags=["Additional"])
def get_property_array_data(fid: str, i_start: int, i_count: int):
    # XXX: make a direct function call, no need to go through REST API again (or is that for granta?)
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
def get_field_as_vtu(fid: str, tdir=Depends(get_temp_dir)):
    # XXX: make a direct function call, no need to go through REST API again (or is that for granta?)
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
def insert_log(data: dict):
    res = db.Logs.insert_one(data)
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
                if not psutil.pid_exists(pid):
                    schedulerStatus = 'Failed'
            except (OSError, ValueError):
                schedulerStatus = 'Failed'
    rec=db.Stat.find_one()
    # pprint(rec)
    statRec=models.MupifDBStatus_Model.Stat_Model.model_validate(rec)
    return models.MupifDBStatus_Model(
        schedulerStatus=schedulerStatus,
        mupifDBStatus=mupifDBStatus,
        # roundtrip to execution_statistics below via API request back to us? wth
        totalStat=mupifDB.schedulerstat.getGlobalStat(),
        schedulerStat=statRec.scheduler
    )


@app.get("/execution_statistics/", tags=["Stats"])
def get_execution_statistics() -> models.MupifDBStatus_Model.ExecutionStatistics_Model:
    res = client.MuPIF.WorkflowExecutions.aggregate([
        {"$group": {"_id": "$Status", "count": {"$sum": 1}}}
    ])
    vals = {}
    tot = 0
    for r in res:
        vals[r['_id']] = r['count']
        tot += r['count']

    return models.MupifDBStatus_Model.ExecutionStatistics_Model(
        totalExecutions = tot,
        finishedExecutions = vals.get('Finished', 0),
        failedExecutions = vals.get('Failed', 0),
        createdExecutions = vals.get('Created', 0),
        pendingExecutions = vals.get('Pending', 0),
        scheduledExecutions = vals.get('Scheduled', 0),
        runningExecutions = vals.get('Running', 0),
    )


@app.get("/settings/", tags=["Settings"])
def get_settings():
    table = db.Settings
    for s in table.find():
        del s['_id']
        return s
    return {}


@app.get("/database/maybe_init",tags=["Settings"])
def db_init():
    # probably initialized already
    if 'Settings' in db.list_collection_names(): return False
    for coll,rec in [
        ('Settings',{'projectName':'TEST','projectLogo':'https://raw.githubusercontent.com/mupif/mupifDB/bd297a4a719336cd9672cfe73f31f7cbe2b4e029/webapi/static/images/mupif-logo.png'}),
        ('UseCases',models.UseCase_Model(projectName='TEST',projectLogo='https://raw.githubusercontent.com/mupif/mupifDB/bd297a4a719336cd9672cfe73f31f7cbe2b4e029/webapi/static/images/mupif-logo.png',ucid='1',Description='Test usecase').model_dump()),
        ('Stat',models.MupifDBStatus_Model.Stat_Model().model_dump(mode="json")),
        ('Workflows',None),
        ('WorkflowsHistory',None),
        ('WorkflowExecutions',None),
        ('IOData',None)
    ]:
        try:
            c=db.create_collection(coll)
            if rec is None: continue
            try: c.insert_one(rec)
            except Exception: log.exception(f'Error populating initial collection {coll} with {rec}.')
        except Exception as e: log.exception(f'Error creating initial collection {coll}.')
    return True




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
        ns = mupif.pyroutil.connectNameserver()
        nameserverStatus = 'OK'
    except:
        nameserverStatus = 'Failed'
    # get Scheduler status
    schedulerStatus = 'Failed'
    query = ns.yplookup(meta_any={"type:scheduler"}) # type: ignore
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
    ns = mupif.pyroutil.connectNameserver();
    return mp.monitor.schedulerInfo(ns)

@app.get("/ns-status2/", tags=["Stats"])
def get_ns_status2():
    ns = mupif.pyroutil.connectNameserver();
    return mp.monitor.nsInfo(ns)

@app.get("/vpn-status2/", tags=["Stats"])
def get_vpn_status2():
    return mupif.monitor.vpnInfo(hidePriv=False)

@app.get("/jobmans-status2/", tags=["Stats"])
def get_jobmans_status2():
    ns = mupif.pyroutil.connectNameserver();
    return mp.monitor.jobmanInfo(ns)


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


if __name__ == '__main__' and cmdline_opts.export_openapi:
    open(cmdline_opts.export_openapi,'w').write(json.dumps(app.openapi(),indent=2))


