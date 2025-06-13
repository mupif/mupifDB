import logging
logging.basicConfig()
log=logging.getLogger('mupifDB.api.server')
import argparse, sys
import multiprocessing
multiprocessing.current_process().name='mupifDB-API'
_parser=argparse.ArgumentParser()
_parser.add_argument('--export-openapi',default='',metavar='FILE')
cmdline_opts,_=_parser.parse_known_args() # don't error on other args, such as --log-level, consumed by uvicorn.run
if __name__ == '__main__' and not cmdline_opts.export_openapi:
    import uvicorn
    import os
    host=os.environ.get('MUPIFDB_RESTAPI_HOST','0.0.0.0')
    port=int(os.environ.get('MUPIFDB_RESTAPI_PORT','8005'))
    uvicorn.run('main:app', host=host, port=port, reload=True, log_config=None)

import time

from fastapi import FastAPI, UploadFile, Depends, HTTPException, Request, File
from fastapi.responses import FileResponse, StreamingResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import fastapi, fastapi.exceptions
from pymongo import MongoClient
import tempfile
import zipfile
import importlib
import inspect
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
def NotFoundError(detail):
    return HTTPException(status_code=404, detail=detail)
def ForbiddenError(detail):
    return HTTPException(status_code=403, detail=detail)

from mupifDB import models


client = MongoClient("mongodb://localhost:"+os.environ.get('MUPIFDB_MONGODB_PORT','27017'))
db = client.MuPIF


# Transaction support for MongoDB, used to re-validate a record after validation,
# aborting the transaction automatically if it does not validate.
#
# Transactions need to be enabled in the db (via startup setting, plus a short setup through mongosh — see Makefile in the root of the repo)
# but we might avoid them just as well by setting the schema on the collection (mongo has provisions for that) and then
# the validation would (hopefully) happen automatically
#
import contextlib
from typing import Iterator,TypeVar
from pymongo.client_session import ClientSession
from pymongo.collection import Collection
from pymongo.database import Database

@contextlib.contextmanager
def db_transaction() -> Iterator[ClientSession|None]:
    if 1:
        # return None as session object, this makes the context no-op
        yield None
    else:
        with client.start_session() as session:
            with session.start_transaction():
                yield session


_PermWhat=Literal['read','child','modify']
_PermOn=Literal['self','parent']
_PermObj=models.GridFSFile_Model|models.MongoObj_Model
T=TypeVar('T')

class Perms(object):
    def __init__(self, db: Database):
        self.db=db
    def has(self, obj: Any, perm: _PermWhat='read', on: _PermOn='self') -> bool:
        'TODO: to be implemented'
        return True
    def ensure(self, obj: T, perm:_PermWhat='read', on: _PermOn='self',diag: str|None=None) -> T:
        'Check permissions on the object (read on obj by default) and return it. Raise ForbiddenError if the check fails.'
        # if obj.parent is None: raise ForbiddenError(f'{obj.__class__.__name__}(dbID={obj.dbID}).parent is None!')
        if not self.has(obj=obj,perm=perm, on=on):
            raise ForbiddenError(f'Forbidden {perm} access to {"the parent of" if on=="parent" else ""} {obj.__class__.__name__}(dbID={obj.dbID}){": "+diag if diag else ""}.')
        return obj
    def TODO(*args,**kw): pass
    def filterSelfRead(self,objs: List[T]) -> List[T]: return [obj for obj in objs if self.has(obj,perm='read',on='self')]
    def notRemote(self, request: Request, diag: str):
        import ipaddress
        if request.client is None: raise ForbiddenError('Client address unknown ({diag}).')
        if not ipaddress.ip_address(request.client.host).is_loopback: raise ForbiddenError('Remote access (from {requests.client.host}) forbidden ({diag}).')

perms = Perms(db=db)


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
    else:
        log.info('MUPIFDB_DRY_RUN / --generate-openapi active, not initializing EDM DB connection.')
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
    return perms.filterSelfRead([m:=models.UseCase_Model.model_validate(r) for r in res])


@app.get("/usecases/{uid}", tags=["Usecases"])
def get_usecase(uid: str) -> models.UseCase_Model:
    res = db.UseCases.find_one({"ucid": uid})
    if res is None: raise NotFoundError(f'Database reports no workflow with ucid={uid}.')
    return perms.ensure(models.UseCase_Model.model_validate(res))


@app.get("/usecases/{uid}/workflows", tags=["Usecases"])
def get_usecase_workflows(uid: str) -> List[models.Workflow_Model]:
    res = db.Workflows.find({"UseCase": uid})
    return perms.filterSelfRead([models.Workflow_Model.model_validate(r) for r in res])


@app.post("/usecases/", tags=["Usecases"])
def post_usecase(uc: models.UseCase_Model) -> str:
    perms.ensure(uc,perm='child',on='parent')
    res = db.UseCases.insert_one(uc.model_dump_db())
    return str(res.inserted_id)


# --------------------------------------------------
# Workflows
# --------------------------------------------------

@app.get("/workflows/", tags=["Workflows"])
def get_workflows() -> List[models.Workflow_Model]:
    res = db.Workflows.find()
    return perms.filterSelfRead([models.Workflow_Model.model_validate(r) for r in res])


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in {'py', 'zip', 'txt', 'json', 'xml'}


def insertWorkflowDefinition(*, wid, description, source, useCase, workflowInputs, workflowOutputs, modulename, classname, models_md, EDM_Mapping:List[models.EDMMapping_Model]=[]):
    """
    Inserts new workflow definition into DB.
    Note there is workflow versioning schema: the current (latest) workflow version are stored in workflows collection.
    The old versions (with the same wid but different version) are stored in workflowsHistory.
    @param wid unique workflow id
    @param description Description
    @param source source URL
    @param useCase useCase ID the workflow belongs to
    @param workflowInputs workflow input metadata (list of dicts)
    @param workflowOutputs workflow output metadata (list of dicts)
    @param modulename
    @param classname
    """
    return insertWorkflowDefinition_model(
        source=source,
        rec=models.Workflow_Model(
            # dbID=None, # this should not be necessary, but pyright warns about it?
            wid=wid,
            Description=description,
            UseCase=useCase,
            IOCard=models.Workflow_Model.IOCard_Model(
                Inputs=workflowInputs,
                Outputs=workflowOutputs
            ),
            modulename=modulename,
            classname=classname,
            Models=models_md,
            EDMMapping=EDM_Mapping
        )
    )

pydantic.validate_call(validate_return=True)
def insertWorkflowDefinition_model(source: pydantic.FilePath, rec: models.Workflow_Model):
    with open(source, 'rb') as f:
        file_content = f.read()
        filename = os.path.basename(source)

        file_like_object = io.BytesIO(file_content)

        mock_upload_file = UploadFile(
            filename=filename,
            file=file_like_object,
        )

        # Call the original upload_file function
        gridfs_id = upload_file(mock_upload_file)
        rec.GridFSID = gridfs_id

        # Ensure the BytesIO object is closed
        mock_upload_file.file.close()

    # first check if workflow with wid already exist in workflows
    try:
        w_rec = get_workflow_by_version(rec.wid, -1)
        # the workflow already exists, need to make a new version
        # clone latest version to History
        log.debug(f'{w_rec=}')
        w_rec.dbID = None  # remove original document id
        # w_rec._id=None
        insert_workflow_history(w_rec)
        # update the latest document
        rec.Version = w_rec.Version+1
        res_id = update_workflow(rec).dbID
        if res_id:
            return {"id": res_id, "wid": rec.wid}
        else:
            print("Update failed")
    except client.NotFoundResponse:
        version = 1
        rec.Version = version
        new_id = insert_workflow(rec)
        return {"id": new_id, "wid": rec.wid}

    return None


@app.post("/usecases/{usecaseid}/workflows", tags=["Usecases"])
async def upload_workflow_and_dependencies(
    usecaseid: str,
    workflow_file: UploadFile = File(..., description="The main workflow Python file."),
    additional_files: List[UploadFile] = File([], description="Additional dependency files.")
):
    """
    Endpoint to upload a main workflow file and multiple additional dependency files.

    Args:
        usecaseid: The ID of the use case associated with the workflow.
        workflow_file: The main workflow Python file (e.g., your_workflow.py).
        additional_files: A list of additional files that are dependencies for the workflow.

    Returns:
        A dictionary containing details of the uploaded files, or a redirect
        to the new workflow's detail page on successful registration.
    """
    admin_rights = True

    if not admin_rights:
        raise HTTPException(status_code=403, detail="You don't have permission to perform this action.")

    log.info(f"Uploading workflow for usecase: {usecaseid}")

    new_workflow_id = None
    wid = None
    workflowInputs = None
    workflowOutputs = None
    description = None
    classname = None
    models_md = None
    EDM_Mapping = []
    modulename = ""
    zip_filename = "files.zip"

    try:
        with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB_fastapi') as tempDir:
            zip_full_path = os.path.join(tempDir, zip_filename)
            # Ensure the directory exists if tempfile doesn't fully create it (it should)
            os.makedirs(os.path.dirname(zip_full_path), exist_ok=True)

            with zipfile.ZipFile(zip_full_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:

                # Process the main workflow file
                log.debug(f"Processing main workflow file: {workflow_file.filename}")
                if workflow_file.filename == '':
                    raise HTTPException(status_code=400, detail="Main workflow file cannot be empty.")

                if not allowed_file(workflow_file.filename) and not workflow_file.filename.endswith('.py'):
                    # You might want to be more strict here, e.g., only .py for main workflow
                    raise HTTPException(status_code=400, detail=f"File type not allowed for workflow_file: {workflow_file.filename}")

                main_file_path = os.path.join(tempDir, workflow_file.filename)
                with open(main_file_path, "wb") as f:
                    f.write(await workflow_file.read())
                zf.write(main_file_path, arcname=workflow_file.filename)

                log.debug(f"Analyzing workflow file {workflow_file.filename}")
                modulename = workflow_file.filename.replace(".py", "")
                sys.path.append(tempDir) # Add tempDir to sys.path to import the module

                try:
                    moduleImport = importlib.import_module(modulename)

                    classes = [obj.__name__ for name,obj in inspect.getmembers(moduleImport) if inspect.isclass(obj) and obj.__module__ == modulename]

                    if len(classes) == 1:
                        classname = classes[0]
                        workflowClass = getattr(moduleImport, classname)
                        workflow_instance = workflowClass()
                        wid = workflow_instance.getMetadata('ID')
                        meta = workflow_instance.getAllMetadata()
                        workflowInputs = meta.get('Inputs', [])
                        workflowOutputs = meta.get('Outputs', [])
                        description = meta.get('Description', '')
                        models_md = meta.get('Models', [])
                        EDM_Mapping = meta.get('EDMMapping', [])
                    else:
                        log.error(f"File {workflow_file.filename} contains {len(classes)} classes (must be one). Classes: {classes}")
                        raise HTTPException(
                            status_code=400,
                            detail=f"Workflow file '{workflow_file.filename}' must contain exactly one class. Found {len(classes)}."
                        )
                except (ImportError, AttributeError, Exception) as e:
                    log.error(f"Error importing or analyzing workflow file {workflow_file.filename}: {e}")
                    raise HTTPException(
                        status_code=400,
                        detail=f"Error processing workflow file '{workflow_file.filename}': {e}"
                    )
                finally:
                    # Clean up sys.path to prevent conflicts with subsequent imports
                    if tempDir in sys.path:
                        sys.path.remove(tempDir)


                # Process additional files
                for file in additional_files:
                    log.debug(f"Processing additional file: {file.filename}")
                    if file.filename == '':
                        log.debug(f"Skipping empty additional file upload.")
                        continue # Skip empty uploads

                    # You might want different allowed_file rules for additional files
                    if not allowed_file(file.filename):
                        log.warning(f"Skipping potentially disallowed file type for additional file: {file.filename}")
                        continue # Or raise an error, depending on your policy

                    additional_file_path = os.path.join(tempDir, file.filename)
                    with open(additional_file_path, "wb") as f:
                        f.write(await file.read())
                    zf.write(additional_file_path, arcname=file.filename)


            # After all files are written and zipped, proceed with workflow registration
            if wid is not None and workflowInputs is not None and workflowOutputs is not None and description is not None and classname is not None:
                log.info('Adding workflow to database.')
                new_workflow_id = insertWorkflowDefinition(
                    wid=wid,
                    description=description,
                    source=zip_full_path,
                    useCase=usecaseid,
                    workflowInputs=workflowInputs,
                    workflowOutputs=workflowOutputs,
                    modulename=modulename,
                    classname=classname,
                    models_md=models_md,
                    EDM_Mapping=EDM_Mapping
                )
            else:
                log.error('Workflow data incomplete. Not adding workflow.')
                raise HTTPException(status_code=400, detail="Incomplete workflow metadata. Cannot register workflow.")

        if new_workflow_id is not None:
            return new_workflow_id

        else:
            raise HTTPException(status_code=500, detail="Failed to register workflow, no ID returned.")

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"An unhandled error occurred during workflow upload: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error during workflow upload: {e}")


@app.get("/workflows/{workflow_id}", tags=["Workflows"])
def get_workflow(workflow_id: str) -> models.Workflow_Model:
    return get_workflow_by_version(workflow_id, -1)


@app.get("/workflows/{workflow_id}/version/{workflow_version}", tags=["Workflows"])
def get_workflow_by_version(workflow_id: str, workflow_version: int) -> models.Workflow_Model:
    res = db.Workflows.find_one({"wid": workflow_id})
    if res is None:
        raise NotFoundError(f'Database reports no workflow with wid={workflow_id}.')
    if workflow_version == -1 or res['Version'] == workflow_version:
        return perms.ensure(models.Workflow_Model.model_validate(res))

    res = db.WorkflowsHistory.find_one({"wid": workflow_id, "Version": workflow_version})
    if res is None: raise NotFoundError(f'Database reports no workflow with wid={workflow_id} and Version={workflow_version}.')
    return perms.ensure(models.Workflow_Model.model_validate(res))


@app.patch("/workflows/", tags=["Workflows"])
def update_workflow(wf: models.Workflow_Model) -> models.Workflow_Model:
    perms.ensure(wf,perm='modify')
    # don't write the result if the result after the update does not validate
    with db_transaction() as session:
        # PERM: self write
        res = db.Workflows.find_one_and_update({'wid': wf.wid}, {'$set': wf.model_dump_db()}, return_document=ReturnDocument.AFTER, session=session)
        return models.Workflow_Model.model_validate(res)


@app.post("/workflows/", tags=["Workflows"])
def insert_workflow(wf: models.Workflow_Model) -> str:
    perms.ensure(wf,perm='child',on='parent')
    res = db.Workflows.insert_one(wf.model_dump_db())
    return str(res.inserted_id)


@app.post("/workflows_history/", tags=["Workflows"])
def insert_workflow_history(wf: models.Workflow_Model) -> str:
    perms.ensure(wf,perm='child',on='parent')
    res = db.WorkflowsHistory.insert_one(wf.model_dump_db())
    return str(res.inserted_id)


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
    return perms.filterSelfRead([models.WorkflowExecution_Model.model_validate(r) for r in res])


@app.get("/executions/{uid}", tags=["Executions"])
def get_execution(uid: str) -> models.WorkflowExecution_Model:
    res = db.WorkflowExecutions.find_one({"_id": bson.objectid.ObjectId(uid)})
    if res is None: raise NotFoundError(f'Database reports no execution with uid={uid}.')
    return perms.ensure(models.WorkflowExecution_Model.model_validate(res))

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
    obj=get_edm_execution_uid(uid) # handles perms
    for m in obj.EDMMapping:
        T='input' if (m.createFrom or m.createNew) else 'output'
        if T==iotype and m.EDMEntity==entity:
            if m.id is not None: return [m.id]
            elif m.ids is not None: return m.ids
            else: return []
    return []


@app.post("/executions/create/", tags=["Executions"])
def create_execution(wec: models.WorkflowExecutionCreate_Model) -> str:
    perms.TODO(wec)
    c = mupifDB.workflowmanager.WorkflowExecutionContext.create(workflowID=wec.wid, workflowVer=wec.version, requestedBy='', ip=wec.ip, no_onto=wec.no_onto)
    return str(c.executionID)


@app.post("/executions/", tags=["Executions"])
def insert_execution(data: models.WorkflowExecution_Model) -> str:
    perms.ensure(data,perm='child',on='parent')
    res = db.WorkflowExecutions.insert_one(data.model_dump_db())
    return str(res.inserted_id)

@app.get("/executions/{uid}/inputs/", tags=["Executions"])
def get_execution_inputs(uid: str) -> List[models.IODataRecordItem_Model]:
    ex = get_execution(uid) # checks perms already
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
    perms.ensure(we,perm='modify',on='self')
    if (we.Status == 'Created' and inputs==True) or (we.Status == 'Running' and inputs==False):
        with db_transaction() as session:
            _id=we.Inputs if inputs else we.Outputs
            id_condition = {'_id': bson.objectid.ObjectId(_id)}
            if data_container.link is not None and inputs==True:
                rec = db.IOData.find_one_and_update(id_condition, {'$set': {"DataSet.$[r].Link": data_container.link}}, array_filters=[{"r.Name": name, "r.ObjID": str(obj_id)}], return_document=ReturnDocument.AFTER, session=session)
            elif data_container.object is not None:
                rec = db.IOData.find_one_and_update(id_condition, {'$set': {"DataSet.$[r].Object": data_container.object}}, array_filters=[{"r.Name": name, "r.ObjID": str(obj_id)}], return_document=ReturnDocument.AFTER, session=session)
            else: return False # raise exception??
            if rec is None: raise NotFoundError(f'Database reports no IOData with {_id=}.')
            models.IODataRecord_Model.model_validate(rec)  # if not validated, transaction is aborted
            return True
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
    perms.TODO()
    with db_transaction() as session:
        rec = db.WorkflowExecutions.find_one_and_update({'_id': bson.objectid.ObjectId(uid), "EDMMapping.Name": data.name}, {"$set": {"EDMMapping.$.id": data.value}}, return_document=ReturnDocument.AFTER, session=session)
        models.WorkflowExecution_Model.model_validate(rec)
    return get_execution(uid)

class M_ModifyExecutionOntoBaseObjectIDMultiple(BaseModel):
    data: list[dict]

@app.patch("/executions/{uid}/set_onto_base_object_id_multiple/", tags=["Executions"])
def modify_execution_id_multiple(uid: str, data: List[M_ModifyExecutionOntoBaseObjectID]):
    for d in data: modify_execution_id(uid,d)
    return get_execution(uid)

class M_ModifyExecutionOntoBaseObjectIDs(BaseModel):
    name: str
    value: list[str]

@app.patch("/executions/{uid}/set_onto_base_object_ids/", tags=["Executions"])
def modify_execution_ids(uid: str, data: M_ModifyExecutionOntoBaseObjectIDs):
    with db_transaction() as session:
        rec=db.WorkflowExecutions.find_one_and_update({'_id': bson.objectid.ObjectId(uid), "EDMMapping.Name": data.name}, {"$set": {"EDMMapping.$.ids": data.value}}, return_document=ReturnDocument.AFTER, session=session)
        models.WorkflowExecution_Model.model_validate(rec)
    return get_execution(uid)


class M_ModifyExecution(BaseModel):
    key: str
    value: str

@app.patch("/executions/{uid}", tags=["Executions"])
def modify_execution(uid: str, data: M_ModifyExecution):
    perms.TODO()
    with db_transaction() as session:
        rec=db.WorkflowExecutions.find_one_and_update({'_id': bson.objectid.ObjectId(uid)}, {"$set": {data.key: data.value}}, return_document=ReturnDocument.AFTER, session=session)
        models.WorkflowExecution_Model.model_validate(rec)
    return get_execution(uid)


@app.patch("/executions/{uid}/schedule", tags=["Executions"])
def schedule_execution(uid: str):
    execution_record = perms.ensure(get_execution(uid),perm='modify')
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
    if res is None: raise NotFoundError(f'Database reports no IOData with uid={uid}.')
    return perms.ensure(models.IODataRecord_Model.model_validate(res))

# TODO: pass and store parent data as well
@app.post("/iodata/", tags=["IOData"])
def insert_execution_iodata(data: models.IODataRecord_Model):
    perms.ensure(data,perm='child',on='parent')
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
    if not foundfile: raise NotFoundError('Database reports no file with {uid=}.')
    # open the corresponding record in fs.files to check perms
    perms.ensure(models.GridFSFile_Model.model_validate(db.get_collection('fs.files').find_one({'_id': bson.objectid.ObjectId(uid)})))
    wfile = io.BytesIO(foundfile.read())
    fn = foundfile.filename
    return StreamingResponse(wfile, headers={"Content-Disposition": "attachment; filename=" + fn})

# TODO: needs parent as parameter, so that perms can be checked
@app.post("/file/", tags=["Files"])
def upload_file(file: UploadFile):
    perms.TODO()
    if file:
        fs = gridfs.GridFS(db)
        sourceID = fs.put(file.file, filename=file.filename)
        return str(sourceID)
    return None


@app.get("/property_array_data/{fid}/{i_start}/{i_count}/", tags=["Additional"])
def get_property_array_data(fid: str, i_start: int, i_count: int):
    # XXX: make a direct function call, no need to go through REST API again (or is that for granta?)
    pfile, fn = mupifDB.restApiControl.getBinaryFileByID(fid) # checks perms
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
def insert_log(data: dict, request: Request):
    perms.notRemote(request,'inserting logging data')
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
def set_scheduler_statistics(data: M_ModifyStatistics, request: Request):
    perms.notRemote(request,'modifying scheduler statistics')
    if data.key in ["scheduler.runningTasks", "scheduler.scheduledTasks", "scheduler.load", "scheduler.processedTasks"]:
        res = db.Stat.update_one({}, {"$set": {data.key: int(data.value)}})
        return True
    return False


@app.get("/status2/", tags=["Stats"])
def get_status2():
    ns = None
    try:
        ns = mp.pyroutil.connectNameserver()
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
    ns = mp.pyroutil.connectNameserver()
    return mp.monitor.schedulerInfo(ns)


@app.get("/ns-status2/", tags=["Stats"])
def get_ns_status2():
    ns = mp.pyroutil.connectNameserver()
    return mp.monitor.nsInfo(ns)


@app.get("/vpn-status2/", tags=["Stats"])
def get_vpn_status2():
    return mp.monitor.vpnInfo(hidePriv=False)


@app.get("/jobmans-status2/", tags=["Stats"])
def get_jobmans_status2():
    ns = mp.pyroutil.connectNameserver()
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


