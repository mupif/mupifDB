import datetime
import pydantic
from pydantic.networks import IPvAnyAddress
from pydantic import BaseModel, Field, AliasChoices, BeforeValidator
from typing import Any, List, Literal, Optional, Iterator, TypeVar, Callable, Annotated, Dict, Tuple
import bson.objectid
from bson.errors import InvalidId
from pydantic_core import CoreSchema, PydanticCustomError, core_schema

DatabaseID = Annotated[str, BeforeValidator(lambda x: str(x) if isinstance(x,bson.objectid.ObjectId) else x)]

# for backwards compat only: load None where (possibly empty) string is required now
Str_EmptyFromNone=Annotated[str, BeforeValidator(lambda x: '' if x is None else x)]
# opposite: saved as empty string, but should be loaded as None if empty
None_FromEmptyStr=Annotated[None, BeforeValidator(lambda x: None if x == '' else x)]
# bool which can be loaded from None (and becomes False)
Bool_FalseFromNone=Annotated[bool, BeforeValidator(lambda x: False if x is None else x)]


ExecutionStatus_Literal=Literal['Created','Pending','Scheduled','Running','Finished','Failed']

class StrictBase(pydantic.BaseModel):
    model_config=pydantic.ConfigDict(extra='forbid')
    """
    The extra functions override pydantic defaults https://github.com/pydantic/pydantic/issues/10141
    so that aliased fields are always (de)serialized as the alias name, but exposed as the orignal name in python
    (e.g. _id and dbID)
    """
    model_config = pydantic.ConfigDict(populate_by_name=True)

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        kwargs.setdefault("by_alias", True)
        return super().model_dump(**kwargs)

    def model_dump_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("by_alias", True)
        return super().model_dump_json(**kwargs)

class DbRef_Model(StrictBase):
    where: str
    id: str

class TEMP_DbLookup_Model(StrictBase):
    where: str
    attrs: List[str]
    values: List[str|int]

class MongoObjBase_Model(StrictBase):
    dbID: Optional[DatabaseID]=Field(None, alias='_id') # type: ignore[arg-type]
    def model_dump_db(self):
        '''
        MongoDB-specific enhancement: with _id=None (default), mongoDB would use _id:null instead of treating it as unset. Therefore remove it from the dump if None.
        If it is ever decided that unset (or default-value) attributes should not be dumped, this could be set on the entire model using model_config, but just for a single attribute.
        '''
        ret=self.model_dump(mode='json')
        if '_id' in ret and ret['_id'] is None: del ret['_id']
        return ret
    def TEMP_getChildren(self) -> List[Tuple[str,DbRef_Model]]: return []
    def TEMP_getLookupChildren(self) -> List[TEMP_DbLookup_Model]: return []

import abc


class ObjectWithParent_Mixin(abc.ABC):
    @abc.abstractmethod
    def TEMP_mongoParentQuerySet(self) -> Tuple[Dict,Dict]: pass


class GridFSFile_Model(MongoObjBase_Model,ObjectWithParent_Mixin):
    length: int
    chunkSize: int
    uploadDate: datetime.datetime
    metadata: Dict[str,Any]={}
    # enable to make parent mandatory when validating the model
    if 0:
        @pydantic.field_validator('metadata')
        @classmethod
        def validate_metadata_parent(cls, md: Dict[str,Any]):
            assert 'parent' in md
            DbRef_Model.model_validate(md['parent'])
            return md
    # emulate parent as property, dynamically get/set via metadata (API in alignment with MongoObj_Model which store it in the db directly)
    @property
    def parent(self):
        if parent:=self.metadata.get('parent',None): return DbRef_Model.model_validate(parent)
        return None
    @parent.setter
    def parent(self,parent):
        if 'parent' in self.metadata: raise RuntimeError(f'Parent is already defined: {self.metadata["parent"]=}.')
        self.metadata['parent']=parent.model_dump(mode='json')
    def TEMP_mongoParentQuerySet(self) -> Tuple[Dict,Dict]:
        assert 'parent' in self.metadata
        DbRef_Model.model_validate(self.metadata['parent'])
        return {'_id': bson.objectid.ObjectId(self.dbID)}, {'$set':{'metadata':self.metadata}}


class MongoObj_Model(MongoObjBase_Model, ObjectWithParent_Mixin):
    # remove Optional to make parent mandatory when validating the model
    parent: Optional[DbRef_Model]=None
    def TEMP_mongoParentQuerySet(self) -> Tuple[Dict,Dict]:
        assert self.parent is not None
        return {'_id': bson.objectid.ObjectId(self.dbID)}, {'$set':{'parent':self.parent.model_dump(mode='json')}}

class UseCase_Model(MongoObj_Model):
    ucid: str
    Description: str=''
    def TEMP_getLookupChildren(self) -> List[TEMP_DbLookup_Model]: return [TEMP_DbLookup_Model(where=where,attrs=['UseCase'],values=[self.ucid]) for where in ('Workflows','WorkflowsHistory')]

class EDMMappingIDs_Model(StrictBase):
    id: Optional[str]=None
    ids: Optional[List[str]]=[]

class EDMMapping_Model(StrictBase):
    id: Optional[str]=None
    ids: Optional[List[str]]=[]
    Name: Str_EmptyFromNone=''
    EDMEntity: Str_EmptyFromNone=''
    EDMList: bool=False
    DBName: Str_EmptyFromNone=''
    OptionsFilter: Dict[str,str]={}
    createNew: Optional[EDMMappingIDs_Model]=None
    createFrom: Optional[str]=None
    # ioType: Optional[Literal['output','input']]=None

class InputOutputBase_Model(StrictBase):
    Name: str
    Description: Optional[str]=None
    Type: str
    Type_ID: str = Field('',alias=AliasChoices('Type_ID','TypeID'))   # XXX deema: '' as default     # type: ignore[arg-type]
    ValueType: Literal['Vector','Scalar','Tensor','ScalarArray','VectorArray','TensorArray','']='Scalar'  # deema: allow ''
    Units: str
    # XXX: disallow None, since that can be emulated via '' or [] ??, and needs special-casing in code?
    ObjID: Str_EmptyFromNone|List[str]|None = Field([],alias=AliasChoices('ObjID','Obj_ID'))  # type: ignore[arg-type] # XXX: test reads None
    EDMPath: Optional[str]=None
    # allow transparent read-only access as TypeID from python as well
    @property
    def TypeID(self) -> str: return self.Type_ID


class IODataRecordItem_Model(InputOutputBase_Model):
    class Link_Model(StrictBase):
        ExecID: Str_EmptyFromNone='' # XXX: test loads None
        Name:   Str_EmptyFromNone='' # XXX: test loads None
        ObjID:  Str_EmptyFromNone='' # XXX: test loads None
    Value: Optional[dict[str,Any]|str]=None # deema: allow str
    Link: Link_Model=Link_Model()
    # links to fs.files
    FileID: Optional[str]=None
    Compulsory: Optional[bool]=False # deema: allow None
    Object: dict[str,Any]={}
    def TEMP_getChildren(self) -> List[Tuple[str,DbRef_Model]]:
        return [('FileID',DbRef_Model(where=where,id=id)) for where,id in [('fs.files',self.FileID)] if id is not None and id!='']

class IODataRecord_Model(MongoObj_Model):
    DataSet: List[IODataRecordItem_Model]=[]
    Name: str=''
    Type: Literal['Inputs','Outputs']


class WorkflowExecutionCreate_Model(StrictBase):
    wid: str
    version: int
    ip: str
    no_onto: bool=False


class Workflow_Model(MongoObj_Model):
    class Model_Model(StrictBase):
        Name: str
        Jobmanager: str
        Instantiate: Optional[bool]=None
    class IOCard_Model(StrictBase):
        class Input_Model(InputOutputBase_Model):
            Compulsory: bool = Field(...,validation_alias='Required')
            Set_at: Literal['timestep','initialization']='timestep'
        class Output_Model(InputOutputBase_Model):
            EDMList: Bool_FalseFromNone=False # XXX: document
        Inputs: List[Input_Model]
        Outputs: List[Output_Model]
    wid: str
    Description: str
    GridFSID: Optional[str] = None
    UseCase: str
    IOCard: IOCard_Model
    modulename: str
    classname: str
    Models: List[Model_Model]=[] # XXX: test needs unset
    EDMMapping: List[EDMMapping_Model]=[]
    Version: int=1

    def TEMP_getLookupChildren(self) -> List[TEMP_DbLookup_Model]: return [TEMP_DbLookup_Model(where='WorkflowExecutions',attrs=['WorkflowID','WorkflowVersion'],values=[self.wid,self.Version])]


class WorkflowExecution_Model(MongoObj_Model):
    WorkflowID: str
    WorkflowVersion: int
    Status: ExecutionStatus_Literal='Created'
    CreatedDate: datetime.datetime
    SubmittedDate: datetime.datetime|None_FromEmptyStr=None # XXX: deema: '' allowed (instead of None)
    StartDate: datetime.datetime|None_FromEmptyStr=None     # XXX: deema: '' allowed (instead of None)
    EndDate: datetime.datetime|None_FromEmptyStr=None       # XXX: deema: '' allowed (instead of None)
    ExecutionLog: Optional[str]=None
    RequestedBy: str
    UserIP: IPvAnyAddress|str='' # deema: '' as default
    Task_ID: Optional[str]=None
    label: str=''
    Attempts: int=0
    EDMMapping: List[EDMMapping_Model]=[]
    # these are only relevant while the execution being processed
    workflowURI: str|None=None
    loggerURI: str|None=None
    Inputs: str
    Outputs: str
    InputsData: Optional[List[IODataRecordItem_Model] | None]=None
    OutputsData: Optional[List[IODataRecordItem_Model] | None]=None

    def TEMP_getChildren(self) -> List[Tuple[str,DbRef_Model]]:
        return [(attr,DbRef_Model(where=where,id=id)) for where,id,attr in [('IOData',self.Inputs,'Inputs'),('IOData',self.Outputs,'Outputs'),('fs.files',self.ExecutionLog,'ExecutionLog')] if id is not None and id!='']





#class ExecutionQuery_Model(StrictBase):
#    status: Optional[ExecutionStatus_Literal]=Field(None,alias='Status')
#    workflow_version: Optional[int]=Field(None,alias='WorkflowVersion')
#    workflow_id: Optional[str]=Field(None,alias='WorkflowID')
#    label: Optional[str]=None
#    num_limit: int=999999

class MupifDBStatus_Model(StrictBase):
    class Stat_Model(MongoObjBase_Model):
        'Persisted in the DB, so deriving from MongoObjBase_Model.'
        class SchedulerStat_Model(StrictBase):
            load:         float=0.
            processedTasks: int=0
            runningTasks:   int=0
            scheduledTasks: int=0
        scheduler: SchedulerStat_Model=SchedulerStat_Model()
    class ExecutionStatistics_Model(StrictBase):
        totalExecutions:    int=0
        finishedExecutions: int=0
        failedExecutions:   int=0
        createdExecutions:  int=0
        pendingExecutions:  int=0
        scheduledExecutions:int=0
        runningExecutions:  int=0

    mupifDBStatus: Literal['OK','Failed']
    schedulerStatus: Literal['OK','Failed']
    schedulerStat: Stat_Model.SchedulerStat_Model
    totalStat: ExecutionStatistics_Model


#{'mupifDBStatus': mupifDBStatus, 'schedulerStatus': schedulerStatus, 'totalStat': stat, 'schedulerStat': schedulerstat}
# {'mupifDBStatus': mupifDBStatus, 'schedulerStatus': schedulerStatus, 'totalStat': stat, 'schedulerStat': schedulerstat}


# ------------------------------
# API models

class PyObjectId(bson.objectid.ObjectId):
    """Custom type for MongoDB's ObjectId, compatible with Pydantic V2"""

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: Callable[[Any], CoreSchema]) -> CoreSchema:
        """
        Pydantic V2 core schema generation for custom types.
        It defines validation and serialization logic.
        """
        # Validator function: Checks if input is a valid ObjectId (or can be converted)
        def validate_objectid(value: Any) -> bson.objectid.ObjectId:
            if isinstance(value, bson.objectid.ObjectId):
                return value
            
            if isinstance(value, str):
                try:
                    return bson.objectid.ObjectId(value)
                except InvalidId as e:
                    # Raise a PydanticCustomError for better error handling
                    raise PydanticCustomError('invalid_object_id', 'Invalid ObjectId value') from e
            
            raise PydanticCustomError('invalid_object_id', 'ObjectId must be a string or ObjectId instance')


        # Define the core schema
        # The schema uses:
        # 1. `before`: A custom function to validate and convert the input to an ObjectId object.
        # 2. `serialization`: Converts the ObjectId object back to a string for JSON/response.
        return core_schema.json_or_python_schema(
            python_schema=core_schema.no_info_before_validator_function(validate_objectid, core_schema.is_instance_schema(bson.objectid.ObjectId)),
            json_schema=core_schema.str_schema(),
            serialization=core_schema.to_string_ser_schema(),
        )

class User_Model(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    mail: str
    name: str
    surname: str
    password: str
    rights_admin: bool = False
    
    class Config:
        arbitrary_types_allowed = True
        json_encoders = {bson.objectid.ObjectId: str} # Still works, but V2 handles it via the custom type now

class Pagination_Model(StrictBase):
    page: int
    pageSize: int
    totalCount: int

# Usecases

class UsecaseEntityResponse(BaseModel):
    entity: UseCase_Model

class UsecaseCollectionResponse(BaseModel):
    collection: List[UseCase_Model]
    pagination: Pagination_Model

# Workflows

class WorkflowEntityResponse(BaseModel):
    entity: Workflow_Model

class WorkflowCollectionResponse(BaseModel):
    collection: List[Workflow_Model]
    pagination: Pagination_Model

# Executions

class ExecutionEntityResponse(BaseModel):
    entity: WorkflowExecution_Model

class ExecutionCollectionResponse(BaseModel):
    collection: List[WorkflowExecution_Model]
    pagination: Pagination_Model

# Users

class UserEntityResponse(BaseModel):
    entity: User_Model

class UserCollectionResponse(BaseModel):
    collection: List[User_Model]
    pagination: Pagination_Model