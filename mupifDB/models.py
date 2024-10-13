import datetime
import pydantic
from pydantic.networks import IPvAnyAddress
from pydantic import Field, AliasChoices, BeforeValidator
from typing import Optional,List,Literal,Any,Annotated,Dict
import bson.objectid

DatabaseID=Annotated[str,BeforeValidator(lambda x: str(x) if isinstance(x,bson.objectid.ObjectId) else x)]

# for backwards compat only: load None where (possibly empty) string is required now
Str_EmptyFromNone=Annotated[str,BeforeValidator(lambda x: '' if x is None else x)]
# opposite: saved as empty string, but should be loaded as None if empty
None_FromEmptyStr=Annotated[None,BeforeValidator(lambda x: None if x=='' else x)]


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

class Parent_Model(StrictBase):
    where: str
    id: str

class MongoObj_Model(StrictBase):
    dbID: Optional[DatabaseID]=Field(None,alias='_id') # type: ignore[arg-type]
    parent: Optional[Parent_Model]=None
    def model_dump_db(self):
        '''
        MongoDB-specific enhancement: with _id=None (default), mongoDB would use _id:null instead of treating it as unset. Therefore remove it from the dump if None.
        If it is ever decided that unset (or default-value) attributes should not be dumped, this could be set on the entire model using model_config, but just for a single attribute.
        '''
        ret=self.model_dump(mode='json')
        if '_id' in ret and ret['_id'] is None: del ret['_id']
        return ret

class UseCase_Model(MongoObj_Model):
    ucid: str
    projectName: str=''
    projectLogo: str=''
    Description: str=''


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
    createFrom: Any='' ### XXXX what is this??




class InputOutputBase_Model(StrictBase):
    Name: str
    Description: Optional[str]=None
    Type: str
    Type_ID: str = Field('',alias=AliasChoices('Type_ID','TypeID'))   # XXX deema: '' as default     # type: ignore[arg-type]
    ValueType: Literal['Vector','Scalar','Tensor','ScalarArray','VectorArray','TensorArray','']='Scalar'  # deema: allow ''
    Units: str
    ObjID: Str_EmptyFromNone|List[str]|None = Field([],alias=AliasChoices('ObjID','Obj_ID'))  # type: ignore[arg-type] # XXX: test reads None
    EDMPath: Optional[str]=None
    # allow transparent read-only access as TypeID from python as well
    @property
    def TypeID(self) -> str: return self.Type_ID


class IODataRecordItem_Model(InputOutputBase_Model):
    class Link_Model(StrictBase):
        ExecID: Str_EmptyFromNone='' # XXX: test loads None
        Name: Str_EmptyFromNone=''   # XXX: test loads None
        ObjID: Str_EmptyFromNone=''  # XXX: test loads None
    Value: Optional[dict[str,Any]|str]=None # deema: allow str
    Link: Link_Model=Link_Model()
    FileID: Optional[str]=None
    Compulsory: Optional[bool]=False # deema: allow None
    Object: dict[str,Any]

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
            EDMList: bool=False # XXX: document
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


#class ExecutionQuery_Model(StrictBase):
#    status: Optional[ExecutionStatus_Literal]=Field(None,alias='Status')
#    workflow_version: Optional[int]=Field(None,alias='WorkflowVersion')
#    workflow_id: Optional[str]=Field(None,alias='WorkflowID')
#    label: Optional[str]=None
#    num_limit: int=999999


