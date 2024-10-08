import datetime
import pydantic
from pydantic.networks import IPvAnyAddress
from pydantic import Field, AliasChoices, BeforeValidator
from typing import Optional,List,Literal,Any,Annotated
import bson.objectid

DatabaseID=Annotated[str,BeforeValidator(lambda x: str(x) if isinstance(x,bson.objectid.ObjectId) else x)]

# for backwards compat only: load None where (possibly empty) string is required now
NoneStr=Annotated[str,BeforeValidator(lambda x: '' if x is None else x)]

class Parent_Model(pydantic.BaseModel):
    where: str
    id: str

class MongoObj_Model(pydantic.BaseModel):
    dbID: Optional[DatabaseID]=Field(None,alias=AliasChoices('_id','dbID'),serialization_alias='_id') # type: ignore[arg-type]
    parent: Optional[Parent_Model]=None

class UseCase_Model(MongoObj_Model):
    ucid: str
    projectName: str=''
    projectLogo: str=''
    Description: str=''


class EDMMappingIDs_Model(pydantic.BaseModel):
    id: Optional[str]=None
    ids: Optional[List[str]]=[]


class EDMMapping_Model(pydantic.BaseModel):
    id: Optional[str]=None
    ids: Optional[List[str]]=[]
    Name: NoneStr=''
    EDMEntity: NoneStr=''
    EDMList: bool=False
    DBName: NoneStr=''
    OptionsFilter: Optional[str]=None
    createNew: Optional[EDMMappingIDs_Model]=None
    createFrom: Any='' ### XXXX what is this??




class InputOutputBase_Model(pydantic.BaseModel):
    Name: str
    Description: Optional[str]=None
    Type: str
    Type_ID: str = Field(...,alias=AliasChoices('Type_ID','TypeID'))        # type: ignore[arg-type]
    ValueType: Literal['Vector','Scalar','Tensor','VectorArray']='Scalar'
    Units: str
    ObjID: str|List[str] = Field([],alias=AliasChoices('ObjID','Obj_ID'))  # type: ignore[arg-type]
    EDMPath: Optional[str]=None
    @property
    def TypeID(self) -> str: return self.Type_ID
    # @property.setter(self,val): self.Type_ID=val


class IODataRecordItem_Model(InputOutputBase_Model):
    class Link_Model(pydantic.BaseModel):
        ExecID: str=''
        Name: str=''
        ObjID: str=''
    Value: Optional[dict[str,Any]]=None
    Link: Link_Model=Link_Model()
    FileID: Optional[str]=None
    Compulsory: bool=False
    Object: dict[str,Any]

class IODataRecord_Model(MongoObj_Model):
    DataSet: List[IODataRecordItem_Model]=[]
    Name: str=''
    Type: Literal['Inputs','Outputs']


class WorkflowExecutionCreate_Model(pydantic.BaseModel):
    wid: str
    version: int
    ip: str
    no_onto: bool=False


class Workflow_Model(MongoObj_Model):
    class Model_Model(pydantic.BaseModel):
        Name: str
        Jobmanager: str
        Instantiate: Optional[bool]=None
    class IOCard_Model(pydantic.BaseModel):
        class Input_Model(InputOutputBase_Model):
            Compulsory: bool = Field(...,validation_alias='Required')
            Set_at: Literal['timestep']
        class Output_Model(InputOutputBase_Model):
            EDMList: Optional[str]=None
        Inputs: List[Input_Model]
        Outputs: List[Output_Model]
    wid: str
    Description: str
    GridFSID: Optional[str] = None
    UseCase: str
    IOCard: IOCard_Model
    modulename: str
    classname: str
    Models: List[Model_Model]
    EDMMapping: List[EDMMapping_Model]=[]
    Version: int=1


class WorkflowExecutionBase_Model(MongoObj_Model):
    WorkflowID: str
    WorkflowVersion: int
    Status: Literal['Created','Pending','Scheduled','Running','Finished','Failed']='Created'
    CreatedDate: datetime.datetime
    SubmittedDate: Optional[datetime.datetime]=None
    StartDate: Optional[datetime.datetime]=None
    EndDate: Optional[datetime.datetime]=None
    ExecutionLog: Optional[str]=None
    RequestedBy: str
    UserIP: IPvAnyAddress|str # can be a hostname as well
    Task_ID: Optional[str]=None
    label: str=''
    Attempts: int=0
    EDMMapping: List[EDMMapping_Model]
    # these are only relevant while the execution being processed
    workflowURI: str|None=None
    loggerURI: str|None=None

class WorkflowExecution_Model(WorkflowExecutionBase_Model):
    Inputs: str
    Outputs: str


#class WorkflowExecutionRecord_Model(WorkflowExecutionBase_Model):
#    Inputs: IODataRecord_Model
#    Outputs: IODataRecord_Model



