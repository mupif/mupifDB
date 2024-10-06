import datetime
import pydantic
from pydantic.networks import IPvAnyAddress
from pydantic import Field, AliasChoices
from typing import Optional,List,Literal

class Parent_Model(pydantic.BaseModel):
    where: str
    id: str

class MongoObj_Model(pydantic.BaseModel):
    dbID: Optional[str]=Field(None,alias=AliasChoices('_id','dbID'),serialization_alias='_id') # type: ignore[arg-type]
    parent: Optional[Parent_Model]=None

class UseCase_Model(MongoObj_Model):
    ucid: str
    Description: str

class EDMMapping_Model(pydantic.BaseModel):
    class EDMObject_Model(pydantic.BaseModel):
        id: Optional[str]=None
        ids: Optional[str]=None
    Name: str
    EDMEntity: str
    DBName: str
    id: Optional[str]=None
    ids: List[str]=[]
    # TODO: createFrom: ...
    createNew: Optional[EDMObject_Model]=None

class WorkflowExecution_Model(MongoObj_Model):
    WorkflowID: str
    WorkflowVersion: int
    Status: Literal['Created','Pending','Scheduled','Running','Finished','Failed']
    CreatedDate: datetime.datetime
    SubmittedDate: Optional[datetime.datetime]
    StartDate: Optional[datetime.datetime]
    EndDate: Optional[datetime.datetime]
    ExecutionLog: Optional[str]=None
    RequestedBy: str
    UserIP: IPvAnyAddress|str # can be a hostname as well
    Inputs: str
    Outputs: str
    Task_ID: str
    label: str
    Attempts: int
    EDMMapping: List[EDMMapping_Model]
    # these are only relevant while the execution being processed
    workflowURI: str|None=None
    loggerURI: str|None=None

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
        class InputOutputBase_Model(pydantic.BaseModel):
            Name: str
            Description: Optional[str]=None
            Type: str
            Type_ID: str = Field(...,alias=AliasChoices('Type_ID','TypeID'))                   # type: ignore[arg-type]
            ValueType: Literal['Vector','Scalar','Tensor','VectorArray']='Scalar'
            Units: str
            ObjID: Optional[str|List[str]] = Field(None,alias=AliasChoices('ObjID','Obj_ID'))  # type: ignore[arg-type]
            @property
            def TypeID(self): return self.Type_ID
            # @property.setter(self,val): self.Type_ID=val
        class Input_Model(InputOutputBase_Model):
            Compulsory: bool = Field(...,validation_alias='Required')
            Set_at: Literal['timestep']
        class Output_Model(InputOutputBase_Model):
            EDMPath: Optional[str]=None
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



