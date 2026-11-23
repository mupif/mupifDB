import pytest
import sys
import time
import socket
from rich.pretty import pprint
from rich import print_json

import logging
logging.basicConfig()
log=logging.getLogger()


from pathlib import Path
#sys.path.append(Path(__file__).absolute()/'..')

from . import models

from bson.objectid import ObjectId

sampleId=ObjectId('6709724d88e11a5a42f7caea')

class TestModels:
    def test_01_strictbase(self):
        with pytest.raises(TypeError): models.StrictBase.model_validate(foo='bar')
    def test_02_mongoobj(self):
        # test DatabasID annotated type (converts bson.objectid.ObjectId to str, when loading from database)
        m=models.MongoObj_Model.model_validate(dict(_id=sampleId))
        # passing _id from python side (not as dict, e.g. from db) is an error, since python uses dbID
        with pytest.raises(TypeError): models.MongoObj_Model.model_validate(_id=sampleId)
        assert m.dbID==str(sampleId)
        assert type(m.dbID)==str
        # dbID attribute is dumped as _id, automatically (StrictBase defined model_dump with by_alias=True by default)
        assert '_id' in m.model_dump() and 'dbID' not in m.model_dump()
        # model_dump_db: don't remove _id when not None
        assert '_id' in m.model_dump_db()
        m2=models.MongoObj_Model()
        # but here _id is None (default), so will not be in the dump at all
        assert '_id' not in m2.model_dump_db()

    def test_03_execution_profiles(self):
        # workflow records store the models' ModelID and the execution profiles of the metadata
        # (as mupif's getAllMetadata() gives them, with keys the record does not know)
        md=dict(wid='w',Description='',UseCase='u',IOCard=dict(Inputs=[],Outputs=[]),modulename='m',classname='C',
            Models=[dict(Name='m',Jobmanager='CVUT.demo01',ModelID='demo01',Module='',Class='')],
            ExecutionProfiles=[dict(Name='fast',Cost='$',Description='',Models=[dict(Name='m',RequiredModelMetadata=['Runtime_seconds'],OptionalModelMetadata=[])])])
        w=models.Workflow_Model.model_validate(md)
        assert w.Models[0].ModelID=='demo01'
        assert w.ExecutionProfiles[0].Models[0].RequiredModelMetadata==['Runtime_seconds']
        # records stored before: no ModelID, no profiles (only the default one)
        old=models.Workflow_Model.model_validate({k:v for k,v in md.items() if k!='ExecutionProfiles'}|dict(Models=[dict(Name='m',Jobmanager='CVUT.demo01')]))
        assert old.ExecutionProfiles==[] and old.Models[0].ModelID is None
    def test_04_execution_model_servers(self):
        # executions: the default profile and no model servers unless set
        e=models.WorkflowExecution_Model.model_validate(dict(WorkflowID='w',WorkflowVersion=1,CreatedDate='2026-11-23T10:00:00',RequestedBy='x',Inputs='',Outputs=''))
        assert e.ExecutionProfileIndex==-1 and e.ModelServers=={}
        e=models.WorkflowExecution_Model.model_validate(e.model_dump()|dict(ExecutionProfileIndex=1,ModelServers={'m':'CVUT.demo01~node2'}))
        assert e.ModelServers=={'m':'CVUT.demo01~node2'}
        assert models.WorkflowExecutionCreate_Model(wid='w',version=1).ExecutionProfileIndex==-1
