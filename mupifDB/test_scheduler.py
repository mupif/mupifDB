import pytest
import sys
import time
import socket
from xprocess import ProcessStarter
from rich.pretty import pprint
from rich import print_json

def anyPort():
    s=socket.socket()
    s.bind(('',0))
    return s.getsockname()[1]

PORTS={'nameserver':anyPort(),'mongodb':anyPort(),'restApi':anyPort()}
PORTS=dict([(k,str(v)) for k,v in PORTS.items()])
pprint(PORTS)

MUPIFDB_REST_SERVER='http://localhost:'+PORTS['restApi']
#import os
#os.environ['MUPIFDB_REST_SERVER']=MUPIFDB_REST_SERVER
import mupifDB
from mupifDB import restApiControl
restApiControl.setRESTserver(MUPIFDB_REST_SERVER)

#import mupif as mp
#mupifExamplesDir=mp.__path__[0]+'/examples/'

sys.path.append(mupifDB.__path__[0]+'/..')
import workflows.mini01 as wfmini01

@pytest.fixture
def nameserver(xprocess):
    class Starter(ProcessStarter):
        env = { 'PYTHONUNBUFFERED':'1' }
        args = [ sys.executable, '-m', 'Pyro5.nameserver', '--port', PORTS['nameserver']]
        timeout = 5
        pattern = 'NS running on '
        terminate_on_interrupt = True
    xprocess.ensure("nameserver",Starter)
    yield
    xprocess.getinfo("nameserver").terminate()

@pytest.fixture
def mongodb(xprocess):
    class Starter(ProcessStarter):
        args = [ '/usr/bin/mongod', '--port', PORTS['mongodb'], '--noauth', '--dbpath=./', '--logpath=/dev/stdout', '--logappend' ]
        timeout = 5
        max_read_lines = 100
        pattern = '"ctx":"listener","msg":"Waiting for connections"'
        terminate_on_interrupt = True
    xprocess.ensure("mongodb",Starter)
    yield
    xprocess.getinfo("mongodb").terminate()

@pytest.fixture
def restApi(xprocess,mongodb,nameserver):
    class Starter(ProcessStarter):
        env = {
            'PYTHONUNBUFFERED':'1',
            'MUPIF_NS':'localhost:'+PORTS['nameserver'],
            'MUPIF_LOG_LEVEL':'DEBUG',
            'MUPIFDB_MONGODB_PORT':PORTS['mongodb'],
            'MUPIFDB_RESTAPI_HOST':'localhost',
            'MUPIFDB_RESTAPI_PORT':PORTS['restApi'],
            'MUPIFDB_REST_SERVER':MUPIFDB_REST_SERVER,
            'PYTHONPATH':mupifDB.__path__[0]+'/..',
        }
        popen_kwargs = { 'cwd': mupifDB.__path__[0]+'/api' }
        args = [ sys.executable, 'main.py' ]
        timeout = 5
        max_read_lines = 50
        pattern = 'Application startup complete.'
        terminate_on_interrupt = True
    xprocess.ensure("restApi",Starter)
    yield
    xprocess.getinfo("restApi").terminate()

@pytest.fixture
def scheduler(xprocess,restApi):
    class Starter(ProcessStarter):
        env = {
            'PYTHONUNBUFFERED':'1',
            'PYTHONPATH':mupifDB.__path__[0]+':'+mupifDB.__path__[0]+'/..',
            'MUPIF_NS':f'localhost:'+PORTS['nameserver'],
            'MUPIF_LOG_LEVEL':'DEBUG',
            'MUPIFDB_REST_SERVER':'http://localhost:'+PORTS['restApi'],
        }
        popen_kwargs = { 'cwd': mupifDB.__path__[0]+'/api' }
        args = [ sys.executable, '-c', 'from mupifDB import workflowscheduler as ws; ws.LOOP_SLEEP_SEC=.5; ws.schedulerStatFile="./sched-stat.json"; ws.main()' ]
        timeout = 10
        max_read_lines = 50
        # pattern = 'Entering main loop to check for Pending executions'
        pattern = 'procInit called'
        terminate_on_interrupt = True
    xprocess.ensure("scheduler",Starter)
    yield
    xprocess.getinfo("scheduler").terminate()

#@pytest.fixture
#def ex2server(scheduler):
#    class Starter(ProcessStarter):
#        env = {
#            'PYTHONUNBUFFERED':'1',
#            'MUPIF_NS':f'localhost:'+PORTS['nameserver'],
#            'MUPIF_LOG_LEVEL':'DEBUG',
#            'PYTHONPATH': mupifExamplesDir+'/02-distrib',
#        }
#        args = [ sys.executable, '-c', 'import application2; import mupif as mp; mp.pyroutil.runAppServer(ns=None,appName="mupif/example/app2",app=application2.Application2())' ]
#        pattern = 'Running mupif/example/app2 at PYRO:obj_'
#        terminate_on_interrupt = True
#        timeout = 3
#    xprocess.ensure('ex2server',Starter)
#    yield
#    xprocess.getinfo('ex2server').terminate()

@pytest.fixture(autouse=True, scope='session')
def test_suite_cleanup():
    yield
    import shutil
    shutil.rmtree('.pytest_cache/d/.xprocess/mongodb',ignore_errors=True)



class TestFoo:
    def test_workflowdemo01(self,scheduler):
        wf=wfmini01.MiniWorkflow1()
        md=lambda k: wf.getMetadata(k)
        wid=md('ID')
        id=mupifDB.workflowmanager.insertWorkflowDefinition(wid=wid,description=md('Description'),source=wfmini01.__file__,useCase='useCase1',workflowInputs=md('Inputs'),workflowOutputs=md('Outputs'),modulename=wf.__module__.split('.')[-1],classname=wf.__class__.__name__,models_md=md('Models'))
        print(f'Workflow inserted, {id=}')
        wrec=restApiControl.getWorkflowRecord(wid)
        assert wrec['wid']==wid
        # print_json(data=wrec)
        weid=restApiControl.createExecution(wid,version='1',ip='localhost')
        #for inp in [
        #    mp.ConstantProperty(value=16., propID=DataID.PID_Concentration,valueType=ValueType.Scalar,unit=mp.U['m'])
        #]:
        #    restApiControl.setExecutionInputObject(weid,
        print(f'Execution created, {weid=}')
        restApiControl.scheduleExecution(weid)
        print(f'Execution scheduled, {weid=}')
        for i in range(10):
            data=restApiControl.getExecutionRecord(weid)
            print(f'Execution status: {data["Status"]}')
            time.sleep(1)
        assert data['Status']=='Finished'


        # time.sleep(10)
    # def test_schedule(self, ex2server):
