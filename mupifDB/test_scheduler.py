import pytest
import sys
import time
import socket
from xprocess import ProcessStarter
from rich.pretty import pprint
from rich import print_json

import logging
logging.basicConfig()
log=logging.getLogger()

def anyPort():
    s=socket.socket()
    s.bind(('',0))
    return s.getsockname()[1]

PORTS={'nameserver':anyPort(),'mongodb':anyPort(),'restApi':anyPort(),'web':anyPort()}
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
        args = [ sys.executable, 'main.py', '--log-level','debug' ]
        timeout = 5
        max_read_lines = 500
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
            'MUPIFDB_REST_SERVER':MUPIFDB_REST_SERVER,
        }
        popen_kwargs = { 'cwd': mupifDB.__path__[0]+'/api' }
        args = [ sys.executable, '-c', 'from mupifDB import workflowscheduler as ws; ws.LOOP_SLEEP_SEC=.5; ws.schedulerStatFile="./sched-stat.json"; ws.main()' ]
        timeout = 10
        max_read_lines = 500
        # pattern = 'Entering main loop to check for Pending executions'
        pattern = 'procInit called'
        terminate_on_interrupt = True
    xprocess.ensure("scheduler",Starter)
    yield
    xprocess.getinfo("scheduler").terminate()

@pytest.fixture
def web(xprocess):
    class Starter(ProcessStarter):
        env = {
            'PYTHONUNBUFFERED':'1',
            'PYTHONPATH':mupifDB.__path__[0],
            #'MUPIF_NS':f'localhost:'+PORTS['nameserver'],
            'MUPIF_LOG_LEVEL':'DEBUG',
            'MUPIFDB_REST_SERVER':'http://localhost:'+PORTS['restApi'],
            'MUPIFDB_WEB_FAKE_AUTH':'1',
            'FLASK_APP':'webapi/index.py',
        }
        popen_kwargs = { 'cwd': mupifDB.__path__[0]+'/..' }
        args = [ sys.executable, '-m', 'flask','run','--host','localhost','--port',PORTS['web']]
        timeout = 10
        max_read_lines = 500
        # pattern = 'Entering main loop to check for Pending executions'
        pattern = ' * Running on http://localhost:'
        terminate_on_interrupt = True
    xprocess.ensure("web",Starter)
    yield
    xprocess.getinfo("web").terminate()


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
    def test_00_status(self,restApi):
        from rich.pretty import pprint
        # this initializes the db
        pprint(restApiControl.getSettings(maybe_init_db=True))
    def test_01_workflowdemo01(self,scheduler):
        wf=wfmini01.MiniWorkflow1()
        md=wf.metadata
        mdd=md.model_dump()
        wid=wf.metadata.ID
        id = mupifDB.restApiControl.postWorkflowFiles(
            'useCase1',
            wfmini01.__file__,
            []
        )
        print(f'Workflow inserted, {id=}')
        wrec=restApiControl.getWorkflowRecord(wid, -1)
        assert wrec.wid==wid
        # print_json(data=wrec)
        weid=restApiControl.createExecution(wid,version=1,ip='localhost')
        print(f'Execution created, {weid=}')
        restApiControl.scheduleExecution(weid)
        print(f'Execution scheduled, {weid=}')
        logID=None
        for i in range(10):
            exe=restApiControl.getExecutionRecord(weid)
            print(f'Execution: {exe.Status}')
            if exe.Status=='Finished': break
            # print(f'Pending executions: {restApiControl.getPendingExecutions(num_limit=10)}')
            time.sleep(1)
        try:
            if exe.ExecutionLog is None:
                print('Execution log not available')
                logContent=None
            logContent,logName=restApiControl.getBinaryFileByID(exe.ExecutionLog)
        except:
            log.error('Error downloading log')
            (log.error if exe.Status!='Finished' else log.info)(logContent.decode('utf-8'))
        assert exe.Status=='Finished'
        assert logContent is not None
    def test_02_web(self,scheduler,web):
        server='http://localhost:'+PORTS['web']
        import requests
        for what in ('about','workflows','workflowexecutions','workflows/1'):
            resp=requests.get(server+f'/{what}')
            open(f'/tmp/{what.replace("/","_")}.html','wb').write(resp.content)
            assert resp.status_code==200

        # time.sleep(10)
    # def test_schedule(self, ex2server):
