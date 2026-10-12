import pytest
import sys
import os
import json
import time
import socket
import shutil
import tempfile
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

# mongod binary: $MUPIFDB_TEST_MONGOD, else the one on $PATH, else the traditional system location
MONGOD=os.environ.get('MUPIFDB_TEST_MONGOD') or shutil.which('mongod') or '/usr/bin/mongod'

# The REST API requires authentication. The test account is registered (and made admin, so that it
# may upload workflows) by the restApi fixture. The scheduler subprocess reads it from the file
# given by API_CREDENTIALS_FILE; this process gets it via setAPICredentials, because pytest imports
# this module as mupifDB.test_scheduler, i.e. the REST client is already imported at this point.
API_CREDENTIALS={'username':'scheduler-test@localhost','password':'scheduler-test-password'}
API_CREDENTIALS_FILE=os.path.join(tempfile.mkdtemp(prefix='mupifdb-test-'),'api-credentials.json')
with open(API_CREDENTIALS_FILE,'w') as f: json.dump(API_CREDENTIALS,f)

import mupifDB
from mupifDB import restApiControl
restApiControl.setRESTserver(MUPIFDB_REST_SERVER)
restApiControl.setAPICredentials(**API_CREDENTIALS)

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
        args = [ MONGOD, '--port', PORTS['mongodb'], '--noauth', '--dbpath=./', '--logpath=/dev/stdout', '--logappend' ]
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
            # fixed JWT key: without it every API restart (each test restarts the fixture) picks a
            # random key and invalidates the access token the client has cached
            'APP_SECRET_KEY':'mupifdb-test-jwt-key',
        }
        popen_kwargs = { 'cwd': mupifDB.__path__[0]+'/api' }
        args = [ sys.executable, 'main.py', '--log-level','debug' ]
        timeout = 5
        max_read_lines = 500
        pattern = 'Application startup complete.'
        terminate_on_interrupt = True
    xprocess.ensure("restApi",Starter)
    _createTestAccount()
    yield
    xprocess.getinfo("restApi").terminate()

def _createTestAccount():
    '''
    Register the test account (see API_CREDENTIALS) through the REST API and grant it admin
    rights directly in MongoDB, which is the only way to obtain an admin without an existing one.
    Idempotent: an already registered account is left as it is.
    '''
    import requests, pymongo
    requests.post(MUPIFDB_REST_SERVER+'/api/register',data=API_CREDENTIALS,timeout=10)
    db=pymongo.MongoClient('mongodb://localhost:'+PORTS['mongodb'],directConnection=True).MuPIF
    db.user.update_one({'mail':API_CREDENTIALS['username']},{'$set':{'rights_admin':True}})

@pytest.fixture
def scheduler(xprocess,restApi):
    class Starter(ProcessStarter):
        env = {
            'PYTHONUNBUFFERED':'1',
            'PYTHONPATH':mupifDB.__path__[0]+':'+mupifDB.__path__[0]+'/..',
            'MUPIF_NS':f'localhost:'+PORTS['nameserver'],
            'MUPIF_LOG_LEVEL':'DEBUG',
            'MUPIFDB_REST_SERVER':MUPIFDB_REST_SERVER,
            'API_CREDENTIALS_FILE':API_CREDENTIALS_FILE,
            'MUPIFDB_SCHEDULER_POLL_INTERVAL':'0.5',
            'MUPIFDB_SCHEDULER_STAT_FILE':'./sched-stat.json',
        }
        popen_kwargs = { 'cwd': mupifDB.__path__[0]+'/api' }
        args = [ sys.executable, '-m', 'mupifDB.scheduler' ]
        timeout = 10
        max_read_lines = 500
        # logged once the scheduler is ready to pick up executions
        pattern = 'Entering main loop to check for Pending executions'
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
        print(restApiControl.getSettings(maybe_init_db=True))
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
        weid = restApiControl.createExecution(wid,version=1,ip='localhost')['inserted_id']
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
    def test_03_pending_sorted_oldest_first(self,restApi):
        '''
        GET /executions returns records in the requested order.

        Regression for finding #7: the API always sorted newest first, so the scheduler started the
        most recently submitted executions first and could starve old ones; it now asks for
        sort=SubmittedDate (oldest first), while the default order stays newest first.
        '''
        wid=wfmini01.MiniWorkflow1().metadata.ID
        try: restApiControl.getWorkflowRecord(wid,-1)
        except Exception: restApiControl.postWorkflowFiles('useCase1',wfmini01.__file__,[])
        weids=[]
        for i in range(3):
            weid=restApiControl.createExecution(wid,version=1,ip='localhost')['inserted_id']
            restApiControl.scheduleExecution(weid)
            weids.append(weid)
        def order(**kw): return [e.dbID for e in restApiControl.getExecutionRecords(status='Pending',num_limit=1000,**kw) if e.dbID in weids]
        assert order(sort='SubmittedDate')==weids
        assert order(sort='-SubmittedDate')==weids[::-1]
        assert order()==weids[::-1]
    def test_02_web(self,scheduler,web):
        server='http://localhost:'+PORTS['web']
        import requests
        for what in ('about','workflows','workflowexecutions','workflows/1'):
            resp=requests.get(server+f'/{what}')
            open(f'/tmp/{what.replace("/","_")}.html','wb').write(resp.content)
            assert resp.status_code==200

        # time.sleep(10)
    # def test_schedule(self, ex2server):
