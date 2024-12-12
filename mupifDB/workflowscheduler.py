import sys
import os
mupifDBModDir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(mupifDBSrcDir := (mupifDBModDir+'/..'))

import time
import atexit
import tempfile
import multiprocessing
import subprocess
import enum
import pidfile
import zipfile
import ctypes
import json
import jsonpickle
import textwrap
from typing import Tuple

from mupifDB import restApiControl, restLogger, my_email

from pathlib import Path
import shutil
import datetime

import Pyro5.api
import Pyro5.errors
import mupif as mp

import logging

multiprocessing.current_process().name='mupifDB-scheduler'

# show remote traceback when remote calls fail
sys.excepthook = Pyro5.errors.excepthook

log=logging.getLogger('scheduler') 
log.setLevel(logging.INFO)
log.addHandler(restLogger.RestLogHandler())

# logging.getLogger().setLevel(logging.DEBUG)

# decrease verbosity here
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("urllib3").propagate = False

LOOP_SLEEP_SEC=20

# try to import schedulerconfig.py
authToken = None
try:
    import schedulerConfig # type: ignore
    authKey = schedulerConfig.authToken
except ImportError:
    log.info("schedulerConfig import failed")


schedulerStatFile = "/var/lib/mupif/persistent/scheduler-stat.json"

api_type = os.environ.get('MUPIFDB_REST_SERVER_TYPE', "mupif")

ns = mp.pyroutil.connectNameserver()
ns_uri = str(ns._pyroUri)

poolsize = 30
stopFlag = False # set to tru to end main scheduler loop


import pydantic
from typing import Literal,List,Optional
import threading
import multiprocessing


class SchedulerStat(mp.BareData):
    class Tasks(mp.BareData):
        running: int=0
        scheduled: int=0
        processed: int=0
        finished: int=0
        failed: int=0
    tasks: Tasks=Tasks()
    load: float=0.
    class JobInfo(mp.BareData):
        we_id: str
        wid: str
        status: Literal['Running','Finished','Failed']
        started: datetime.datetime
        finished: Optional[datetime.datetime]
    lastJobs: List[JobInfo]=[]
    class Hist(mp.BareData):
        interval: int=60*60
        count: int=48
        headPeriod: int=0
        processed: List[int]=[]
        pooled: List[int]=[]
        finished: List[int]=[]
        failed: List[int]=[]
        load: List[float]=[]
        def reset(self):
            self.moveBy(2*self.count)
            self.headPeriod=0
        def moveBy(self,n):
            def m(seq,n): seq[:]=seq[n:]+min(n,self.count)*[0] # modify in-place
            m(self.processed,n); m(self.pooled,n); m(self.finished,n); m(self.failed,n); m(self.load,n)
        def selfCheck(self):
            if set([len(s) for s in (self.processed,self.pooled,self.finished,self.failed)])!=set([self.count]): self.reset()
        def advance(self):
            self.selfCheck()
            per=int(time.time()//self.interval)
            if per==self.headPeriod: return # nothing to do
            elif per<self.headPeriod: raise RuntimeError('Traveling to the past?')
            else: self.moveBy(per-self.headPeriod)
            self.headPeriod=per
    hist48h: Hist=Hist(interval=3600,count=48)

    def advanceTime(self):
        self.hist48h.advance()
    def lastJobNew(self, job: JobInfo, max=5) -> None:
        self.lastJobs.append(job)
        self.lastJobs=self.lastJobs[-max:]
    def lastJobDone(self, we_id, status: Literal['Finished','Failed'], finished: datetime.datetime):
        match=[j for j in self.lastJobs if j.we_id==we_id]
        if not match: return # job already gone
        if len(match)>1:
            log.error('Multiple lastJobs with {we_id=}??')
            return
        match[0].finished=finished
        match[0].status=status

    def updateLoad(self) -> None:
        self.load=int(100*self.tasks.running*1./poolsize)
        self.hist48h.load[-1]=self.load
    def sync(self):
        self.updateLoad()
        restApiControl.setStatScheduler(
            runningTasks=self.tasks.running,
            scheduledTasks=self.tasks.scheduled,
            processedTasks=self.tasks.processed,
            load=self.load
        )
    @staticmethod
    def load_from_file(f):
        with open(f,'r') as f: return SchedulerStat.model_validate_json(f.read())
    def save_to_file(self,f):
        open(f,'w').write(self.model_dump_json())

import functools

def pyro_only(no_remote=False):
    def deco(fn):
        @functools.wraps(fn)
        def inner(*args, **kwargs):
            import Pyro5.callcontext
            if not Pyro5.callcontext.current_context.client: raise PermissionError('Must only be called via Pyro')
            if no_remote:
                import ipaddress
                import psutil
                import socket
                addr=ipaddress.ip_address(Pyro5.callcontext.current_context.client_sock_addr[0]) # type: ignore
                if addr.is_loopback: pass
                else:
                    ips=set([ipaddress.ip_address(rec.address) for rec in sum(psutil.net_if_addrs().values(),[]) if rec.family in (socket.AF_INET,socket,socket.AF_INET6)])
                    if addr not in ips: raise PermissionError(f'Must only be called locally (not from {addr})')
            return fn(*args,**kwargs)
        return inner
    return deco


@Pyro5.api.expose
class SchedulerMonitor(object):
    """
    Communication point about scheduler statistics. No locking necessary, since all access is done through Pyro which serializes the calls.
    """

    # class attribute, holding the URI of the instance once exposed over Pyro
    URI: str|None=None

    def __init__(self, ns): # , schedulerStat,lock):
        self.ns = ns
        self.stat = SchedulerStat()
        self.lock = threading.RLock()
        global schedulerStatFile
        if (Path(schedulerStatFile).is_file()):
            try:
                self.stat=SchedulerStat.load_from_file(schedulerStatFile)
            except:
                log.exception(f'Failure reading persistent schedule statistics from {schedulerStatFile}, starting from scratch.')

    def runServer(self):
        log.info("SchedulerMonitor: runningServer")
        return mp.pyroutil.runServer(ns=self.ns, appName="mupif.scheduler", app=self, metadata={"type:scheduler"})

    def advanceTime(self):
        with self.lock: self.stat.advanceTime()

    @pyro_only()
    def getStatistics(self,raw=False):
        # raw=True makes it suitable for reconstructing the model on the other side
        # the default raw=False will return data translated to the old format
        self.advanceTime()
        s=self.stat
        if raw: return s.model_dump(mode='json')
        return dict(
            runningTasks     = s.tasks.running,
            scheduledTasks   = s.tasks.scheduled,
            processedTasks   = s.tasks.processed,
            finishedTasks    = s.tasks.finished,
            failedTasks      = s.tasks.failed,
            lastJobs         = [list(j.model_dump(mode='json').values()) for j in s.lastJobs],
            currentLoad      = s.load,
            processedTasks48 = s.hist48h.processed,
            pooledTasks48    = s.hist48h.pooled,
            finishedTasks48  = s.hist48h.finished,
            failedTasks48    = s.hist48h.failed,
            load48           = s.hist48h.load,
        )

    @staticmethod
    def getExecutions(status='Running'):
        try:
            return restApiControl.getExecutionRecords(status=status)
        except Exception as e:
            log.error(repr(e))
            return []
    def stop(self):
        stopFlag=True
        self.ns.remove("mupif.scheduler")
    # no-op: runServer wants this for some reason?
    def registerPyro(self,*,daemon,ns,uri,appName,exclusiveDaemon): 
        pass

    @pyro_only(no_remote=True)
    def updateRunning(self,we_id,wid):
        self.advanceTime()
        self.stat.tasks.scheduled-=1
        self.stat.tasks.running+=1
        self.stat.lastJobNew(SchedulerStat.JobInfo(we_id=we_id,wid=wid,status='Running',started=datetime.datetime.now(),finished=None))
        self.stat.sync()

    @pyro_only(no_remote=True)
    def updateScheduled(self,numPending: int):
        self.advanceTime()
        self.stat.tasks.scheduled=numPending
        self.stat.hist48h.pooled[-1]+=numPending
        self.stat.sync()

    @pyro_only(no_remote=True)
    def updateFinished(self,retCode,we_id):
        self.advanceTime()
        self.stat.tasks.running-=1
        self.stat.tasks.processed+=1
        self.stat.hist48h.processed[-1]+=1
        if retCode==0:
            self.stat.tasks.finished+=1
            self.stat.hist48h.finished[-1]+=1
        else:
            self.stat.tasks.failed+=1
            self.stat.hist48h.failed[-1]+=1
        self.stat.lastJobDone(we_id=we_id,status=('Finished' if retCode==0 else 'Failed'),finished=datetime.datetime.now())
        self.stat.sync()

    @pyro_only(no_remote=True)
    def persistStat(self):
        self.advanceTime()
        self.stat.save_to_file(schedulerStatFile)




def copyLogToDB (we_id, workflowLogName):
    try:
        with open(workflowLogName,'r') as f:
            for l in f: log.info('WORKFLOW LOG: '+l[:-1])
        log.info("Copying log files to database")
        with open(workflowLogName, 'rb') as f:
            logID = restApiControl.uploadBinaryFile(f)
            if logID is not None:
                restApiControl.setExecutionParameter(we_id, 'ExecutionLog', logID)
            log.info("Copying log files done")
    except Exception as e:
        log.error(repr(e))


def executeWorkflow(we_id: str) -> None:
    try:
        log.info("executeWorkflow invoked")
        return executeWorkflow_inner1(we_id)
    except Exception as e:
        log.exception("Execution of workflow %s failed." % we_id)

def executeWorkflow_inner1(we_id: str) -> None:
    we_rec = restApiControl.getExecutionRecord(we_id)
    if we_rec is None:
        log.error("Workflow Execution record %s not found" % we_id)
        raise KeyError("Workflow Execution record %s not found" % we_id)
    else:
        log.info("Workflow Execution record %s found" % we_id)

    workflowVersion = we_rec.WorkflowVersion
    wid = we_rec.WorkflowID
    workflow_record = restApiControl.getWorkflowRecordGeneral(wid=wid, version=workflowVersion)
    if workflow_record is None:
        log.error("Workflow document with wid %s, verison %s not found" % (wid, workflowVersion))
        raise KeyError("Workflow document with ID %s, version %s not found" % (wid, workflowVersion))
    else:
        log.info("Workflow document with wid %s, id %s, version %s found" % (wid, we_rec.dbID, workflowVersion))

    # check if status is "Scheduled"
    if we_rec.Status == 'Scheduled' or api_type == 'granta':  # todo remove granta
        return executeWorkflow_inner2(we_id,we_rec,workflow_record)
    else:
        log.error("WEID %s not scheduled for execution" % we_id)
        raise KeyError("WEID %s not scheduled for execution" % we_id)

def executeWorkflow_inner2(we_id: str, we_rec, workflow_record) -> None:
    '''Process workflow which is already scheduled'''
    _mon=Pyro5.api.Proxy(SchedulerMonitor.URI)
    wid = we_rec.WorkflowID
    completed = 1  # todo check
    log.info("we_rec status is Scheduled, processing")
    # execute the selected workflow
    # take workflow source and run python interpreter on it in a temporary directory
    tempRoot = '/tmp'
    log.info("Creating temp dir")
    with tempfile.TemporaryDirectory(dir=tempRoot, prefix='mupifDB') as tempDir:
        # if (1):  # uncomment this to keep temdDir
        #     tempDir = tempfile.mkdtemp(dir=tempRoot, prefix='mupifDB_')
        log.info("temp dir %s created" % (tempDir,))
        workflowLogName = tempDir+'/workflow.log'
        execScript = Path(tempDir+'/workflow_execution_script.py')
        # copy workflow source to tempDir
        try:
            executeWorkflow_copyInputs(we_id,workflow_record,tempDir,execScript)
        except Exception as e:
            log.exception('Error in executeWorkflow_inner2')
            # set execution code to failed ...yes or no?
            restApiControl.setExecutionStatus(we_id,'Failed')
            my_email.sendEmailAboutExecutionStatus(we_id)
            return None
        # execute
        log.info("Executing we_id %s, tempdir %s" % (we_id, tempDir))
        # update status
        _mon.updateRunning(we_id,wid)
        # updateStatRunning(lock, schedulerStat, we_id, wid)
        #runningJobs[we_id]=wid # for runtime monitoring
        restApiControl.setExecutionStatus(we_id,'Running')
        restApiControl.setExecutionAttemptsCount(we_id, we_rec.Attempts+1)
        # uses the same python interpreter as the current process
        cmd = [sys.executable, execScript, '-eid', str(we_id)]
        # log.info(cmd)
        with open(workflowLogName, 'w') as workflowLog:
            ll = 10*'='
            workflowLog.write(textwrap.dedent(f'''
                {ll} WORKFLOW STARTING at {(t0:=datetime.datetime.now()).isoformat(timespec='seconds')} {ll}
                {ll} command is {cmd} {ll}'''))
            env = os.environ.copy()
            if 'PYTHONPATH' in env:
                env['PYTHONPATH'] += f'{os.pathsep}{mupifDBSrcDir}'
            else:
                env['PYTHONPATH'] = mupifDBSrcDir
            env['MUPIF_NS'] = ns_uri
            env['MUPIFDB_REST_SERVER_TYPE'] = api_type

            completed = subprocess.call(cmd, cwd=tempDir, stderr=subprocess.STDOUT, stdout=workflowLog, env=env)
            workflowLog.write(textwrap.dedent(f'''
                {ll} WORKFLOW FINISHED at {(t1:=datetime.datetime.now()).isoformat(timespec='seconds')} {ll}
                {ll} duration: {str((dt:=(t1-t0))-datetime.timedelta(microseconds=dt.microseconds))} {ll}
                {ll} exit status of {cmd}: {completed} ({'ERROR' if completed!=0 else 'SUCCESS'}) {ll}'''))

        # log.info(tempDir)
        log.info('command:' + str(cmd) + ' Return Code:'+str(completed))

        # store execution log
        logID = None

        p = Path(tempDir)
        for it in p.iterdir():
            log.info(it)

        try:
            copyLogToDB(we_id, workflowLogName)
        except:
            log.info("Copying log files was not successful")

        # update status
        _mon.updateFinished(completed,we_id)
        # updateStatFinished(lock, schedulerStat, completed, we_id)
        # del runningJobs[we_id] # remove we_id from running jobs; for monitoring
    log.info("Updating we_id %s status to %s" % (we_id, completed))
    # set execution code to completed
    if completed == 0:
        log.warning("Workflow execution %s Finished" % we_id)
        restApiControl.setExecutionStatus(we_id,'Finished')
        my_email.sendEmailAboutExecutionStatus(we_id)
        # return we_id, ExecutionResult.Finished # XXX ??
    elif completed == 1:
        log.warning("Workflow execution %s Failed" % we_id)
        restApiControl.setExecutionStatus(we_id,'Failed')
        my_email.sendEmailAboutExecutionStatus(we_id)
        # return we_id, ExecutionResult.Failed # XXX ??
    elif completed == 2:
        log.warning("Workflow execution %s could not be initialized due to lack of resources" % we_id)
        restApiControl.setExecutionStatus(we_id, 'Pending', revertPending=True)
    else:
        pass


def executeWorkflow_copyInputs(we_id,workflow_record,tempDir,execScript) -> None:
    python_script_filename = workflow_record.modulename + ".py"

    fc, fn = restApiControl.getBinaryFileByID(workflow_record.GridFSID)
    with open(tempDir + '/' + fn, "wb") as f:
        f.write(fc)
        f.close()

    if fn.split('.')[-1] == 'py':
        log.info("downloaded .py file..")
        if fn == python_script_filename:
            log.info("Filename check OK")
        else:
            log.info("Filename check FAILED")

    elif fn.split('.')[-1] == 'zip':
        log.info("downloaded .zip file, extracting..")
        log.info(fn)
        zf = zipfile.ZipFile(tempDir + '/' + fn, mode='r')
        filenames = zipfile.ZipFile.namelist(zf)
        log.info("Zipped files:")
        log.info(filenames)
        zf.extractall(path=tempDir)
        if python_script_filename in filenames:
            log.info("Filename check OK")
        else:
            log.error("Filename check FAILED")

    else:
        log.error("Unsupported file extension")

    log.info("Copying executor script.")

    shutil.copy(mupifDBModDir+'/workflow_execution_script.py', execScript)


def stopPool(var_pool):
    try:
        log.info("Stopping the scheduler, waiting for workers to terminate")
        # @TODO: do not reset processedTasks, continue counting
        restApiControl.setStatScheduler(runningTasks=0, scheduledTasks=0, load=0, processedTasks=0)
        var_pool.close()  # close pool
        var_pool.join()  # wait for completion
        log.info("All tasks finished, exiting")
    except Exception as e:
        log.error(repr(e))


def checkWorkflowResources(wid, version):
    try:
        workflow = restApiControl.getWorkflowRecordGeneral(wid, int(version))
        models_md = workflow.Models
        try:
            res = mp.Workflow.checkModelRemoteResourcesByMetadata(models_md=models_md)
            return res
        except:
            return False
    except Exception as e:
        log.error(repr(e))
        return False


def checkExecutionResources(eid):
    try:
        log.info("Checking execution resources")
        if api_type == 'granta':
            return True  # todo granta temporary
        execution = restApiControl.getExecutionRecord(eid)
        return checkWorkflowResources(execution.WorkflowID, execution.WorkflowVersion)
    except Exception as e:
        log.exception('Error in checkExecutionResources')
        return False



def scheduler_startup_execute_scheduled(pool):
    # import first already scheduled executions
    try:
        scheduled_executions = restApiControl.getScheduledExecutions()
    except Exception as e:
        log.exception('Error getting scheduled execution:')
        scheduled_executions = []

    for wed in scheduled_executions:
        log.info(f'{wed.dbID} found as Scheduled')
        # add the correspoding weid to the pool, change status to scheduled
        weid = wed.dbID
        assert weid is not None # make pyright happy
        # result1 = pool.apply_async(test)
        # log.info(result1.get())
        if checkExecutionResources(weid):
            result = pool.apply_async(executeWorkflow, args=(weid,), callback=procFinish, error_callback=procError)
            log.info(result)
            log.info(f"WEID {weid} added to the execution pool")
        else:
            log.info(f"WEID {weid} cannot be scheduled due to unavailable resources")
            try:
                we_rec = restApiControl.getExecutionRecord(weid)
                restApiControl.setExecutionAttemptsCount(weid, we_rec.Attempts + 1)
            except Exception as e:
                log.exception('Error running scheduled execution {weid=}:')


def scheduler_schedule_pending(pool):
    # retrieve weids with status "Scheduled" from DB
    try:
        pending_executions = restApiControl.getPendingExecutions(num_limit=poolsize*10)
    except Exception as e:
        log.exception('Error checking pending executions')
        pending_executions = []

    monitor=Pyro5.api.Proxy(SchedulerMonitor.URI)
    monitor.updateScheduled(len(pending_executions))

    for wed in pending_executions:
        weid = wed.dbID
        assert weid is not None # make pyright happy
        log.info(f'{weid} found as pending ({wed.Attempts=})')
        # check number of attempts for execution
        if int(wed.Attempts) > 10:
            try:
                restApiControl.setExecutionStatus(weid,'Created')
                if api_type != 'granta':
                    my_email.sendEmailAboutExecutionStatus(weid)
            except Exception as e:
                log.exception('')
        else:
            time.sleep(2)
            if checkExecutionResources(weid):
                # add the correspoding weid to the pool, change status to scheduled
                res = False
                try:
                    res = restApiControl.setExecutionStatus(weid,'Scheduled')
                except Exception as e:
                    log.exception('')

                if not res:
                    log.info("Could not update execution status")
                else:
                    log.info("Updated status of execution")

                result = pool.apply_async(executeWorkflow, args=(weid,), callback=procFinish, error_callback=procError)
                # log.info(result.get())
                log.info(f"WEID {weid} added to the execution pool")
            else:
                log.info(f"WEID {weid} cannot be scheduled due to unavailable resources")
                if api_type != 'granta':
                    try:
                        we_rec = restApiControl.getExecutionRecord(weid)
                        restApiControl.setExecutionAttemptsCount(weid, we_rec.Attempts + 1)
                    except Exception as e:
                        log.exception('Failure getting execution record (for execution with resources unavailable)')


    # display progress (consider use of tqdm)
    lt = time.localtime(time.time())
    if api_type != 'granta':
        try:
            stat=SchedulerStat.model_validate(monitor.getStatistics(raw=True))
            log.info(f'Scheduled/Running/Load: {stat.tasks.scheduled}/{stat.tasks.running}/{stat.load}')
        except Exception as e:
            log.exception('')


# callbacks for the task pool
def procInit():     pass
def procFinish(r):  pass
def procError(e):   log.error('Error running pool task:',exc_info=e)


def main():
    import requests.adapters
    import urllib3
    # FIXME: session not used at all
    adapter = requests.adapters.HTTPAdapter(max_retries=urllib3.Retry(total=8, backoff_factor=.05))
    session = requests.Session()
    for proto in ('http://', 'https://'):
        session.mount(proto, adapter)
    #
    # @bp: do we want to reset processedTasks to zero? Perhaps keeping it ?
    #
    try:
        restApiControl.setStatScheduler(runningTasks=0, scheduledTasks=0, load=0, processedTasks=0, session=session)
    except Exception as e:
        log.error(repr(e))

    if 1:
        _monitor = SchedulerMonitor(ns)
        SchedulerMonitor.URI=_monitor.runServer()
        monitor=Pyro5.api.Proxy(SchedulerMonitor.URI)



        # https://github.com/mupif/mupifDB/issues/14
        # explicitly use forking (instead of spawning) so that there are no leftover monitoring processes
        # if scheduler gets killed externally (such as in the docker by supervisor, which won't reap
        # orphan processes
        # even though fork is the default for POSIX, mp.simplejobmanager was setting the (global)
        # default to spawn, therefore it was used here as well. So better to be explicit (and also
        # fix the — perhaps unnecessary now? — global setting in simplejobmanager)
        pool = multiprocessing.get_context('fork').Pool(processes=poolsize, initializer=procInit)
        atexit.register(stopPool, pool)
        try:
            with pidfile.PIDFile(filename='mupifDB_scheduler_pidfile'):
                log.info("Starting MupifDB Workflow Scheduler")

                sys.excepthook = Pyro5.errors.excepthook

                try:
                    log.info("Importing already scheduled executions…")
                    scheduler_startup_execute_scheduled(pool)
                    log.info("Done")

                    log.info("Entering main loop to check for Pending executions")
                    # add new execution (Pending)
                    while stopFlag is not True:
                        scheduler_schedule_pending(pool)
                        # lazy update of persistent statistics, done in main thread thus thread safe
                        monitor.persistStat()
                        # log.info("waiting..")
                        time.sleep(LOOP_SLEEP_SEC)

                except Exception as err:
                    log.exception("Error in workflow execution")
                    stopPool(pool)
                except:
                    log.exception("Unknown error encountered?!")
                    stopPool(pool)
        except pidfile.AlreadyRunningError:
            log.error('Already running.')
    log.info("Exiting MupifDB Workflow Scheduler\n")

if __name__ == '__main__':
    main()

