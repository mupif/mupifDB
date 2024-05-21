import sys
import os
sys.path.append(mupifDBModDir := (os.path.dirname(os.path.abspath(__file__))))
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

import restApiControl
import restLogger
import my_email

from pathlib import Path
import shutil
import datetime

import Pyro5
import mupif as mp

import logging

log=logging.getLogger('workflow-scheduler') 
log.addHandler(restLogger.RestLogHandler())

# decrease verbosity here
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("urllib3").propagate = False

# try to import schedulerconfig.py
authToken = None
try:
    import schedulerConfig
    authKey = schedulerConfig.authToken
except ImportError:
    log.info("schedulerConfig import failed")
# WorkflowScheduler is a daemon, which
# will try to execute pending workflow executions in DB (those with status "Scheduled")
# internally uses a multiprocessing pool to handle workflow execution requests
#


# WEID Status
# Created -> Pending -> Scheduled -> Running -> Finished | Failed
#
# Created-> the execution record allocated and initialized
# Pending -> execution record finalized (inputs sets), ready to be scheduled
# Scheduled -> execution scheduled by the scheduler
# Running -> execution processed (it is running)
# Finished|Failed -> execution finished
#

class ExecutionResult(enum.Enum):
    Finished = 1  # successful
    Failed = 2


class index(enum.IntEnum):
    status = 0
    scheduledTasks = 1
    runningTasks = 2
    processedTasks = 3
    load = 4


def historyMoveHelper(data, epoch, prefix): 
    # fill the gap between current index and last valid entry (self.index)
    indx = int(epoch/data[prefix+'epochDelta'])
    if (indx == data[prefix+'index']):
        return
    elif (indx < data[prefix+'index']):
        raise IndexError ();
    else:
        gap = indx-data[prefix+'index']
        if (gap>data[prefix+'size']):
            gap=data[prefix+'size']

        arrays = [prefix+'pooledTasks', prefix+'processedTasks', prefix+'finishedTasks', prefix+'failedTasks', prefix+'load', prefix+'loadTicks']
        for a in arrays:
            for i in range(gap):
               data[a].pop(0)
               data[a].append(0)
        data[prefix+'index'] = indx

def historyMove(data, epoch):
    historyMoveHelper(data, epoch, 's1_')
    
def historyUpdateLoad(data, epoch, currentLoad):
    historyMove(data, epoch)
    data['s1_load'][-1] = (data['s1_load'][-1]*data['s1_loadTicks'][-1] + currentLoad)/(data['s1_loadTicks'][-1]+1)
    data['s1_loadTicks'][-1]+=1
def historyUpdatePooled(data, epoch, numberOfPendingExecution):
    historyMove(data,epoch)
    data['s1_pooledTasks'][-1] = numberOfPendingExecution
def historyUpdateRunning(data, epoch):
    historyMove(data,epoch)
    data['s1_pooledTasks'][-1] -= 1
    #data['s1_runningTasks'][-1] += 1
def historyUpdateFinished(data, epoch):
    historyMove(data,epoch)
    data['s1_processedTasks'][-1] += 1
    data['s1_finishedTasks'][-1] += 1
    #data['s1_runningTasks'][-1] -= 1
def historyUpdateFailed(data, epoch):
    historyMove(data,epoch)
    data['s1_processedTasks'][-1] +=1
    data['s1_failedTasks'][-1] += 1
    #data['s1_runningTasks'][-1] -= 1

# global vars 
runningTasks = 0
scheduledTasks = 0
processedTasks = 0
finishedTasks = 0 # with success
failedTasks = 0
lastJobs = {}  # dict, we-id key


schedulerStatFile = "/var/lib/mupif/persistent/scheduler-stat.json"

api_type = os.environ.get('MUPIFDB_REST_SERVER_TYPE', "mupif")

ns = mp.pyroutil.connectNameserver()
ns_uri = str(ns._pyroUri)

poolsize = 30
stopFlag = False # set to tru to end main scheduler loop

fd = None
buf = None

@Pyro5.api.expose
class SchedulerMonitor (object):
    def __init__(self, ns, schedulerStat,lock):
        self.ns = ns
        self.stat = schedulerStat
        self.lock = lock
    def runServer(self):
        log.info("SchedulerMonitor: runingServer")
        return mp.pyroutil.runServer(ns=self.ns, appName="mupif.scheduler", app=self, metadata={"type:scheduler"})
    def getStatistics(self):
        with self.lock:
            runningTasks=self.stat['runningTasks']
            scheduledTasks=self.stat['scheduledTasks']
            processedTasks = self.stat['processedTasks']
            finishedTasks=self.stat['finishedTasks']
            failedTasks=self.stat['failedTasks']
            lastJobs=self.stat['lastJobs']
            load = self.stat['load']
            pooledTasks48 = self.stat['s1_pooledTasks'][:]
            processedTasks48 = self.stat['s1_processedTasks'][:]
            finishedTasks48 = self.stat['s1_finishedTasks'][:]
            failedTasks48 = self.stat['s1_failedTasks'][:]
            load48 = self.stat['s1_load'][:]
        return {
            'runningTasks':runningTasks, 
            'scheduledTasks':scheduledTasks,
            'processedTasks': processedTasks,
            'finishedTasks': finishedTasks,
            'failedTasks': failedTasks,
            'lastJobs': lastJobs,
            'currentLoad': load,
            'processedTasks48': processedTasks48,
            'pooledTasks48': pooledTasks48,
            'finishedTasks48': finishedTasks48,
            'failedTasks48': failedTasks48,
            'load48': load48
        }

    @staticmethod
    def getExecutions(status='Running'):
        try:
            return restApiControl.getExecutionRecords(status=status)
        except Exception as e:
            log.error(repr(e))
            return []
    def stop (self):
        stopFlag=True
        self.ns.remove("mupif.scheduler")
    # no-op: runServer wants  this for some reason?
    def registerPyro(self,*,daemon,ns,uri,appName,exclusiveDaemon): 
        pass


def procInit():
    global fd
    global buf
    # create new empty file to back memory map on disk
    # fd = os.open('/tmp/workflowscheduler', os.O_RDWR)

    # Create the mmap instace with the following params:
    # fd: File descriptor which backs the mapping or -1 for anonymous mapping
    # length: Must in multiples of PAGESIZE (usually 4 KB)
    # flags: MAP_SHARED means other processes can share this mmap
    # prot: PROT_WRITE means this process can write to this mmap
    # buf = mmap.mmap(fd, mmap.PAGESIZE, mmap.MAP_SHARED, mmap.PROT_WRITE)
    log.info("procInit called")


def procFinish(r):
    log.info("procFinish called")


def procError(r):
    log.info("procError called:"+str(r))


def updateStatRunning(lock, schedulerStat, we_id, wid):
    with lock:
        log.info("updateStatRunning called")
        # print (schedulerStat)
        # print ('------------------')

        schedulerStat['scheduledTasks'] = schedulerStat['scheduledTasks']-1
        schedulerStat['runningTasks']=schedulerStat['runningTasks']+1
        # schedulerStat['runningJobs'].append(str(we_id)+':'+str(wid)) # won't work
        # Modifications to mutable values or items in dict and list proxies will not be propagated through the manager,
        # because the proxy has no way of knowing when its values or items are modified.
        # To modify such an item, you can re-assign the modified object to the container proxy.
        jobs = [(we_id, wid, 'Running', datetime.datetime.now().isoformat(timespec='seconds'), '-')]
        for i in range(min(4,len(schedulerStat['lastJobs']))):
            jobs.append(schedulerStat['lastJobs'][i])
        schedulerStat['lastJobs'] = jobs

        # print (we_id, wid)
        # print (schedulerStat)
        # print ('=======================')
        epoch=time.time()
        l = int(100 * int(schedulerStat['runningTasks']) / poolsize)
        historyUpdateRunning(schedulerStat,epoch)  
        restApiControl.setStatScheduler(load=l)
        schedulerStat['load'] = l
        restApiControl.setStatScheduler(runningTasks = schedulerStat['runningTasks'])
        restApiControl.setStatScheduler(scheduledTasks = schedulerStat['scheduledTasks'])



def updateStatScheduled(lock, schedulerStat, numberOfPendingExecutions):
    with lock:
        log.info("updateStatScheduled called")
        #
        schedulerStat['scheduledTasks'] = numberOfPendingExecutions
        restApiControl.setStatScheduler(scheduledTasks = numberOfPendingExecutions)
        epoch=time.time()
        historyUpdatePooled(schedulerStat, epoch, numberOfPendingExecutions)


def updateStatFinished(lock, schedulerStat, retCode, we_id):
    try:
        with lock:
            log.info("updateStatFinished called")

            #stats_temp = restApiControl.getStatScheduler()
            #restApiControl.setStatScheduler(load=int(100*int(stats_temp['runningTasks'])/poolsize))
            #
            schedulerStat['runningTasks']=schedulerStat['runningTasks']-1
            if (retCode == 0):
                schedulerStat['processedTasks']=schedulerStat['processedTasks']+1
                schedulerStat['finishedTasks']=schedulerStat['finishedTasks']+1
            elif (retCode == 1):
                schedulerStat['processedTasks']=schedulerStat['processedTasks']+1
                schedulerStat['failedTasks'] =schedulerStat['failedTasks']+1
            l = int(100 * int(schedulerStat['runningTasks']) / poolsize)            
            restApiControl.setStatScheduler(load=l)
            schedulerStat['load'] = l
            restApiControl.setStatScheduler(runningTasks=schedulerStat['runningTasks'])
            restApiControl.setStatScheduler(processedTasks=schedulerStat['processedTasks'])

            jobs = []
            for i in range(len(schedulerStat['lastJobs'])):
                if (schedulerStat['lastJobs'][i][0] == we_id):
                    if (retCode == 0):
                        jobs.append((we_id, schedulerStat['lastJobs'][i][1], "Finished", schedulerStat['lastJobs'][i][3], datetime.datetime.now().isoformat(timespec='seconds')))
                    else:
                        jobs.append((we_id, schedulerStat['lastJobs'][i][1], "Failed", schedulerStat['lastJobs'][i][3], datetime.datetime.now().isoformat(timespec='seconds')))
                else:
                    jobs.append(schedulerStat['lastJobs'][i])
            schedulerStat['lastJobs'] = jobs

            epoch=time.time()
            historyUpdateFinished(schedulerStat, epoch)
            # print (schedulerStat)
    except Exception as e:
        log.error(repr(e))


def updateStatPersistent (schedulerStat):
    # log.info("updateStatPersistent called")
    if (False):
        return
    else:
        localStat = schedulerStat.copy()
        arrayToCopy = ['s1_pooledTasks', 's1_processedTasks', 's1_finishedTasks', 's1_failedTasks', 's1_load', 's1_loadTicks']
        for i in arrayToCopy:
           localStat[i] = schedulerStat[i][:]
        
        jsonFile= open(schedulerStatFile, 'w')
        json.dump(localStat, jsonFile)
        jsonFile.close()
        # log.info("Update:", stat)
        # log.info("updateStatPersistent finished")


def copyLogToDB (we_id, workflowLogName):
    try:
        log.info("Copying log files to database")
        with open(workflowLogName, 'rb') as f:
            logID = restApiControl.uploadBinaryFile(f)
            if logID is not None:
                restApiControl.setExecutionParameter(we_id, 'ExecutionLog', logID)
            log.info("Copying log files done")
    except Exception as e:
        log.error(repr(e))


def executeWorkflow(lock, schedulerStat, we_id: str) -> Tuple[str,ExecutionResult]:
    try:
        log.info("executeWorkflow invoked")
        return executeWorkflow_inner1(lock, schedulerStat, we_id)
    except Exception as e:
        log.error("Execution of workflow %s failed." % we_id)
        log.error(repr(e))

def executeWorkflow_inner1(lock, schedulerStat, we_id: str) -> Tuple[str,ExecutionResult]:
        we_rec = restApiControl.getExecutionRecord(we_id)
        if we_rec is None:
            log.error("Workflow Execution record %s not found" % we_id)
            raise KeyError("Workflow Execution record %s not found" % we_id)
        else:
            log.info("Workflow Execution record %s found" % we_id)

        workflowVersion = int(we_rec['WorkflowVersion'])
        wid = we_rec['WorkflowID']
        workflow_record = restApiControl.getWorkflowRecordGeneral(wid=wid, version=workflowVersion)
        if workflow_record is None:
            log.error("Workflow document with wid %s, verison %s not found" % (wid, workflowVersion))
            raise KeyError("Workflow document with ID %s, version %s not found" % (wid, workflowVersion))
        else:
            log.info("Workflow document with wid %s, id %s, version %s found" % (wid, we_rec['_id'], workflowVersion))

        # check if status is "Scheduled"
        if we_rec['Status'] == 'Scheduled' or api_type == 'granta':  # todo remove granta
            return executeWorkflow_inner2(lock,schedulerStat,we_id,we_rec,workflow_record)
        else:
            log.error("WEID %s not scheduled for execution" % we_id)
            raise KeyError("WEID %s not scheduled for execution" % we_id)

def executeWorkflow_inner2(lock, schedulerState, we_id: str, we_rec, workflow_record) -> Tuple[str,ExecutionResult]:
            '''Process workflow which is already scheduled'''
            wid = we_rec['WorkflowId']
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
                if not executeWorkflow_copyInputs(we_id,workflow_record,tempDir,execScript):
                    return we_id, ExecutionResult.Failed
                # execute
                log.info("Executing we_id %s, tempdir %s" % (we_id, tempDir))
                # update status
                updateStatRunning(lock, schedulerStat, we_id, wid)
                #runningJobs[we_id]=wid # for runtime monitoring
                restApiControl.setExecutionStatusRunning(we_id)
                restApiControl.setExecutionAttemptsCount(we_id, int(we_rec['Attempts'])+1)
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
                updateStatFinished(lock, schedulerStat, completed, we_id)
                # del runningJobs[we_id] # remove we_id from running jobs; for monitoring
            log.info("Updating we_id %s status to %s" % (we_id, completed))
            # set execution code to completed
            if completed == 0:
                log.warning("Workflow execution %s Finished" % we_id)
                restApiControl.setExecutionStatusFinished(we_id)
                my_email.sendEmailAboutExecutionStatus(we_id)
                return we_id, ExecutionResult.Finished
            elif completed == 1:
                log.warning("Workflow execution %s Failed" % we_id)
                restApiControl.setExecutionStatusFailed(we_id)
                my_email.sendEmailAboutExecutionStatus(we_id)
                return we_id, ExecutionResult.Failed
            elif completed == 2:
                log.warning("Workflow execution %s could not be initialized due to lack of resources" % we_id)
                restApiControl.setExecutionStatusPending(we_id, True)
            else:
                pass


def executeWorkflow_copyInputs(we_id,workflow_record,tempDir,execScript) -> bool:
                try:
                    python_script_filename = workflow_record['modulename'] + ".py"

                    fc, fn = restApiControl.getBinaryFileByID(workflow_record['GridFSID'])
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
                except Exception as e:
                    log.error(repr(e))
                    # set execution code to failed ...yes or no?
                    restApiControl.setExecutionStatusFailed(we_id)
                    my_email.sendEmailAboutExecutionStatus(we_id)
                    try:
                        copyLogToDB(we_id, workflowLogName)
                    except:
                        log.info("Copying log files was not successful")
                    return False
                return True




def stop(var_pool):
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
        workflow = restApiControl.getWorkflowRecordGeneral(wid, version)
        models_md = workflow.get('Models', [])
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
        return checkWorkflowResources(execution['WorkflowID'], execution['WorkflowVersion'])
    except Exception as e:
        log.error(repr(e))
        return False


if __name__ == '__main__':

    if (Path(schedulerStatFile).is_file()):
        with open(schedulerStatFile,'r') as f:
            stat = json.load(f)
            # print (stat)
    else:
        # create empty stat
        stat={'runningTasks':0, 'scheduledTasks': 0, 'load':0, 'processedTasks':0, 'finishedTasks':0, 'failedTasks':0}
        #with open(schedulerStatFile,'w') as f:
        #    f.write(jsonpickle.encode(stat))

    import requests.adapters
    import urllib3
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

    

    with multiprocessing.Manager() as manager:
        schedulerStat=manager.dict()
        schedulerStat['runningTasks'] = 0
        schedulerStat['scheduledTasks'] = 0
        schedulerStat['load'] = 0
        schedulerStat['processedTasks'] = stat.get('processedTasks', 0)
        schedulerStat['finishedTasks'] = stat.get('finishedTasks', 0)
        schedulerStat['failedTasks'] = stat.get('failedTasks', 0)
        schedulerStat['lastJobs'] = []  # manager.list()
        # 48hrs statistics
        schedulerStat['s1_size']=48
        schedulerStat['s1_index']=stat.get('s1_index', 0)
        schedulerStat['s1_epochDelta']=60*60
        schedulerStat['s1_pooledTasks']=manager.list(stat.get('s1_pooledTasks', [0]*schedulerStat['s1_size']))
        schedulerStat['s1_processedTasks']=manager.list(stat.get('s1_processedTasks', [0]*schedulerStat['s1_size']))
        schedulerStat['s1_finishedTasks']=manager.list(stat.get('s1_finishedTasks', [0]*schedulerStat['s1_size']))
        schedulerStat['s1_failedTasks']=manager.list(stat.get('s1_failedTasks', [0]*schedulerStat['s1_size']))
        schedulerStat['s1_load']=manager.list(stat.get('s1_load', [0]*schedulerStat['s1_size']))
        schedulerStat['s1_loadTicks']=manager.list(stat.get('s1_loadTicks', [0]*schedulerStat['s1_size']))
        
        updateStatPersistent (schedulerStat)

        statusLock = manager.Lock()

        monitor = SchedulerMonitor(ns, schedulerStat, statusLock)

        # run scheduler monitor
        monitor.runServer()

        # https://github.com/mupif/mupifDB/issues/14
        # explicitly use forking (instead of spawning) so that there are no leftover monitoring processes
        # if scheduler gets killed externally (such as in the docker by supervisor, which won't reap
        # orphan processes
        # even though fork is the default for POSIX, mp.simplejobmanager was setting the (global)
        # default to spawn, therefore it was used here as well. So better to be explicit (and also
        # fix the — perhaps unnecessary now? — global setting in simplejobmanager)
        pool = multiprocessing.get_context('fork').Pool(processes=poolsize, initializer=procInit)
        atexit.register(stop, pool)
        try:
            with pidfile.PIDFile(filename='mupifDB_scheduler_pidfile'):
                log.info("Starting MupifDB Workflow Scheduler\n")

                try:
                    # import first already scheduled executions
                    log.info("Importing already scheduled executions")
                    try:
                        scheduled_executions = restApiControl.getScheduledExecutions()
                    except Exception as e:
                        log.error(repr(e))
                        scheduled_executions = []

                    for wed in scheduled_executions:
                        log.info(str(wed['_id']) + " found as Scheduled")
                        # add the correspoding weid to the pool, change status to scheduled
                        weid = wed['_id']
                        # result1 = pool.apply_async(test)
                        # log.info(result1.get())
                        if checkExecutionResources(weid):
                            result = pool.apply_async(executeWorkflow, args=(statusLock, schedulerStat,weid), callback=procFinish, error_callback=procError)
                            log.info(result)
                            log.info("WEID %s added to the execution pool" % weid)
                        else:
                            log.info("WEID %s cannot be scheduled due to unavailable resources" % weid)
                            try:
                                we_rec = restApiControl.getExecutionRecord(weid)
                                restApiControl.setExecutionAttemptsCount(weid, int(we_rec['Attempts']) + 1)
                            except Exception as e:
                                log.error(repr(e))

                    log.info("Done\n")

                    log.info("Entering main loop to check for Pending executions")
                    # add new execution (Pending)
                    while stopFlag is not True:
                        # retrieve weids with status "Scheduled" from DB
                        try:
                            pending_executions = restApiControl.getPendingExecutions(num_limit=poolsize*10)
                            # if schedulerStat['scheduledTasks'] < 200:
                            #     pending_executions = restApiControl.getPendingExecutions(num_limit=poolsize*4)
                            # else:
                            #     pending_executions = []
                        except Exception as e:
                            log.error(repr(e))
                            pending_executions = []

                        updateStatScheduled(statusLock, schedulerStat, len(pending_executions))  # update status
                        for wed in pending_executions:
                            log.info(str(wed['_id']) + " found as pending")
                            weid = wed['_id']

                            # check number of attempts for execution
                            if int(wed['Attempts']) > 10:
                                try:
                                    restApiControl.setExecutionStatusCreated(weid)
                                    if api_type != 'granta':
                                        my_email.sendEmailAboutExecutionStatus(weid)
                                except Exception as e:
                                    log.error(repr(e))

                            else:
                                time.sleep(2)
                                if checkExecutionResources(weid):
                                    # add the correspoding weid to the pool, change status to scheduled
                                    res = False
                                    try:
                                        res = restApiControl.setExecutionStatusScheduled(weid)
                                    except Exception as e:
                                        log.error(repr(e))

                                    if not res:
                                        log.info("Could not update execution status")
                                    else:
                                        log.info("Updated status of execution")
                                    
                                    result = pool.apply_async(executeWorkflow, args=(statusLock, schedulerStat, weid), callback=procFinish, error_callback=procError)
                                    # log.info(result.get())
                                    log.info("WEID %s added to the execution pool" % weid)
                                else:
                                    log.info("WEID %s cannot be scheduled due to unavailable resources" % weid)
                                    if api_type != 'granta':
                                        try:
                                            we_rec = restApiControl.getExecutionRecord(weid)
                                            restApiControl.setExecutionAttemptsCount(weid, int(we_rec['Attempts']) + 1)
                                        except Exception as e:
                                            log.error(repr(e))

                        # ok, no more jobs to schedule for now, wait
                        l = int(100*int(schedulerStat['runningTasks'])/poolsize)
                        with statusLock:
                             restApiControl.setStatScheduler(load=l)
                             historyUpdateLoad(schedulerStat, time.time(), l)

                        # display progress (consider use of tqdm)
                        lt = time.localtime(time.time())
                        if api_type != 'granta':
                            try:
                                #stats = restApiControl.getStatScheduler()
                                # bp HUHUHUHUHUHUUH
                                log.info(str(lt.tm_mday)+"."+str(lt.tm_mon)+"."+str(lt.tm_year)+" "+str(lt.tm_hour)+":"+str(lt.tm_min)+":"+str(lt.tm_sec)+" Scheduled/Running/Load:" +
                                    str(schedulerStat['scheduledTasks'])+"/"+str(schedulerStat['runningTasks'])+"/"+str(schedulerStat['load']))
                            except Exception as e:
                                log.error(repr(e))

                        # lazy update of persistent statistics, done in main thread thus thread safe
                        with statusLock:
                            updateStatPersistent(schedulerStat)
                        log.info("waiting..")
                        time.sleep(20)
                except Exception as err:
                    log.info("Error: " + repr(err))
                    stop(pool)
                except:
                    log.info("Unknown error encountered")
                    stop(pool)
        except pidfile.AlreadyRunningError:
            log.error('Already running.')

    log.info("Exiting MupifDB Workflow Scheduler\n")
