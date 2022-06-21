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

import restApiControl
import my_email

from pathlib import Path
import shutil
import datetime

import Pyro5
import mupif as mp

import logging
# logging.basicConfig(filename='scheduler.log',level=logging.DEBUG)
log = logging.getLogger()

# try to import schedulerconfig.py
authToken = None
try:
    import schedulerConfig
    authKey = schedulerConfig.authToken
except ImportError:
    print("schedulerConfig import failed")
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

# global vars 
runningTasks = 0
scheduledTasks = 0
processedTasks = 0
finishedTasks = 0 # with success
failedTasks = 0
lastJobs = {} # dict, we-id key


schedulerStatFile = "/var/lib/mupif/persistent/scheduler-stat.json"

api_type = os.environ.get('MUPIFDB_REST_SERVER_TYPE', "mupif")

ns=mp.pyroutil.connectNameserver()
ns_uri=str(ns._pyroUri)


poolsize = 3
stopFlag = False # set to tru to end main scheduler loop

fd = None
buf = None

@Pyro5.api.expose
class SchedulerMonitor (object):
    def __init__(self, ns, schedulerStat):
        self.ns = ns
        self.stat = schedulerStat
    def runServer(self):
        return mp.pyroutil.runServer(ns=self.ns, appName="mupif.scheduler", app=self, metadata=set("type:scheduler"))
    def getStatistics(self):
        runningTaks=self.stat['runningTasks']
        scheduledTasks=self.stat['scheduledTasks']
        processedTasks = self.stat['processedTasks']
        finishedTasks=self.stat['finishedTasks']
        failedTasks=self.stat['failedTasks']
        lastJobs=self.stat['lastJobs']
        return {
            'runningTaks':runningTasks, 
            'scheduledTasks':scheduledTasks,
            'processedTasks': processedTasks,
            'finishedTasks': finishedTasks,
            'failedTasks': failedTasks,
            'lastJobs': lastJobs 
            } 
    def stop (self):
        stopFlag=True
        self.ns.remove("mupif.cheduler")
    # no-op: runServer wants  this for some reason?
    def registerPyro(self,daemon,ns,uri,appName,externalDaemon): pass




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
    print("procInit called")


def procFinish(r):
    print("procFinish called")


def procError(r):
    print("procError called:"+r)


def updateStatRunning(lock, schedulerStat, we_id, wid):
    if api_type != 'granta':
        with lock:
            print("updateStatRunning called")
            print (schedulerStat)
            print ('------------------')
            updateStatPersistent(scheduledTasks=-1, runningTasks=+1)
            #restApiControl.setStatScheduler(load=int(100 * int(stats_temp['runningTasks']) / poolsize))
            #
            schedulerStat['scheduledTasks'] =  schedulerStat['scheduledTasks']-1
            schedulerStat['runningTasks']=schedulerStat['runningTasks']+1
            #schedulerStat['runningJobs'].append(str(we_id)+':'+str(wid)) # won't work
            #Modifications to mutable values or items in dict and list proxies will not be propagated through the manager, 
            #because the proxy has no way of knowing when its values or items are modified. 
            #To modify such an item, you can re-assign the modified object to the container proxy.
            jobs = [(we_id, wid, 'Running', datetime.datetime.now().isoformat(timespec='seconds'), '-')]
            for i in range(min(4,len(schedulerStat['lastJobs']))):
                jobs.append(schedulerStat['lastJobs'][i])
            schedulerStat['lastJobs'] = jobs

            print (we_id, wid)
            print (schedulerStat)
            print ('=======================')


def updateStatScheduled(lock, schedulerStat):
    if api_type != 'granta':
        with lock:
            print("updateStatScheduled called")
            updateStatPersistent(scheduledTasks=+1)
            #
            schedulerStat['scheduledTasks']=schedulerStat['scheduledTasks']+1

def updateStatFinished(lock, schedulerStat, retCode, we_id):
    if api_type != 'granta':
        with lock:
            print("updateStatFinished called")
            
            updateStatPersistent(runningTasks=-1, processedTasks=+1, finishedTasks=int(retCode==0), failedTasks=int(retCode==1))
            stats_temp = restApiControl.getStatScheduler()
            restApiControl.setStatScheduler(load=int(100*int(stats_temp['runningTasks'])/poolsize))
            #
            schedulerStat['runningTasks']=schedulerStat['runningTasks']-1
            if (retCode == 0):
                schedulerStat['processedTasks']=schedulerStat['processedTasks']+1
                schedulerStat['finishedTasks']=schedulerStat['finishedTasks']+1
            elif (retCode == 1):
                schedulerStat['processedTasks']=schedulerStat['processedTasks']+1
                schedulerStat['failedTasks'] =schedulerStat['failedTasks']+1
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

            print (schedulerStat)
            print ('FFFFFFFFFFFFFFFFFFFF')


def updateStatPersistent (runningTasks=0, processedTasks=0, scheduledTasks=0, finishedTasks=0, failedTasks=0):
    #print("updateStatPersistent called")
    with open(schedulerStatFile, 'r+') as jsonFile:
        stat = json.load(jsonFile)
        #print(stat)
        if (runningTasks):
            stat['runningTasks'] = stat['runningTasks']+runningTasks
        if (processedTasks):
            stat['processedTasks'] = stat['processedTasks']+ processedTasks
        if (scheduledTasks):
            stat['scheduledTasks'] = stat['scheduledTasks']+ scheduledTasks
        if (finishedTasks):
            stat['finishedTasks'] = stat['finishedTasks']+ finishedTasks
        if (failedTasks):
            stat['failedTasks'] = stat['failedTasks']+failedTasks
        jsonFile.seek(0)
        #print(stat)
        json.dump(stat, jsonFile)
        #print("Update:", stat)
    #print("updateStatPersistent finished")
  


def setupLogger(fileName, level=logging.DEBUG):
    """
    Set up a logger which prints messages on the screen and simultaneously saves them to a file.
    The file has the suffix '.log' after a loggerName.

    :param str fileName: file name, the suffix '.log' is appended.
    :param object level: logging level. Allowed values are CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET
    :rtype: logger instance
    """
    logger = logging.getLogger()
    formatLog = '%(asctime)s %(levelname)s:%(filename)s:%(lineno)d %(message)s \n'
    formatTime = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(formatLog, formatTime)
    fileHandler = logging.FileHandler(fileName, mode='w')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)

    logger.setLevel(level)
    logger.addHandler(fileHandler)
    logger.addHandler(streamHandler)

    return logger

def copyLogToDB (we_id, workflowLogName):
    log.info("Copying log files to database")
    # if os.path.exists(tempDir+'/mupif.log'):
    # temp
    with open(workflowLogName, 'rb') as f:
        d = f.read()
        f.close()
        f = open('./temp_log.log', 'wb')
        f.write(d)
        f.close()

    with open(workflowLogName, 'rb') as f:
        logID = restApiControl.uploadBinaryFile(f)
        if logID is not None:
            restApiControl.setExecutionParameter(we_id, 'ExecutionLog', logID)
        log.info("Copying log files done")


def executeWorkflow(lock, schedulerStat, we_id):
    log.info("executeWorkflow invoked")

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
        print("Workflow document with wid %s, verison %s not found" % (wid, workflowVersion))
        log.error("Workflow document with wid %s, verison %s not found" % (wid, workflowVersion))
        raise KeyError("Workflow document with ID %s, version %s not found" % (wid, workflowVersion))
    else:
        print("Workflow document with wid %s, id %s, version %s found" % (wid, we_rec['_id'], workflowVersion))
        log.info("Workflow document with wid %s, id %s, version %s found" % (wid, we_rec['_id'], workflowVersion))

    # check if status is "Scheduled"
    if we_rec['Status'] == 'Scheduled' or api_type == 'granta':  # todo remove granta
        completed = 1  # todo check
        print("we_rec status is Scheduled, processing")
        log.info("we_rec status is Scheduled, processing")
        # execute the selected workflow
        # take workflow source and run python interpreter on it in a temporary directory
        tempRoot = '/tmp'
        log.info("Creating temp dir")
        with tempfile.TemporaryDirectory(dir=tempRoot, prefix='mupifDB') as tempDir:
            # if (1):  # uncomment this to keep temdDir
            #     tempDir = tempfile.mkdtemp(dir=tempRoot, prefix='mupifDB_')
            print("temp dir %s created" % (tempDir,))
            log.info("temp dir %s created" % (tempDir,))
            # copy workflow source to tempDir
            try:
                python_script_filename = workflow_record['modulename'] + ".py"

                fc, fn = restApiControl.getBinaryFileByID(workflow_record['GridFSID'])
                with open(tempDir + '/' + fn, "wb") as f:
                    f.write(fc)
                    f.close()

                if fn.split('.')[-1] == 'py':
                    print("downloaded .py file..")
                    if fn == python_script_filename:
                        print("Filename check OK")
                    else:
                        print("Filename check FAILED")

                elif fn.split('.')[-1] == 'zip':
                    print("downloaded .zip file, extracting..")
                    print(fn)
                    zf = zipfile.ZipFile(tempDir + '/' + fn, mode='r')
                    filenames = zipfile.ZipFile.namelist(zf)
                    print("Zipped files:")
                    print(filenames)
                    zf.extractall(path=tempDir)
                    if python_script_filename in filenames:
                        print("Filename check OK")
                        log.info("Filename check OK")
                    else:
                        print ("Filename check FAILED")
                        log.error("Filename check FAILED")

                else:
                    log.error("Unsupported file extension")

                print("Copying executor script.")

                execScript = Path(tempDir+'/workflow_execution_script.py')
                shutil.copy(mupifDBModDir+'/workflow_execution_script.py', execScript)
            except Exception as e:
                log.error(str(e))
                # set execution code to failed ...yes or no?
                restApiControl.setExecutionStatusFailed(we_id)
                my_email.sendEmailAboutExecutionStatus(we_id)
                try:
                    copyLogToDB(we_id, workflowLogName)
                except:
                    log.info("Copying log files was not successful")

                return we_id, ExecutionResult.Failed

            # execute
            print("Executing we_id %s, tempdir %s" % (we_id, tempDir))
            log.info("Executing we_id %s, tempdir %s" % (we_id, tempDir))
            # update status
            updateStatRunning(lock, schedulerStat, we_id, wid)
            #runningJobs[we_id]=wid # for runtime monitoring
            restApiControl.setExecutionStatusRunning(we_id)
            restApiControl.setExecutionAttemptsCount(we_id, int(we_rec['Attempts'])+1)
            # uses the same python interpreter as the current process
            cmd = [sys.executable, execScript, '-eid', str(we_id)]
            # print(cmd)
            workflowLogName = tempDir+'/workflow.log'
            with open(workflowLogName, 'w') as workflowLog:
                ll = 10*'='
                workflowLog.write(f'''
{ll} WORKFLOW STARTING at {(t0:=datetime.datetime.now()).isoformat(timespec='seconds')} {ll}
{ll} command is {cmd} {ll}''')
                env = os.environ.copy()
                if 'PYTHONPATH' in env:
                    env['PYTHONPATH'] += f'{os.pathsep}{mupifDBSrcDir}'
                else:
                    env['PYTHONPATH'] = mupifDBSrcDir
                env['MUPIF_NS'] = ns_uri
                env['MUPIFDB_REST_SERVER_TYPE'] = api_type

                completed = subprocess.call(cmd, cwd=tempDir, stderr=subprocess.STDOUT, stdout=workflowLog, env=env)
                workflowLog.write(f'''
{ll} WORKFLOW FINISHED at {(t1:=datetime.datetime.now()).isoformat(timespec='seconds')} {ll}
{ll} duration: {str((dt:=(t1-t0))-datetime.timedelta(microseconds=dt.microseconds))} {ll}
{ll} exit status of {cmd}: {completed} ({'ERROR' if completed!=0 else 'SUCCESS'}) {ll}''')

            # print(tempDir)
            log.info('command:' + str(cmd) + ' Return Code:'+str(completed))

            # store execution log
            logID = None

            p = Path(tempDir)
            for it in p.iterdir():
                print(it)

            try:
                copyLogToDB(we_id, workflowLogName)
            except:
                log.info("Copying log files was not successful")

            # update status
            updateStatFinished(lock, schedulerStat, completed, we_id)
            #del runningJobs[we_id] # remove we_id from running jobs; for monitoring
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

    else:
        log.error("WEID %s not scheduled for execution" % we_id)
        raise KeyError("WEID %s not scheduled for execution" % we_id)


def stop(var_pool):
    log.info("Stopping the scheduler, waiting for workers to terminate")
    # @TODO: do not reset processedTasks, continue conting
    restApiControl.setStatScheduler(runningTasks=0, scheduledTasks=0, load=0, processedTasks=0)
    var_pool.close()  # close pool
    var_pool.join()  # wait for completion
    log.info("All tasks finished, exiting")


def checkWorkflowResources(wid, version):
    workflow = restApiControl.getWorkflowRecordGeneral(wid, version)
    models_md = workflow.get('Models', [])
    try:
        res = mp.Workflow.checkModelRemoteResourcesByMetadata(models_md=models_md)
        return res
    except:
        return False


def checkExecutionResources(eid):
    print("Checking execution resources")
    if api_type == 'granta':
        return True  # todo granta temporary
    execution = restApiControl.getExecutionRecord(eid)
    return checkWorkflowResources(execution['WorkflowID'], execution['WorkflowVersion'])


if __name__ == '__main__':
    # for testing
    # restApiControl.setExecutionStatusPending('a6e623e7-12a5-4da3-8d40-fc1e7ec00811')

    setupLogger(fileName="scheduler.log")
    #statusLock = multiprocessing.Lock()

    if (Path(schedulerStatFile).is_file()):
        with open(schedulerStatFile,'r') as f:
            stat=json.load(f)
    else:
        # create empty stat
        stat={'runningTasks':0, 'scheduledTasks': 0, 'load':0, 'processedTasks':0, 'finishedTasks':0, 'failedTasks':0}
        with open(schedulerStatFile,'w') as f:
            json.dump(stat, f)

    import requests.adapters
    import urllib3
    adapter = requests.adapters.HTTPAdapter(max_retries=urllib3.Retry(total=8, backoff_factor=.05))
    session = requests.Session()
    for proto in ('http://', 'https://'):
        session.mount(proto, adapter)
    #
    # @bp: do we want to reset processedTasks to zero? Perhaps keeping it ?
    #
    restApiControl.setStatScheduler(runningTasks=0, scheduledTasks=0, load=0, processedTasks=0, session=session)

    

    with multiprocessing.Manager() as manager:
        schedulerStat=manager.dict()
        schedulerStat['runningTasks'] = 0
        schedulerStat['scheduledTasks'] = 0
        schedulerStat['load'] = 0
        schedulerStat['processedTasks'] = stat['processedTasks']
        schedulerStat['finishedTasks'] = stat['finishedTasks']
        schedulerStat['failedTasks'] = stat['failedTasks']
        schedulerStat['lastJobs'] = [] #manager.list()
        statusLock = manager.Lock()

        monitor = SchedulerMonitor(ns, schedulerStat)
        #run scheduler monitor 
        monitor.runServer()

        
        pool = multiprocessing.Pool(processes=poolsize, initializer=procInit)
        atexit.register(stop, pool)
        try:
            with pidfile.PIDFile(filename='mupifDB_scheduler_pidfile'):
                log.info("Starting MupifDB Workflow Scheduler\n")

                try:
                    # import first already scheduled executions
                    log.info("Importing already scheduled executions")
                    for wed in restApiControl.getScheduledExecutions():
                        print(str(wed['_id']) + " found as Scheduled")
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
                            we_rec = restApiControl.getExecutionRecord(weid)
                            restApiControl.setExecutionAttemptsCount(weid, int(we_rec['Attempts']) + 1)

                    log.info("Done\n")

                    log.info("Entering main loop to check for Pending executions")
                    # add new execution (Pending)
                    while stopFlag is not True:
                        # retrieve weids with status "Scheduled" from DB
                        for wed in restApiControl.getPendingExecutions():
                            print(str(wed['_id']) + " found as pending")
                            weid = wed['_id']

                            # check number of attempts for execution
                            if wed['Attempts'] > 60*10:
                                restApiControl.setExecutionStatusCreated(weid)
                                if api_type != 'granta':
                                    my_email.sendEmailAboutExecutionStatus(weid)

                            else:
                                if checkExecutionResources(weid):
                                    # add the correspoding weid to the pool, change status to scheduled
                                    if not restApiControl.setExecutionStatusScheduled(weid):
                                        print("Could not update execution status")
                                    else:
                                        print("Updated status of execution")
                                    updateStatScheduled(statusLock, schedulerStat)  # update status
                                    result = pool.apply_async(executeWorkflow, args=(statusLock, schedulerStat, weid), callback=procFinish, error_callback=procError)
                                    # log.info(result.get())
                                    log.info("WEID %s added to the execution pool" % weid)
                                else:
                                    log.info("WEID %s cannot be scheduled due to unavailable resources" % weid)
                                    if api_type != 'granta':
                                        we_rec = restApiControl.getExecutionRecord(weid)
                                        restApiControl.setExecutionAttemptsCount(weid, int(we_rec['Attempts']) + 1)

                            break
                        # ok, no more jobs to schedule for now, wait

                        # display progress (consider use of tqdm)
                        lt = time.localtime(time.time())
                        if api_type != 'granta':
                            stats = restApiControl.getStatScheduler()
                            print(str(lt.tm_mday)+"."+str(lt.tm_mon)+"."+str(lt.tm_year)+" "+str(lt.tm_hour)+":"+str(lt.tm_min)+":"+str(lt.tm_sec)+" Scheduled/Running/Load:" +
                                str(stats['scheduledTasks'])+"/"+str(stats['runningTasks'])+"/"+str(stats['load']))
                        print("waiting..")
                        time.sleep(60)
                except Exception as err:
                    log.info("Error: " + repr(err))
                    stop(pool)
                except:
                    log.info("Unknown error encountered")
                    stop(pool)
        except pidfile.AlreadyRunningError:
            log.error('Already running.')

    log.info("Exiting MupifDB Workflow Scheduler\n")
