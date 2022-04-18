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

import restApiControl
import my_email

from pathlib import Path
import shutil
import datetime

import logging
# logging.basicConfig(filename='scheduler.log',level=logging.DEBUG)
log = logging.getLogger()

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


poolsize = 3
statusLock = multiprocessing.Lock()

fd = None
buf = None


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
    print("procError called")


def updateStatRunning():
    with statusLock:
        print("updateStatRunning called")
        restApiControl.updateStatScheduler(scheduledTasks=-1, runningTasks=+1)
        stats_temp = restApiControl.getStatScheduler()
        restApiControl.setStatScheduler(load=int(100 * int(stats_temp['runningTasks']) / poolsize))


def updateStatScheduled():
    with statusLock:
        print("updateStatScheduled called")
        restApiControl.updateStatScheduler(scheduledTasks=+1)


def updateStatFinished():
    with statusLock:
        print("updateStatFinished called")
        restApiControl.updateStatScheduler(runningTasks=-1, processedTasks=+1)
        stats_temp = restApiControl.getStatScheduler()
        restApiControl.setStatScheduler(load=int(100*int(stats_temp['runningTasks'])/poolsize))


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


def executeWorkflow(we_id):
    log.info("executeWorkflow invoked")
    print("database connected")
    log.info("database connected")
    # get workflow execution record
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
    if we_rec['Status'] == 'Scheduled':
        completed = 1
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

                fn = restApiControl.getFileNameByID(workflow_record['GridFSID'])
                with open(tempDir + '/' + fn, "wb") as f:
                    f.write(restApiControl.getBinaryFileContentByID(workflow_record['GridFSID']))
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
                    else:
                        print("Filename check FAILED")

                else:
                    print("Unsupported file extension")

                print("Copying executor script.")

                execScript = Path(tempDir+'/workflow_execution_script.py')
                shutil.copy(mupifDBModDir+'/workflow_execution_script.py', execScript)
            except Exception as e:
                log.error(str(e))
                # set execution code to failed ...yes or no?
                return we_id, ExecutionResult.Failed

            # execute
            print("Executing we_id %s, tempdir %s" % (we_id, tempDir))
            log.info("Executing we_id %s, tempdir %s" % (we_id, tempDir))
            # update status
            updateStatRunning()
            restApiControl.setExecutionStatusRunning(we_id)
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
                log.info("Copying log files to database")
                # if os.path.exists(tempDir+'/mupif.log'):
                with open(workflowLogName, 'rb') as f:
                    logID = restApiControl.uploadBinaryFileContent(f)
                    if logID is not None:
                        restApiControl.setExecutionParameter(we_id, 'ExecutionLog', logID)
                log.info("Copying log files done")
            except:
                log.info("Copying log files was not successful")

            # update status
            updateStatFinished()
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
    restApiControl.setStatScheduler(runningTasks=0, scheduledTasks=0, load=0, processedTasks=0)
    var_pool.close()  # close pool
    var_pool.join()  # wait for completion
    log.info("All tasks finished, exiting")


if __name__ == '__main__':
    # for testing
    # restApiControl.setExecutionStatusPending('61a5854c97ac8ebf9887bbc1')

    setupLogger(fileName="scheduler.log")

    with statusLock:
        import requests.adapters
        import urllib3
        adapter = requests.adapters.HTTPAdapter(max_retries=urllib3.Retry(total=8, backoff_factor=.05))
        session = requests.Session()
        for proto in ('http://', 'https://'):
            session.mount(proto, adapter)
        restApiControl.setStatScheduler(runningTasks=0, scheduledTasks=0, load=0, processedTasks=0, session=session)

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
                    result = pool.apply_async(executeWorkflow, args=(weid,), callback=procFinish, error_callback=procError)
                    log.info(result)
                    log.info("WEID %s added to the execution pool" % weid)
                log.info("Done\n")

                log.info("Entering loop to check for Pending executions")
                # add new execution (Pending)
                while True:
                    # retrieve weids with status "Scheduled" from DB
                    for wed in restApiControl.getPendingExecutions():
                        print(str(wed['_id']) + " found as pending")
                        # add the correspoding weid to the pool, change status to scheduled
                        weid = wed['_id']
                        if not restApiControl.setExecutionStatusScheduled(weid):
                            print("Could not update execution status")
                        else:
                            print("Updated status of execution")
                        updateStatScheduled()  # update status
                        result = pool.apply_async(executeWorkflow, args=(weid,), callback=procFinish, error_callback=procError)
                        # log.info(result.get())
                        log.info("WEID %s added to the execution pool" % weid)
                    # ok, no more jobs to schedule for now, wait

                    # display progress (consider use of tqdm)
                    lt = time.localtime(time.time())
                    stats = restApiControl.getStatScheduler()
                    print(str(lt.tm_mday)+"."+str(lt.tm_mon)+"."+str(lt.tm_year)+" "+str(lt.tm_hour)+":"+str(lt.tm_min)+":"+str(lt.tm_sec)+" Scheduled/Running/Load:" +
                          str(stats['scheduledTasks'])+"/"+str(stats['runningTasks'])+"/"+str(stats['load']))
                    time.sleep(30)
            except Exception as err:
                log.info("Error: " + repr(err))
                stop(pool)
            except:
                log.info("Unknown error encountered")
                stop(pool)
    except pidfile.AlreadyRunningError:
        log.error('Already running.')

    log.info("Exiting MupifDB Workflow Scheduler\n")
