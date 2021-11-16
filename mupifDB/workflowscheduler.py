import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")

import time
import atexit
import tempfile
import multiprocessing
import subprocess
from pymongo import MongoClient
import gridfs
import enum
import pidfile
import workflowmanager
import zipfile
import ctypes

import restApiControl
import bson

from pathlib import Path

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
statusArray = multiprocessing.Array(ctypes.c_int, [1, 0, 0, 0, 0], lock=False)

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
    # fd.close()


def procError(r):
    print("procError called")


def updateStatRunning():
    with statusLock:
        print("updateStatRunning called")
        statusArray[index.runningTasks] += 1
        statusArray[index.scheduledTasks] -= 1
        statusArray[index.load] = ctypes.c_int(int(100 * statusArray[index.runningTasks]/poolsize))
        restApiControl.setStatScheduler(runningTasks=statusArray[index.runningTasks], scheduledTasks=statusArray[index.scheduledTasks], load=statusArray[index.load])


def updateStatScheduled():
    with statusLock:
        statusArray[index.scheduledTasks] += 1
        restApiControl.setStatScheduler(scheduledTasks=statusArray[index.scheduledTasks])


def updateStatFinished():
    with statusLock:
        statusArray[index.runningTasks] -= 1
        statusArray[index.processedTasks] += 1
        statusArray[index.load] = ctypes.c_int(int(100*statusArray[index.runningTasks]/poolsize))
        restApiControl.setStatScheduler(runningTasks=statusArray[index.runningTasks], load=statusArray[index.load], processedTasks=statusArray[index.processedTasks])


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
    # returns a tuple (we_id, result)
    print("executeWorkflow invoked")
    log.info("executeWorkflow invoked")
    client = MongoClient()
    db = client.MuPIF
    fs = gridfs.GridFS(db)
    print("database connected")
    log.info("database connected")
    # get workflow execution record
    we_rec = restApiControl.getExecutionRecord(we_id)
    if we_rec is None:
        print("Workflow Execution record %s not found" % we_id)
        log.error("Workflow Execution record %s not found" % we_id)
        raise KeyError("Workflow Execution record %s not found" % we_id)
    else:
        print("Workflow Execution record %s found" % we_id)
        log.info("Workflow Execution record %s found" % we_id)

    # get workflow record (needed to get workflow source to execute
    workflowVersion = we_rec['WorkflowVersion']
    wid = we_rec['WorkflowID']
    id = we_rec['_id']
    wd = workflowmanager.getWorkflowDoc(wid, version=workflowVersion)
    if wd is None:
        print("Workflow document with wid %s, verison %s not found" % (wid, workflowVersion))
        log.error("Workflow document with wid %s, verison %s not found" % (wid, workflowVersion))
        raise KeyError("Workflow document with ID %s, version %s not found" % (wid, workflowVersion))
    else:
        print("Workflow document with wid %s, id %s, version %s found" % (wid, id, workflowVersion))
        log.info("Workflow document with wid %s, id %s, version %s found" % (wid, id, workflowVersion))

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

                with open(tempDir+'/w.py', "wb") as f:
                    f.write(restApiControl.getBinaryFileContentByID(wd['GridFSID']))

                p = Path(tempDir)
                for it in p.iterdir():
                    print(it)

            except Exception as e:
                log.error(str(e))
                # set execution code to failed ...yes or no?
                return 1

            # execute
            print("Executing we_id %s, tempdir %s" % (we_id, tempDir))
            log.info("Executing we_id %s, tempdir %s" % (we_id, tempDir))
            # print("Executing we_id %s, tempdir %s" % (we_id, tempDir))
            # update status
            updateStatRunning()
            restApiControl.setExecutionStatusRunning(we_id)
            cmd = ['/usr/bin/python3', tempDir+'/w.py', '-eid', str(we_id)]
            print(cmd)
            completed = subprocess.call(cmd, cwd=tempDir)
            # print(tempDir)
            print('command:' + str(cmd) + ' Return Code:'+str(completed))
            # store execution log
            logID = None

            p = Path(tempDir)
            for it in p.iterdir():
                print(it)

            try:
                print("Copying log files to database")
                log.info("Copying log files to database")
                with open(tempDir+'/mupif.log', 'rb') as f:
                    logID = fs.put(f, filename="mupif.log")  # todo
                log.info("Copying log files done")
                print("Copying log files done")
            except:
                log.info("Copying log files was not successful")
                print("Copying log files was not successful")

            # update status
            updateStatFinished()
        log.info("Updating we_id %s status to %s" % (we_id, completed))
        # set execution code to completed
        if completed == 0:
            print("Workflow execution %s Finished" % we_id)
            log.info("Workflow execution %s Finished" % we_id)
            restApiControl.setExecutionStatusFinished(we_id, logID)
            return we_id, ExecutionResult.Finished
        else:
            print("Workflow execution %s Failed" % we_id)
            log.info("Workflow execution %s Failed" % we_id)
            restApiControl.setExecutionStatusFailed(we_id, logID)
            return we_id, ExecutionResult.Failed

    else:
        print("WEID %s not scheduled for execution" % we_id)
        log.error("WEID %s not scheduled for execution" % we_id)
        raise KeyError("WEID %s not scheduled for execution" % we_id)


def stop(var_pool):
    log.info("Stopping the scheduler, waiting for workers to terminate")
    restApiControl.setStatScheduler(runningTasks=statusArray[index.runningTasks], scheduledTasks=statusArray[index.scheduledTasks], load=statusArray[index.load], processedTasks=statusArray[index.processedTasks])
    var_pool.close()  # close pool
    var_pool.join()  # wait for completion
    log.info("All tasks finished, exiting")


if __name__ == '__main__':

    restApiControl.setExecutionStatusScheduled('6192db6aa76a4dd9e08a90de')

    setupLogger(fileName="scheduler.log")
    with statusLock:
        statusArray[index.status] = 1
        restApiControl.setStatScheduler(runningTasks=statusArray[index.runningTasks], scheduledTasks=statusArray[index.scheduledTasks], load=statusArray[index.load], processedTasks=statusArray[index.processedTasks])

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
                        # result1 = pool.apply_async(test)
                        # log.info(result1.get())
                        result = pool.apply_async(executeWorkflow, args=(weid,), callback=procFinish, error_callback=procError)
                        # log.info(result.get())
                        log.info("WEID %s added to the execution pool" % weid)
                    # ok, no more jobs to schedule for now, wait

                    # display progress (consider use of tqdm)
                    lt = time.localtime(time.time())
                    print(str(lt.tm_mday)+"."+str(lt.tm_mon)+"."+str(lt.tm_year)+" "+str(lt.tm_hour)+":"+str(lt.tm_min)+":"+str(lt.tm_sec)+" Scheduled/Running/Load:" +
                          str(statusArray[index.scheduledTasks])+"/"+str(statusArray[index.runningTasks])+"/"+str(statusArray[index.load]))
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
