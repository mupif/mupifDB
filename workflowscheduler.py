import sys, os
import time
import atexit
import tempfile
import urllib.request
import multiprocessing
import subprocess
import mupifDB
from datetime import datetime
from pymongo import MongoClient
import gridfs
import enum
import math
import pidfile
import workflowmanager
import zipfile

import logging
#logging.basicConfig(filename='scheduler.log',level=logging.DEBUG)
log = logging.getLogger()

# WorkflowScheduler is a daemon, which 
# will try to execute pending workflow executions in DB (those with status "Scheduled")
# internally uses a multiprocessing pool to handle workflow execution requests
#


#WEID Status
#Created -> Pending -> Scheduled -> Running -> Finished | Failed
#
#Created-> the execution record allocated and initialized
#Pending -> execution record finalized (inputs sets), ready to be scheduled
#Scheduled -> execution scheduled by the scheduler
#Running -> execution processed (it is running)
#Finished|Failed -> execution finished
#  

class ExecutionResult(enum.Enum):
    Finished = 1  #successfull 
    Failed   = 2

class index(enum.IntEnum):
    status = 0
    scheduledTasks = 1
    runningTasks = 2
    processedTasks = 3
    load = 4

poolsize = 3
statusLock = multiprocessing.Lock()
statusArray = multiprocessing.Array(ctypes.c_int, [1,0,0,0,0], lock=False)

fd = None
buf= None

def procInit ():
    global fd
    global buf
    #create new empty file to back memory map on disk
    #fd = os.open('/tmp/workflowscheduler', os.O_RDWR)

    # Create the mmap instace with the following params:
    # fd: File descriptor which backs the mapping or -1 for anonymous mapping
    # length: Must in multiples of PAGESIZE (usually 4 KB)
    # flags: MAP_SHARED means other processes can share this mmap
    # prot: PROT_WRITE means this process can write to this mmap
    #buf = mmap.mmap(fd, mmap.PAGESIZE, mmap.MAP_SHARED, mmap.PROT_WRITE)
    print("procInit called")


def procFinish(r):
    print ("procFinish called")
    #fd.close()

def procError(r):
    print ("procError called")
    
def updateStatRunning(db):
    with (statusLock):
        print ("updateStatRunning called")
        statusArray[index.runningTasks] += 1
        statusArray[index.scheduledTasks] -= 1
        statusArray[index.load] = ctypes.c_int(int(100 *statusArray[index.runningTasks]/poolsize))
        db.Stat.update_one({}, {'$inc': {'scheduler.runningTasks': 1, 'scheduler.scheduledTasks': -1}, '$set': {'scheduler.load': statusArray[index.load]}})

def updateStatScheduled(db):
    with statusLock:
        statusArray[index.scheduledTasks] += 1
        db.Stat.update_one({}, {'$inc': {'scheduler.scheduledTasks': 1}})         

def updateStatFinished(db):
    with statusLock:
        statusArray[index.runningTasks] -= 1
        statusArray[index.processedTasks] += 1   
        statusArray[index.load] = ctypes.c_int(int(100*statusArray[index.runningTasks]/poolsize))
        db.Stat.update_one({}, {'$inc': {'scheduler.runningTasks': -1, 'scheduler.processedTasks': 1}, '$set': {'scheduler.load': statusArray[index.load]}})    

def setupLogger(fileName, level=logging.DEBUG):
    """
    Set up a logger which prints messages on the screen and simultaneously saves them to a file.
    The file has the suffix '.log' after a loggerName.
    
    :param str fileName: file name, the suffix '.log' is appended.
    :param object level: logging level. Allowed values are CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET
    :rtype: logger instance
    """
    l = logging.getLogger()
    # l = logging.getLogger(loggerName)
    formatLog = '%(asctime)s %(levelname)s:%(filename)s:%(lineno)d %(message)s \n'
    formatTime = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(formatLog, formatTime)
    fileHandler = logging.FileHandler(fileName, mode='w')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)

    l.setLevel(level)
    l.addHandler(fileHandler)
    l.addHandler(streamHandler)
    
    return l



def executeWorkflow(weid):
    # returns a tuple (weid, result)

    log.info("executeWorkflow invoked")
    client = MongoClient()
    db = client.MuPIF
    fs = gridfs.GridFS(db)
    log.info("db connected")
    #get workflow execution record 
    wed = db.WorkflowExecutions.find_one({'_id': weid})
    if (wed is None):
        log.error("Workflow Execution record %s not found"%(weid))
        raise KeyError ("Workflow Execution record %s not found"%(weid))
    else:
        log.info ("Workflow Execution record %s found"%(weid))

    #get workflow record (needed to get workflow source to execute
    workflowVersion = wed['WorkflowVersion']
    wid = wed['WorkflowID']
    id = wed['_id']
    wd = workflowmanager.getWorkflowDoc (db, wid, version=workflowVersion) 
    if (wd is None):
        log.error ("Workflow document with wid %s, verison %s not found"%(wid, workflowVersion))
        raise KeyError ("Workflow document wit ID %s, version %s not found"%(wid, workflowVersion))
    else:
        log.info ("Workflow document with wid %s, id %s, version %s found"%(wid, id, workflowVersion))
    
    #check if status is "Scheduled"
    if (wed['Status']=='Scheduled'):
        completed = 1
        log.info("wed status is Scheduled, processing")
        # execute the selected workflow
        # take workflow source and run python interpreter on it in a temporary directory
        tempRoot = '/tmp'
        log.info("Creating temp dir")
        with tempfile.TemporaryDirectory(dir=tempRoot, prefix='mupifDB') as tempDir:
            #if (1):  # uncomment this to keep temdDir 
            #    tempDir = tempfile.mkdtemp(dir=tempRoot, prefix='mupifDB_')
            log.info("temp dir %s created"%(tempDir,))
            #copy workflow source to tempDir
            try:
                #wpy = db.gridfs.get(wd['Source']).read()
                #with open ("tempDir+'/w.py", "w") as f:
                #    f.write(wpy)
                #print (wd)
                print("Opening gridfsID %s"%(wd['GridFSID']))
                wfile = fs.find_one(filter={'_id': wd['GridFSID']}) #zipfile
                with open (tempDir+'/w.zip', "wb") as f:
                    f.write(wfile.read())
                #print (wfile.read())
                zipfile.ZipFile(tempDir+'/w.zip', mode='r').extractall(path=tempDir)
                #urllib.request.urlretrieve (wd['Source'], tempDir+'/w.py')
            except Exception as e:
                log.error (str(e))
                # set execution code to failed
                #db.WorkflowExecutions.update_one({'_id': id}, {'$set': {'Status': 'Failed'}})
                return 1
            #execute
            log.info("Executing weid %s, tempdir %s"%(weid, tempDir))
            #print("Executing weid %s, tempdir %s"%(weid, tempDir))
            # update status
            updateStatRunning(db)
            db.WorkflowExecutions.update_one({'_id': weid}, {'$set': {'Status': 'Running', 'StartDate':str(datetime.now())}})
            #wec.set('StartDate', str(datetime.now()))
            cmd = ['/usr/bin/python3',tempDir+'/w.py', '-eid', str(weid) ]
            #print (cmd)
            completed = subprocess.call(cmd, cwd=tempDir)
            #print (tempDir)
            #print ('command:' + str(cmd) + ' Return Code:'+str(completed))
            #store execution log
            logID = None
            log.info("Copying log files to db")
            with open(tempDir+'/mupif.log', 'rb') as f:
                logID=fs.put(f, filename="mupif.log")
            log.info("Copying log files done")
            # update status
            updateStatFinished(db)
        log.info ("Updating weid %s status to %s"%(weid, completed))
        #set execution code to completed
        if (completed == 0):
            log.info ("Workflow execution %s Finished"%(weid))
            db.WorkflowExecutions.update_one({'_id': weid}, {'$set': {'Status': 'Finished', 'EndDate':str(datetime.now()), 'ExecutionLog': logID}})
            return (weid, ExecutionResult.Finished)
        else:
            log.info ("Workflow execution %s Failed"%(weid))
            db.WorkflowExecutions.update_one({'_id': weid}, {'$set': {'Status': 'Failed', 'EndDate':str(datetime.now()), 'ExecutionLog': logID}})
            return (weid, ExecutionResult.Failed)
        
    else:
        log.error("WEID %s not scheduled for execution"%(weid))
        raise KeyError ("WEID %s not scheduled for execution"%(weid))
    

def stop (pool):
    log.info("Stopping the scheduler, waiting for workers to terminate")
    client = MongoClient()                                                                                                                                                                                
    db = client.MuPIF
    db.Stat.update_one({}, {'$set': {'scheduler.runningTasks': 0, 'scheduler.scheduledTasks': 0, 'scheduler.load': 0, 'scheduler.processedTasks':0}}, upsert=True) 
    pool.close() # close pool
    pool.join() # wait for comppletion
    log.info ("All tasks finished, exiting")







if __name__ == '__main__':
    client = MongoClient()
    db = client.MuPIF
    fs = gridfs.GridFS(db)
    setupLogger(fileName="scheduler.log")
    with (statusLock):  
        statusArray[index.status] = 1
        # open 
        #create new empty file to back memory map on disk
        #fd = os.open('/tmp/workflowscheduler', os.O_CREAT|os.O_TRUNC|os.O_RDWR)
        #zero out the file to ensure it's the right size
        #assert os.write(fd, b'\x00'*mmap.PAGESIZE) == mmap.PAGESIZE
        #buf = mmap.mmap(fd, mmap.PAGESIZE, mmap.MAP_SHARED, mmap.PROT_WRITE)
        #print ("mmmap file initialized")
        #fileStat()
        db.Stat.update_one({}, {'$set': {'scheduler.runningTasks': 0, 'scheduler.scheduledTasks': 0, 'scheduler.load': 0, 'scheduler.processedTasks':0}}, upsert=True)
        
    pool = multiprocessing.Pool(processes=poolsize, initializer=procInit)
    atexit.register(stop, pool)
    try:
        with pidfile.PIDFile(filename='mupifDB_scheduler_pidfile'):
            log.info ("Starting MupifDB Workflow Scheduler\n")

            try:
            #if (1):

                #import first already scheduled executions
                log.info("Importing already scheduled executions")
                for wed in db.WorkflowExecutions.find({"Status": 'Scheduled'}):                                                                                                                           
                    # add the correspoding weid to the pool, change status to scheduled                                                                                                                
                    weid = wed['_id']
                    req = pool.apply_async(executeWorkflow, args=(weid,), callback=procFinish, error_callback=procError)
                    log.info("WEID %s added to the execution pool"%(weid))
                log.info("Done\n")

                log.info("Entering loop to check for Pending executions")
                # add new execution (Pending)
                while (True):
                    # retrieve weids with status "Scheduled" from DB
                    for wed in db.WorkflowExecutions.find({"Status": 'Pending'}):
                        # add the correspoding weid to the pool, change status to scheduled
                        weid = wed['_id']
                        db.WorkflowExecutions.update_one({'_id': weid}, {'$set': {'Status': 'Scheduled','ScheduledDate':str(datetime.now())}})
                        updateStatScheduled(db) # update status
                        req = pool.apply_async(executeWorkflow, args=(weid,), callback=procFinish, error_callback=procError)
                        log.info("WEID %s added to the execution pool"%(weid))
                    # ok, no more jobs to schedule for now, wait

                    # display progress (consider use of tqdm)
                    lt = time.localtime(time.time())
                    print(str(lt.tm_mday)+"."+str(lt.tm_mon)+"."+str(lt.tm_year)+" "+str(lt.tm_hour)+":"+str(lt.tm_min)+":"+str(lt.tm_sec)+" Scheduled/Running/Load:"+
                        str(statusArray[index.scheduledTasks])+"/"+str(statusArray[index.runningTasks])+"/"+str(statusArray[index.load]))                    
                    time.sleep(60)
            except Exception as err:
                log.info ("Error: " + repr(err))
                stop(pool)
            except:
                log.info("Unknown error encountered")
                stop(pool)
    except pidfile.AlreadyRunningError:
        log.error ('Already running.')

    log.info ("Exiting MupifDB Workflow Scheduler\n")
        

