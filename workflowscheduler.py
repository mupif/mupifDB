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
from enum import Enum
import math
import pidfile

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

class ExecutionResult(Enum):
    Finished = 1  #successfull 
    Failed   = 2



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


# some global vars to monitor progress

class Progress:
    def __init__(self, db):
        #self.db=db
        self.numberOfTasks = 0
        self.numberOfFinishedTasks = 0
        self.numberOfFailedTasks = 0
        #sd = db.Scheduler.find_one({"_id": 'Status'})
        #if sd:
        #    self.numberOfTasks = sd['NumberOfTasks']
        #    self.numberOfFinishedTasks = sd['NumberOfFinishedTasks']
        #    self.numberOfFailedTasks = sd['NumberOfFailedTasks']
        #else:
        #    db.Scheduler.insert_one({'_id':'Status', 'NumberOfTasks': self.numberOfTasks, 'NumberOfFinishedTasks':self.numberOfFinishedTasks, 'NumberOfFailedTasks':self.numberOfFailedTasks})
        
        
    def updateProgress(self, result):
        print("Progress: received " + str(result))
        (weid, res) = result
        if (res == ExecutionResult.Finished):
            self.numberOfFinishedTasks+=1
        else:
            self.numberOfFailedTasks+=1
    def updateTotals(self):
        self.numberOfTasks+=1
    def commit(self):
        # commits the state to db
        # self.db.Scheduler.update_one({'_id': 'Status'}, {'$set': {'NumberOfTasks': self.numberOfTasks, 'NumberOfFinishedTasks':self.numberOfFinishedTasks, 'NumberOfFailedTasks':self.numberOfFailedTasks}})
        return
    
    def __str__(self):
        numberOfProcessedTasks = self.numberOfFinishedTasks+self.numberOfFailedTasks
        if self.numberOfTasks > 0:
            progress = round(100*(numberOfProcessedTasks/self.numberOfTasks))
        else:
            progress = 0
        return "Scheduler {numberOfTasks: %s, numberOfProcessedTasks: %s (numberOfFinished:%s, numberOfFailed:%s), Progress:%s%%}"%(self.numberOfTasks, numberOfProcessedTasks, self.numberOfFinishedTasks, self.numberOfFailedTasks, progress )


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

    #get workflow record (needed to get workflow source to execute)
    wid = wed['WorkflowID']
    wd = db.Workflows.find_one({"_id": wid})
    if (wd is None):
        log.error ("Workflow document wit ID %s not found"%(wid))
        raise KeyError ("Workflow document wit ID %s not found"%(wid))
    else:
        log.info ("Workflow document wit ID %s found"%(wid))
    
    #check if status is "Scheduled"
    if (wed['Status']=='Scheduled'):
        completed = 1
        log.info("wed status is Scheduled, processing")
        # execute the selected workflow
        # take workflow source and run python interpreter on it in a temporary directory
        tempRoot = '/tmp'
        log.info("Creating temp dir")
        with tempfile.TemporaryDirectory(dir=tempRoot, prefix='mupifDB') as tempDir:
            #tempDir = tempfile.mkdtemp(dir=tempRoot, prefix='mupifDB_'+weid)
            log.info("temp dir created")
            #copy workflow source to tempDir
            try:
                urllib.request.urlretrieve (wd['Source'], tempDir+'/w.py')
            except Exception as e:
                log.error (str(e))
                # set execution code to failed
                #db.WorkflowExecutions.update_one({'_id': id}, {'$set': {'Status': 'Failed'}})
                return 1
            #execute
            log.info("Executing weid %s, tempdir %s"%(weid, tempDir))
            #print("Executing weid %s, tempdir %s"%(weid, tempDir))
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
    

def stop ():
    log.info("Stopping the scheduler, waiting for workers to terminate")
    pool.close() # close pool
    pool.join() # wait for comppletion
    log.info ("All tasks finished, exiting")


atexit.register(stop)
pool = multiprocessing.Pool(processes=4)


if __name__ == '__main__':
    client = MongoClient()
    db = client.MuPIF
    fs = gridfs.GridFS(db)
    p = Progress(db)
    setupLogger(fileName="scheduler.log")
    
    try:
        with pidfile.PIDFile(filename='mupifDB_scheduler_pidfile'):
            log.info ("Starting MupifDB Workflow Scheduler\n")

            try:

                #import first already scheduled executions
                log.info("Importing already scheduled executions")
                for wed in db.WorkflowExecutions.find({"Status": 'Scheduled'}):                                                                                                                           
                    # add the correspoding weid to the pool, change status to scheduled                                                                                                                
                    weid = wed['_id']
                    req = pool.apply_async(executeWorkflow, args=(weid,), callback=p.updateProgress)
                    p.updateTotals()
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
                        req = pool.apply_async(executeWorkflow, args=(weid,), callback=p.updateProgress)
                        p.updateTotals()
                        log.info("WEID %s added to the execution pool"%(weid))
                    # ok, no more jobs to schedule for now, wait

                    # display progress (consider use of tqdm)
                    lt = time.localtime(time.time())
                    print(str(lt.tm_mday)+"."+str(lt.tm_mon)+"."+str(lt.tm_year)+" "+str(lt.tm_hour)+":"+str(lt.tm_min)+":"+str(lt.tm_sec)+" "+str(p))
                    p.commit()
                    time.sleep(60)
            except Exception as err:
                log.info ("Error: " + repr(err))
                stop()
            except:
                log.info("Unknown error encountered")
                stop()
    except pidfile.AlreadyRunningError:
        log.error ('Already running.')

    log.info ("Exiting MupifDB Workflow Scheduler\n")
        

