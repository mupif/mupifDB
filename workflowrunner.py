import tempfile
import urllib.request
import multiprocessing
import subprocess
import mupifDB
from datetime import datetime
from pymongo import MongoClient
import gridfs

#client = MongoClient()
#db = client.MuPIF

def execWorkflow (id, wed, wd):

    client = MongoClient()
    db = client.MuPIF
    fs = gridfs.GridFS(db)

    print ("__executeWorkflow called")
    print (" workflow execution status is %s"%wed['Status'])
    if (wed['Status']=='Created'):
        # execute the selected workflow
        # take workflow source and run python interpreter on it in a temporary directory
        tempRoot = '/tmp'
        tempDir = tempfile.mkdtemp(dir=tempRoot, prefix='mupifDB_tmp')
        #copy workflow source to tempDir
        try:
            urllib.request.urlretrieve (wd['Source'], tempDir+'/w.py')
        except Exception as e:
            print (e)
            # set execution code to failed
            #db.WorkflowExecutions.update_one({'_id': id}, {'$set': {'Status': 'Failed'}})
            return
        #execute
        db.WorkflowExecutions.update_one({'_id': id}, {'$set': {'Status': 'Running', 'StartDate':str(datetime.now())}})
        #wec.set('StartDate', str(datetime.now()))
        cmd = ['/usr/bin/python3',tempDir+'/w.py', '-eid', str(id) ]
        print (cmd)
        completed = subprocess.call(cmd, cwd=tempDir)
        print (tempDir)
        print ('command:' + str(cmd) + ' Return Code:'+str(completed))
        #store execution log
        logID = None
        with open(tempDir+'/mupif.log', 'rb') as f:
            logID=fs.put(f)
        #set execution code to completed
        if (completed == 0):
            db.WorkflowExecutions.update_one({'_id': id}, {'$set': {'Status': 'Finished', 'EndDate':str(datetime.now()), 'ExecutionLog': logID}})
        else:
            db.WorkflowExecutions.update_one({'_id': id}, {'$set': {'Status': 'Failed', 'EndDate':str(datetime.now()), 'ExecutionLog': logID}})
        return 0
    else:
        print ("Workflow execution already scheduled for execution")
        raise KeyError ("Workflow execution already scheduled for execution")
