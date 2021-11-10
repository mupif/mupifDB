# todo Is this file used?

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
import tempfile
import urllib.request
import multiprocessing
import subprocess
from datetime import datetime
from pymongo import MongoClient
import gridfs
import zipfile


#client = MongoClient()
#db = client.MuPIF

def execWorkflow (wid, wed, wd):

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
            # urllib.request.urlretrieve (wd['Source'], tempDir+'/w.py'
            # uncompress zip achive in gridfs with workflow 
            wfile = fs.find_one(filter={'_id': wd['GridFSID']}) #zipfile
            zipfile.ZipFile(wfile, mode='r').extractall(path=tempDir)


        except Exception as e:
            print (e)
            # set execution code to failed
            #db.WorkflowExecutions.update_one({'_id': wid}, {'$set': {'Status': 'Failed'}})
            return
        #execute
        db.WorkflowExecutions.update_one({'_id': wid}, {'$set': {'Status': 'Running', 'StartDate':str(datetime.now())}})
        #wec.set('StartDate', str(datetime.now()))
        cmd = ['/usr/bin/python3',tempDir+'/w.py', '-eid', str(wid) ]
        print (cmd)
        completed = subprocess.call(cmd, cwd=tempDir)
        print (tempDir)
        print ('command:' + str(cmd) + ' Return Code:'+str(completed))
        #store execution log
        logID = None
        with open(tempDir+'/mupif.log', 'rb') as f:
            logID=fs.put(f, filename="mupif.log")
        #set execution code to completed
        if (completed == 0):
            db.WorkflowExecutions.update_one({'_id': wid}, {'$set': {'Status': 'Finished', 'EndDate':str(datetime.now()), 'ExecutionLog': logID}})
        else:
            db.WorkflowExecutions.update_one({'_id': wid}, {'$set': {'Status': 'Failed', 'EndDate':str(datetime.now()), 'ExecutionLog': logID}})
        return 0
    else:
        print ("Workflow execution already scheduled for execution")

