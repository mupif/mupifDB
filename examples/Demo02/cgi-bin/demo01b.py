#!/usr/bin/python3
from pymongo import MongoClient
import mupifDB
from bson import ObjectId
from datetime import datetime
import demoapp01
import cgi

if __name__ == "__main__":

    client = MongoClient()
    db = client.MuPIF

    form = cgi.FieldStorage() 
    if "eid" not in form:
        #generate new execution record
        wid = 'Workflow99'
        #mupifDB.workflowmanager.insertWorkflowDefinition(db,wid,'Demo','1.0','file://localhost/home/bp/devel/mupifDB/examples/Demo02/demoapp01.py', 
        #                                                'DemoUseCase', workflow.getMetadata('inputs'), workflow.getMetadata('outputs'))
        # schedule execution
        c = mupifDB.workflowmanager.WorkflowExecutionContext.create(db, wid, 'borpat@senam.cz' )
        #set inputs in DB
        # consider inputs optionally regerenced by ID (PropertyID.PID______)
        #
        print("Content-type: text/html\n")
        print ("<html><body>")
        mupifDB.workflowmanager.generateWEInputCGI(db, c.executionID)
        print ("</body></html>")
    else:
        eid = form.getvalue('eid')
        print("Content-type: text/html\n")
        print ("<html><body>")
        print ("Summary of input record for EID:%s<br>"%eid)
        c = mupifDB.workflowmanager.WorkflowExecutionContext(db, eid)
        mupifDB.workflowmanager.setWEInputCGI (db, eid, form)
        print('<br>')
        print("<a href=http://localhost:5000/executeworkflow/%s> EXECUTE </a>"%eid)
        print ("</body></html>")
