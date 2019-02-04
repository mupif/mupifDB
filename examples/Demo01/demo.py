#!/usr/bin/python3
from pymongo import MongoClient
from mupifDB.workflowmanager import WorkflowExecutionContext
from bson import ObjectId
from datetime import datetime

client = MongoClient()
db = client.MuPIF

c = WorkflowExecutionContext (db, ObjectId("5c38b016d328f231ccf7224a"))
print(c.getIODataDoc('Inputs').get('YoungModulus'))
c.getIODataDoc('Outputs').set('Displacementvector', ObjectId())

c2 = WorkflowExecutionContext(db, WorkflowExecutionContext.create(db, 'Workflow02', 'borek.patzak@gmail.com'))
c2.set('StartDate', str(datetime.now()))
