#!/usr/bin/python3
from pymongo import MongoClient
import mupifDB
from bson import ObjectId
from datetime import datetime
import mupif
import mupif.Physics.PhysicalQuantities as PQ
from mupifDB import workflowdemo



if __name__ == "__main__":

    client = MongoClient()
    db = client.MuPIF

    workflow = workflowdemo.workflowdemo()
    wid = 'Workflow99'

    id = db.Workflows.find_one({"_id":wid})
    if (id is None):
        id = mupifDB.workflowmanager.insertWorkflowDefinition(db,wid,'thermal','1.0','file://localhost/home/bp/devel/mupifDB/workflowdemo.py', 
                                                             'DemoUseCase', workflow.getMetadata('Inputs'), workflow.getMetadata('Outputs'))
    # schedule execution
    c = mupifDB.workflowmanager.WorkflowExecutionContext.create(db, wid, 'borpat@senam.cz')
    #set inputs in DB
    # consider inputs optionally regerenced by ID (PropertyID.PID______)
    #
    inputs = c.getIODataDoc('Inputs')
    inputs.set('Effective conductivity', 0.5, obj_id=None)
    inputs.set('Dimension', 10.0, obj_id=0) 
    inputs.set('Dimension', 2.2, obj_id=1) 

    inputs.set('External temperature', 20.0, obj_id=0) 
    inputs.set('External temperature', 2.0, obj_id=2) 
    inputs.set('Convention coefficient', 0.5, obj_id=0) 
    inputs.set('Convention coefficient', 0.5, obj_id=2) 

    #execute (all mappings of inputs from DB to workflow happening there)
    c.execute()
