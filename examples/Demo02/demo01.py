#!/usr/bin/python3
from pymongo import MongoClient
import mupifDB
from bson import ObjectId
from datetime import datetime
import demoapp01



if __name__ == "__main__":

    client = MongoClient()
    db = client.MuPIF

    workflow = demoapp01.thermal(None,None)
    workflow.setup()
    wid = 'Workflow99'
    #mupifDB.workflowmanager.insertWorkflowDefinition(db,wid,'Demo','1.0','file://localhost/home/bp/devel/mupifDB/examples/Demo02/demoapp01.py', 
    #                                                'DemoUseCase', workflow.getMetadata('inputs'), workflow.getMetadata('outputs'))
    # schedule execution
    c = mupifDB.workflowmanager.WorkflowExecutionContext.create(db, wid, 'borpat@senam.cz' )
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
