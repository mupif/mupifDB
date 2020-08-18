# cleans the IOData records not linked to any workflow execution

import mupifDB
from pymongo import MongoClient
from bson import ObjectId

def cleanUnusedIODataRecords(db):
    linkedIOData = set()
    cleaned = 0
    wenum = 0
    # loop over executions to collect linked IOData IDs
    for wed in db.WorkflowExecutions.find():
        linkedIOData.add(wed['Inputs'])
        linkedIOData.add(wed['Outputs'])
        wenum += 1
    # now process IOData and delete not linked ones
    for iod in db.IOData.find():
        if (iod['_id'] not in linkedIOData):
            print ("Deleting %s"%(iod['_id']))
            db.IOData.delete_one({'_id': iod['_id']})
            cleaned +=1
    IODataNum = db.IOData.count_documents({})
    print("WorkflowExecutions: %s, IORecords: %s, Cleaned %d unlinked records"%(wenum, IODataNum, cleaned))
    



if __name__ == "__main__":
    client = MongoClient()
    db = client.MuPIF

    cleanUnusedIODataRecords(db)
    

