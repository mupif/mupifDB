import mupifDB
from pymongo import MongoClient
from bson import ObjectId
import argparse

if __name__ == "__main__":
    client = MongoClient()
    db = client.MuPIF

    parser = argparse.ArgumentParser()
    parser.add_argument('-eid', '--executionID', required=True, dest="id")
    parser.add_argument('-s', '--state', required=True, dest="state")
    args = parser.parse_args()
    weid = ObjectId(args.id)
    state = args.state

    doc = db.WorkflowExecutions.find_one({'_id': weid})
    if (doc):
        db.WorkflowExecutions.update_one({'_id': weid}, {'$set': {'Status': state, 'StartDate':None, 'EndDate':None, 'ScheduledDate':None}})
        print ('WEID: %s state set to \'%s\''%(weid, state))
    else:
        print('WorkflowExecution %s record not found'%(weid))
    

