import pymongo
from bson import ObjectId


client = pymongo.MongoClient()
db = client.MuPIF


def cleanAllWorkflowExecutions (wid):
    query = {"WorkflowID": wid}
    # first loop over executions
    wer = db.WorkflowExecutions
    for s in wer.find(query):
        print (s)
        #get IO Records
        inputs = s['Inputs']
        outputs= s['Outputs']
        #delete IO records
        db.IOData.delete_one({'_id':ObjectId(inputs)})
        db.IOData.delete_one({'_id':ObjectId(outputs)})
    # delete all execution records
    count = wer.delete_many(query)
    print (count.deleted_count, " records deleted")
    

if __name__ == "__main__":
    cleanAllWorkflowExecutions('Workflow99')
