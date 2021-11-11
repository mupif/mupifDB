import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.MuPIF


def cleanAllWorkflowExecutions(wid):
    # first loop over executions
    workflows = db.WorkflowExecutions
    for s in workflows.find({"WorkflowID": wid}):
        # get IO Records
        inputs = s['Inputs']
        outputs = s['Outputs']
        # delete IO records
        db.IOData.delete_one({'_id': inputs})
        db.IOData.delete_one({'_id': outputs})
    # delete all execution records
    count = db.WorkflowExecutions.delete_many({'WorkflowId': wid})
    print(count.deleted_count, " records deleted")
    

if __name__ == "__main__":
    cleanAllWorkflowExecutions('Workflow99')
