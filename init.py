import pymongo


# run this only once to initialize MuPIF DB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.MuPIF
if (not db):
    usecases = db["UseCases"]
    usecases.insert_one({"_id":"DemoUseCase", "Description":"Demo UseCase"})
    #workflows= db["Workflows"]
    #workflowexecutions = db["WorkflowExecutions"]
    #iodata = db["IOData"]

    #force creation of empty collections
    db.create_collection("Workflows")
    db.create_collection("WorkflowsHistory")
    db.create_collection("WorkflowExecutions")
    db.create_collection("IOData")
    print ("MuPIF DB init completed")
else:
    print ("MuPIF DB already exists, exiting")


