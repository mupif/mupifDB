import pymongo
import sys
port=(sys.argv[1] if len(sys.argv)>1 else '27017')
# run this only once to initialize MuPIF DB
client = pymongo.MongoClient(f"mongodb://localhost:{port}/")
db = client.MuPIF
if db is not None:
    usecases = db["UseCases"]
    usecases.insert_one({"_id": "DemoUseCase", "Description": "Demo UseCase"})
    Stat = db["Stat"]
    Stat.insert_one({"scheduler": {"load": 0, "processedTasks": 0, "runningTasks": 0, "scheduledTasks": 0}})

    # force creation of empty collections
    db.create_collection("Workflows")
    db.create_collection("WorkflowsHistory")
    db.create_collection("WorkflowExecutions")
    db.create_collection("IOData")
    print("MuPIF DB init completed")
else:
    print("MuPIF DB creation failed")
