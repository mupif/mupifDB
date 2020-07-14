import pymongo


# run this only once to initialize MuPIF DB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.MuPIF
usecases = db["UseCases"]
usecases.insert_one({"_id":"DemoUseCase", "Description":"Demonstration use case})
workflows= db["Workflows"]

workflowexecutions = db["WorkflowExecutions"]
iodata = db["IOData"]


