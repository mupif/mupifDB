# mupifDbRestApi.py

from flask import Flask,redirect, url_for
from flask import jsonify
from flask import request
from flask_pymongo import PyMongo
import mupifDB
import gridfs

from mongoflask import MongoJSONEncoder, ObjectIdConverter




app = Flask(__name__)
app.json_encoder = MongoJSONEncoder
app.url_map.converters['objectid'] = ObjectIdConverter

app.config['MONGO_DBNAME'] = 'MuPIF'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/MuPIF'

mongo = PyMongo(app)

@app.route('/')
def home_page():
    ans = "MuPIFDB Connected"
    ans +="<br>Basic REST API services:</br>"
    ans +="<ul>"
    ans +="<li><a href=\"http://127.0.0.1:5000/usecases\">List of UseCases</a></li>"
    ans +="<li><a href=\"http://127.0.0.1:5000/workflows\">List of Workflows<a></li>"
    ans +="<li><a href=\"http://127.0.0.1:5000/workflowexecutions\">List of Workflow Executions<a></li>"
    ans +="</ul>"

    ans +="<br>Advanced REST API services:</br>"
    ans +="<ul>"
    ans +="<li>http://127.0.0.1:5000/usecase/ID Deatils of usecase with given ID</li>"
    ans +="<li>http://127.0.0.1:5000/usecase/ID/workflows Workflows available for Usecase ID</li>"
    ans +="<li>http://127.0.0.1:5000/workflow/ID Details of workflow ID</li>"
    
    ans +="</ul>"
    

    return ans

@app.route('/usecases', methods=['GET'])
def get_usecases():
  usecases = mongo.db.UseCases
  output = []
  for s in usecases.find():
    output.append({'id' : s['_id']})
  return jsonify({'result' : output})

@app.route('/usecases/<usecase>', methods=['GET'])
def get_usecase(usecase):
  usecases = mongo.db.UseCases
  output = []
  for s in usecases.find():
    output.append({'id' : s['_id'], 'Description' : s['Description']})
  return jsonify({'result' : output})

@app.route('/usecases/<usecase>/workflows', methods=['GET'])
def get_usecase_workflows(usecase):
  workflows = mongo.db.Workflows
  output = []
  for s in workflows.find({"UseCases": usecase}):
    output.append({'id' : s['_id']})
  return jsonify({'result' : output})

@app.route('/workflows')
def get_workflows():
  workflows = mongo.db.Workflows
  output = []
  for s in workflows.find():
    output.append({'id' : s['_id'], 'Description' : s['Description']})
  return jsonify({'result' : output})

@app.route('/workflows/<id>')
def get_workflow(id):
  workflows = mongo.db.Workflows
  output = []
  for s in workflows.find({"_id": id}):
    output.append({'id' : s['_id'], 'Description' : s['Description'], 'UseCases': s['UseCases'], 'IOCard': s['IOCard']})
  return jsonify({'result' : output})

@app.route('/workflowexecutions')
def get_workflowexecutions():
  we = mongo.db.WorkflowExecutions
  output = []
  for s in we.find():
    output.append({'id' : str(s['_id']), 'StartDate' : s['StartDate'], 'EndDate': s['EndDate'], 'WorkflowID': s['WorkflowID']})
  return jsonify({'result' : output})

@app.route('/workflowexecutions/<ObjectId:id>')
def get_workflowexecution(id):
  we = mongo.db.WorkflowExecutions
  output = []
  print (str(id))
  for s in we.find({"_id": id}):
    log = None
    if s['ExecutionLog'] is not None:
      log = "http://localhost:5000/gridfs/%s"%s['ExecutionLog']
      print (log)
    
    output.append({'Start Date' : str(s['StartDate']), 'End Date': str(s['EndDate']), 'WorkflowID': str(s['WorkflowID']), 'Status': s['Status'], 'Inputs': s['Inputs'], 'Outputs':s['Outputs'], 'ExecutionLog': log})
    return jsonify({'result' : output})


@app.route('/workflowexecutions/<ObjectId:id>/inputs')
def get_workflowexecutioninputs(id):
  we = mongo.db.WorkflowExecutions
  wi = we.find_one({"_id":id})
  wid = wi['Inputs']
  output = []
  if (wid is not None):
    inp = mongo.db.IOData.find_one({'_id': wi['Inputs']})
    print (inp)
    output = inp['DataSet']
    
  return jsonify({'result' : output})

@app.route('/workflowexecutions/<ObjectId:id>/outputs')
def get_workflowexecutionoutputs(id):
  we = mongo.db.WorkflowExecutions
  wi = we.find_one({"_id":id})
  wid = wi['Outputs']
  output = []

  if (wid is not None):
    inp = mongo.db.IOData.find_one({'_id': wi['Outputs']})
    print (inp)
    output=inp['DataSet']

  return jsonify({'result' : output})

@app.route('/executeworkflow/<ObjectId:id>')
def executeworkflow (id):
    c = mupifDB.workflowmanager.WorkflowExecutionContext(mongo.db, id)
    c.execute()
    return redirect(url_for("get_workflowexecution", id=id))

@app.route("/uploads/<path:filename>")
def get_upload(filename):
    return mongo.send_file(filename)

@app.route('/gridfs/<ObjectId:id>')
def download (id):
  fs = gridfs.GridFSBucket(mongo.db)
  return fs.open_download_stream(id).read()

@app.route("/uploads/<path:filename>", methods=["POST"])
def save_upload(filename):
    mongo.save_file(filename, request.files["file"])
    #return "Uploaded"
    return redirect(url_for("get_upload", filename=filename))



if __name__ == '__main__':
    app.run(debug=True)
