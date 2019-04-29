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
  return "MuPIFDB REST API</br>Database Connected</br></br>Follow <a href=\"http://127.0.0.1:5000/help\">http://127.0.0.1:5000/help</a> for API documentation"


@app.route('/help')
def help():
    ans = """
    <style>
    table, th, td {border: 1px solid black; border-collapse: collapse;}
    th, td { padding: 5px; text-align: left; }
    </style>

    <br>Basic MuPIFDB REST API services:</br>
    <table>
    <tr><th>Service</th><th>Description</th></tr>
    <tr><td>/usecases</td><td><List of UseCases</td></tr>
    <tr><td>/workflows</td><td>List of Workflows</td></tr>
    <tr><td>/workflowexecutions</td><td>List of Workflow Executions</td></tr>
    </table>

    <br>Advanced REST API services:</br>
    <table>
    <tr>
    <th>Service</th><th>Description</th>
    </tr>
    <tr><td>/usecase/ID</td><td> Deatils of usecase with given ID</td></tr>
    <tr><td>/usecase/ID/workflows</td><td> Workflows available for Usecase ID</td></tr>
    <tr><td>/workflows/ID</td><td> Details of workflow ID</td></tr>
    <tr><td>/workflowexecutions/ID</td><td>Show execution ID status</td></tr>
    <tr><td>/workflowexecutions/ID/inputs</td><td>Show inputs for execution ID </td></tr>
    <tr><td>/workflowexecutions/ID/outputs</td><td>Show outputs for execution ID </td></tr>
    <tr><td>/executeworkflow/ID</td><td>Execute workflow ID </td></tr>
    <tr><td>/uploads/filenamepath</td><td>Uploads file where filenamepath is file URL into gridfs</td></tr>
    <tr><td>/uploads/filenamepath", methods=["POST"]</td><td></td></tr>
    <tr><td>/gridfs/ID</td><td>Show stored file with given ID</td></tr>
    </table>
    """

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
