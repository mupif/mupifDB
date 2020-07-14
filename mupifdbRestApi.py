# mupifDbRestApi.py

from flask import Flask,redirect, url_for
from flask import jsonify
from flask import request
from flask_pymongo import PyMongo
import mupifDB
import gridfs
import re

from mongoflask import MongoJSONEncoder, ObjectIdConverter




app = Flask(__name__)
app.json_encoder = MongoJSONEncoder
app.url_map.converters['objectid'] = ObjectIdConverter

app.config['MONGO_DBNAME'] = 'MuPIF'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/MuPIF'

nameObjectIDpair = re.compile('([\w ]+){(\d+)}')

mongo = PyMongo(app)

@app.route('/')
def home_page():
  return "MuPIFDB: MuPIF Database and Workflow Manager with REST API</br>Database Connected</br></br>Follow <a href=\"http://127.0.0.1:5000/help\">http://127.0.0.1:5000/help</a> for API documentation"


@app.route('/help')
def help():
    ans = """
    <style>
    table, th, td {border: 1px solid black; border-collapse: collapse;}
    th, td { padding: 5px; text-align: left; }
    </style>
    <h2> MuPIF_DB: database and workflow manager solution for MuPIF with REST API</h2>
    <br>Basic MuPIFDB REST API services:</br>
    <table>
    <tr><th>Service</th><th>Description</th></tr>
    <tr><td>/usecases</td><td>List of UseCases</td></tr>
    <tr><td>/workflows</td><td>List of Workflows</td></tr>
    <tr><td>/workflowexecutions</td><td>List of Workflow Executions</td></tr>
    </table>

    <br>Advanced REST API services:</br>
    <table>
    <tr>
    <th>Service</th><th>Description</th>
    </tr>
    <tr><td>/usecases/ID</td><td> Details of usecase with given ID</td></tr>
    <tr><td>/usecases/ID/workflows</td><td> Workflows available for Usecase ID</td></tr>
    <tr><td>/workflows/ID</td><td> Details of workflow ID</td></tr>

    <tr><td>/workflowexecutions/init/ID</td><td>Initialize (schedules) execution of workflow with Id, returns workflowexecutionID</td></tr>
    

    <tr><td>/workflowexecutions/ID</td><td>Show execution ID status</td></tr>
    <tr><td>/workflowexecutions/ID/inputs</td><td>Show inputs for execution ID </td></tr>
    <tr><td>/workflowexecutions/ID/outputs</td><td>Show outputs for execution ID </td></tr>
    <tr><td>/workflowexecutions/ID/set?NAME=value</td><td>Sets input parameter for workflow execution ID, NAME is string in the form "Name{obj_ID}", where curly brackes are optional and are used to set object_id</td></tr>
    <tr><td>/workflowexecutions/ID/get?NAME</td><td>Gets output parameter for workflow execution ID, NAME is string in the form "Name{obj_ID}", where curly brackes are optional and are used to set object_id</td></tr>
    <tr><td>/executeworkflow/ID</td><td>Execute workflow ID. Note that scheduled workflow execution can be executed only once from "Created" state, for another executions one has to schedule new execution and set inputs.</td></tr>
    <tr><td>/uploads/filenamepath</td><td>Uploads file where filenamepath is file URL into gridfs</td></tr>
    <tr><td>/uploads/filenamepath", methods=["POST"]</td><td></td></tr>
    <tr><td>/gridfs/ID</td><td>Show stored file with given ID</td></tr>
    </table>

    <br>Demo:<br>
    <ul>
    <li> Get list of available UseCases: <a href="/usecases">/usecases</a></li>
    <li> Get list of available workflows for specific usecase: <a href="/usecases/DemoUseCase/workflows">/usecases/DemoUseCase/workflows</a></li>
    <li> Get info on specific workflow: <a href="/workflows/Workflow98">/workflows/Workflow98</a></li>
    <li> Schedule execution of workflow: <a href="/workflowexecutions/init/Workflow98">/workflowexecutions/init/Workflow98</a>, returned workflow execution ID</li>
    <li> Get status of workflow execution: <a href="/workflowexecutions/5f0d97ded1186552c4da9163">/workflowexecutions/5f0d97ded1186552c4da9163</a></li>
    <li> Get workflow execution inputs: <a href="/workflowexecutions/5f0d97ded1186552c4da9163/inputs">/workflowexecutions/5f0d97ded1186552c4da9163/inputs</a></li>
    <li> Setting workflow execution inputs: <a href="/workflowexecutions/5f0d97ded1186552c4da9163/set?YoungModulus=30.e9&Dimension{0}=10.&Dimension{1}=0.1&Dimension{2}=0.3&Force=10e3">/workflowexecutions/5f0d97ded1186552c4da9163/set?YoungModulus=30.e9&Dimension{0}=10.&Dimension{1}=0.1&Dimension{2}=0.3&Force=10e3</a></li>
    <li> Executing workflow: <a href="/executeworkflow/5f0d97ded1186552c4da9163">/executeworkflow/5f0d97ded1186552c4da9163</a></li>
    <li> Get status of workflow execution: <a href="/workflowexecutions/5f0d97ded1186552c4da9163">/workflowexecutions/5f0d97ded1186552c4da9163</a></li>
    <li> Get Workflow execution outputs: <a href="/workflowexecutions/5f0d97ded1186552c4da9163/outputs">/workflowexecutions/5f0d97ded1186552c4da9163/outputs</a></li>
    </ul>

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

@app.route('/workflowexecutions/init/<wid>')
def initWorkflowExecution(wid):
    #generate new execution record
    # schedule execution
    c = mupifDB.workflowmanager.WorkflowExecutionContext.create(mongo.db, wid, 'borpat@senam.cz' )
    return jsonify({'result': c.executionID })
    
@app.route('/workflowexecutions/<ObjectId:id>/set')
def setWorkflowExecutionParameter(id):
    c = mupifDB.workflowmanager.WorkflowExecutionContext(mongo.db, id)
    print (c)
    inp = c.getIODataDoc ('Inputs')
    print (inp)
    for key, value in request.args.items():
      
      m = nameObjectIDpair.match (key)
      if (m):
        name = m.group(1)
        objid = m.group(2)
        print(f'Setting {name}({objid}):{value}')
        inp.set(name, value, obj_id=int(objid))
      else:
        print(f'Setting {key}:{value}')
        inp.set(key, value)
    return jsonify({'result': c.executionID })

@app.route('/workflowexecutions/<ObjectId:id>/get')
def getWorkflowExecutionParameter(id):
    c = mupifDB.workflowmanager.WorkflowExecutionContext(mongo.db, id)
    orec = c.getIODataDoc ('Outputs')
    output = []
    for key, value in request.args.items():
      
      m = nameObjectIDpair.match (key)
      if (m):
        name = m.group(1)
        objid = m.group(2)
        print(f'Getting {name}({objid})')
        output.append(orec.getRec(name, obj_id=int(objid)))
      else:
        print(f'Getting {key}:{value}')
        output.append(orec.getRec(key, obj_id=None))

    return jsonify({'result' : output})
  

@app.route('/executeworkflow/<ObjectId:id>')
def executeworkflow (id):
    #print(id)
    c = mupifDB.workflowmanager.WorkflowExecutionContext(mongo.db, id)
    c.execute()
    return redirect(url_for("get_workflowexecution", id=id))
    #return (id)

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
