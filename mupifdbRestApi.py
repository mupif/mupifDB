# mupifDbRestApi.py

from flask import Flask,redirect, url_for
from flask import jsonify
from flask import request
from flask_pymongo import PyMongo
import mupifDB
import gridfs
import re
import os,psutil

from mongoflask import MongoJSONEncoder, ObjectIdConverter
import pygal


app = Flask(__name__)
app.json_encoder = MongoJSONEncoder
app.url_map.converters['objectid'] = ObjectIdConverter

app.config['MONGO_DBNAME'] = 'MuPIF'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/MuPIF'
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

nameObjectIDpair = re.compile('([\w ]+){(\d+)}')
nameObjectIDpairNone = re.compile('([\w ]+){None}')

mongo = PyMongo(app)

# Registering an Error Handler
@app.errorhandler(mupifDB.error.InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@app.errorhandler(KeyError)
def handle_error(e):
    response = jsonify({"error":{
        "code": 400,
        "type": "KeyError",
        "message": str(e),
    }})
    #response.content_type = "application/json"
    return response


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
    <tr><td><a href="/status">/status</a></td><td>MupifDB status</td></tr>
    <tr><td><a href="/usecases">/usecases</a></td><td>List of UseCases</td></tr>
    <tr><td><a href="/workflows">/workflows</a></td><td>List of Workflows</td></tr>
    <tr><td><a href="/workflowexecutions">/workflowexecutions</a></td><td>List of Workflow Executions</td></tr>
    <tr><td><a href="/schedulerStats/total2.svg">/schedulerStats/total2.svg</a></td><td>Scheduler statistics (svg chart)</td></tr>
    <tr><td><a href="/schedulerStats/weekly.svg">/schedulerStats/weekly.svg</a></td><td>Scheduler weekly statistics (svg chart)</td></tr>
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
    <li> Schedule execution of workflow: <a href="/workflowexecutions/init/Workflow98">/workflowexecutions/init/Workflow98</a>, returns workflow execution id (WEID)</li>
    <li>For given execution id (WEID):<ul>
            <li> Get status of workflow execution: <a href="/workflowexecutions/WEID">/workflowexecutions/WEID</a></li>
            <li> Get workflow execution inputs: <a href="/workflowexecutions/WEID/inputs">/workflowexecutions/WEID/inputs</a></li>
            <li> Setting workflow execution inputs (inputs can be set only for execution with status 'Created'): <a href="/workflowexecutions/WEID/set?YoungModulus=30.e9&Dimension{0}=10.&Dimension{1}=0.1&Dimension{2}=0.3&Force=10e3">/workflowexecutions/WEID/set?YoungModulus=30.e9&Dimension{0}=10.&Dimension{1}=0.1&Dimension{2}=0.3&Force=10e3</a></li>
            <li> Executing workflow (workflow can be executed only with status 'Created'): <a href="/executeworkflow/WEID">/executeworkflow/WEID</a></li>
            <li> Get status of workflow execution: <a href="/workflowexecutions/WEID">/workflowexecutions/WEID</a></li>
            <li> Get Workflow execution outputs: <a href="/workflowexecutions/WEID/outputs">/workflowexecutions/WEID/outputs</a></li>
            </ul></li>
    </ul>

    """

    return ans
    
@app.route('/usecases', methods=['GET'])
def get_usecases():
  usecases = mongo.db.UseCases
  output = []
  for s in usecases.find():
    output.append({'id' : s['_id'], 'Description' : s['Description']})
  return jsonify({'result' : output})

@app.route('/usecases/<usecase>', methods=['GET'])
def get_usecase(usecase):
  usecases = mongo.db.UseCases
  output = []
  for s in usecases.find({"_id": usecase}):
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
  #for s in we.find({"_id": id}):
  #  log = None
  #  if s['ExecutionLog'] is not None:
  #    log = "http://localhost:5000/gridfs/%s"%s['ExecutionLog']
  #    print (log)
    
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
      mnone = nameObjectIDpairNone.match (key)
      if (m):
        name = m.group(1)
        objid = m.group(2)
        print(f'Setting {name}({objid}):{value}')
        inp.set(name, value, obj_id=int(objid))
      elif (mnone):
        name = mnone.group(1)
        print(f'Setting {name}:{value}')
        inp.set(name, value)
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
    user = request.headers.get('From')
    remoteAddr = request.remote_addr
    print ("Execution request by %s from %s"%(user, remoteAddr))
        
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

@app.route("/status")
def status():
    output=[]
    mupifDBStatus='OK'
    schedulerStatus='OK'

    pidfile='mupifDB_scheduler_pidfile'
    if not os.path.exists(pidfile):
        schedulerStatus= 'Failed'
    else:
        with open(pidfile, "r") as f:
          try:
            pid = int(f.read())
          except (OSError, ValueError):
            schedulerStatus = 'Failed'

        if not psutil.pid_exists(pid):
          schedulerStatus ='Failed'

    # get some scheduler stats
    stat=mupifDB.schedulerstat.getGlobalStat(mongo.db)          
    output.append({'mupifDBStatus':mupifDBStatus, 'schedulerStatus':schedulerStatus, 'schedulerStats':stat})
    return jsonify({'result' : output})  
    
@app.route("/schedulerStats/total.svg")
def schedulerStat():
    # get some scheduler stats
    stat=mupifDB.schedulerstat.getGlobalStat(mongo.db)
    #render some graphics
    gauge = pygal.SolidGauge(
        width=800, height=300, explicit_size=True,
        half_pie=True, inner_radius=0.70,
        style=pygal.style.styles['default'](value_font_size=10))
    #gauge = pygal.SolidGauge(inner_radius=0.70)
    percent_formatter = lambda x: '{:.10g}%'.format(x)
    value_formatter = lambda x: '{:.10g}'.format(x)
    gauge.value_formatter = percent_formatter

    gauge.add('Finished executions', [{'value': stat['finishedExecutions'], 'max_value': stat['totalExecutions']}], formatter=value_formatter)
    gauge.add('Failed executions', [{'value': round(100*stat['failedExecutions']/stat['totalExecutions']), 'max_value': 100}])
    gauge.add('Created executions', [{'value': round(100*stat['createdExecutions']/stat['totalExecutions']), 'max_value': 100}])
    gauge.add('Pending executions', [{'value': round(100*stat['pendingExecutions']/stat['totalExecutions']), 'max_value': 100}])
    gauge.add('Scheduled executions', [{'value': round(100*stat['scheduledExecutions']/stat['totalExecutions']), 'max_value': 100}])
    gauge.add('Running executions', [{'value': round(100*stat['runningExecutions']/stat['totalExecutions']), 'max_value': 100}])

    return gauge.render_response()

@app.route("/schedulerStats/total2.svg")
def schedulerStat2():
    # get some scheduler stats
    stat=mupifDB.schedulerstat.getGlobalStat(mongo.db)
    #render some graphics
    gauge = pygal.Pie(width=800, height=300, inner_radius=.4, print_values=True)
    gauge.add('Finished executions', stat['finishedExecutions'])
    gauge.add('Failed executions', stat['failedExecutions'])
    gauge.add('Created executions', stat['createdExecutions'])
    gauge.add('Pending executions', stat['pendingExecutions'])
    gauge.add('Scheduled executions', stat['scheduledExecutions'])
    gauge.add('Running executions', stat['runningExecutions'])

    return gauge.render_response()
 


@app.route("/schedulerStats/weekly.svg")
def schedulerStatWeekly():
    ws = mupifDB.schedulerstat.getWeeklyExecutionStat(mongo.db)
    line_chart = pygal.Bar(width=800, height=300, explicit_size=True)
    line_chart.title = 'MupifDB Scheduler Usage Weekly Statistics'
    #line_chart.x_labels = map(str, range(2002, 2013))
    for label, data in ws.items():
        line_chart.add(label, data)
    return line_chart.render_response()    


if __name__ == '__main__':
    app.run(debug=True)
