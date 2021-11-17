import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")

from flask import Flask, redirect, url_for, send_file, send_from_directory, flash, request, jsonify
from flask_pymongo import PyMongo
from flask_cors import CORS
import mupifDB
import gridfs
import re
import psutil
import tempfile
import io
import zipfile

from mongoflask import MongoJSONEncoder, ObjectIdConverter

# for small stat use plain matplotlib
# import matplotlib.pyplot as plt
# plt.switch_backend('agg')

app = Flask(__name__)
CORS(app)
app.json_encoder = MongoJSONEncoder
app.url_map.converters['objectid'] = ObjectIdConverter

app.config['MONGO_DBNAME'] = 'MuPIF'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/MuPIF'
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

nameObjectIDpair = re.compile('([\w ]+){(\d+)}')
nameObjectIDpairNone = re.compile('([\w ]+){None}')

mongo = PyMongo(app)

# initialize the database, this is done only once
if 'UseCases' not in mongo.db.list_collection_names():
    usecases = mongo.db["UseCases"]
    usecases.insert_one({"_id": "DemoUseCase", "Description": "Demo UseCase"})
    Stat = mongo.db["Stat"]
    Stat.insert_one({"scheduler": {"load": 0, "processedTasks": 0, "runningTasks": 0, "scheduledTasks": 0}})
    # force creation of empty collections
    mongo.db.create_collection("Workflows")
    mongo.db.create_collection("WorkflowsHistory")
    mongo.db.create_collection("WorkflowExecutions")
    mongo.db.create_collection("IOData")
    print('MuPIF DB first-time setup completed.')


# Registering an Error Handler
@app.errorhandler(mupifDB.error.InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@app.errorhandler(KeyError)
def handle_error(e):
    response = jsonify({"error": {
        "code": 400,
        "type": "KeyError",
        "message": str(e),
    }})
    # response.content_type = "application/json"
    return response


@app.route('/')
def home_page():
    return f"MuPIFDB: MuPIF Database and Workflow Manager with REST API</br>Database Connected</br></br>Follow <a href=\"{request.host_url}/help\">{request.host_url}/help</a> for API documentation"


@app.route('/help')
def printHelp():
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
    <tr><td><a href="/schedulerStats/hourly.svg">/schedulerStats/hourly.svg</a></td><td>Scheduler hourly statistics (svg chart)</td></tr>
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
    <tr><td>/workflowexecutions/ID/set?NAME=value</td><td>Sets input parameter for workflow execution ID, NAME is string in the form "Name{obj_ID}", where curly brackes are optional and are used to set object_id. The value is string with format depending on input type. If type is mupif.Property then value should be string convertible to number (integer or floating point). If type is mupif.Field then string should be convertible to python tuple used to initialize ConstantField (Example "permeability=(1.e-3, 1.e-3, 1.e-3)").</td></tr>
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


# --------------------------------------------------
# Usecases
# --------------------------------------------------

@app.route('/usecases', methods=['GET'])
def get_usecases():
    table = mongo.db.UseCases
    output = []
    for s in table.find():
        output.append({'id': s['_id'], 'Description': s['Description']})
    return jsonify({'result': output})


@app.route('/usecases/<usecase>', methods=['GET'])
def get_usecase(usecase):
    table = mongo.db.UseCases
    output = []
    for s in table.find({"_id": usecase}):
        output.append({'id': s['_id'], 'Description': s['Description']})
    return jsonify({'result': output})


@app.route('/usecases/<usecase>/workflows', methods=['GET'])
def get_usecase_workflows(usecase):
    table = mongo.db.Workflows
    output = []
    for s in table.find({"UseCases": usecase}):
        output.append({'wid': s['wid'], '_id': s['_id']})
    return jsonify({'result': output})


# --------------------------------------------------
# Workflows
# --------------------------------------------------

@app.route('/workflows')
def get_workflows():
    table = mongo.db.Workflows
    output = []
    for s in table.find():
        output.append(s)
        # output.append({'wid': s['wid'], 'Description': s['Description'], '_id': s['_id']})
    return jsonify({'result': output})


@app.route('/workflows/<wid>')
def get_workflow(wid):
    table = mongo.db.Workflows
    output = []
    for s in table.find({"wid": wid}):
        output.append(s)
        # output.append({'_id': s['_id'], 'wid': s['wid'], 'Description': s['Description'], 'UseCases': s['UseCases'], 'IOCard': s['IOCard'], 'Version': s.get('Version', 1)})
    return jsonify({'result': output})


@app.route('/workflows/<wid>/modify')
def modifyWorkflow(wid):
    for key, value in request.args.items():
        mongo.db.Workflows.update_one({'_id': wid}, {"$set": {key: value}})
    return jsonify({'result': True})


# --------------------------------------------------
# Workflows history
# --------------------------------------------------

@app.route('/workflowshistory/<wid>/<int:version>')
def get_workflowFromHistory(wid, version):
    table = mongo.db.WorkflowsHistory
    output = []
    for s in table.find({"wid": wid, "Version": version}):
        output.append({'_id': s['_id'], 'wid': s['wid'], 'Description': s['Description'], 'UseCases': s['UseCases'], 'IOCard': s['IOCard'], 'Version': s.get('Version', 1)})
    return jsonify({'result': output})


# --------------------------------------------------
# Executions
# --------------------------------------------------

@app.route('/workflowexecutions')
def get_workflowexecutions():
    table = mongo.db.WorkflowExecutions
    output = []
    for s in table.find():
        output.append(s)
        # output.append({'id': str(s['_id']), 'StartDate': s['StartDate'], 'EndDate': s['EndDate'], 'WorkflowID': s['WorkflowID'], "Status": s['Status']})
    return jsonify({'result': output})


@app.route('/workflowexecutions/<objectid:weid>')
def get_workflowexecution(weid):
    table = mongo.db.WorkflowExecutions
    output = []
    print(str(weid))
    for s in table.find({"_id": weid}):
        #  log = None
        #  if s['ExecutionLog'] is not None:
        #    log = "http://localhost:5000/gridfs/%s"%s['ExecutionLog']
        #    print(log)
        output.append(s)
        # output.append({'Start Date': str(s['StartDate']), 'End Date': str(s['EndDate']), 'WorkflowID': str(s['WorkflowID']), 'Status': s['Status'], 'Inputs': s['Inputs'], 'Outputs': s['Outputs'], 'ExecutionLog': str(s['ExecutionLog'])})
    return jsonify({'result': output})


@app.route('/workflowexecutions/Pending')
def get_workflowexecutionWithStatusPending():
    table = mongo.db.WorkflowExecutions
    output = []
    for s in table.find({"Status": "Pending"}):
        output.append(s)
        # output.append({'Start Date': str(s['StartDate']), 'End Date': str(s['EndDate']), 'WorkflowID': str(s['WorkflowID']), 'Status': s['Status'], 'Inputs': s['Inputs'], 'Outputs': s['Outputs'], 'ExecutionLog': str(s['ExecutionLog'])})
    return jsonify({'result': output})


@app.route('/workflowexecutions/Scheduled')
def get_workflowexecutionWithStatusScheduled():
    table = mongo.db.WorkflowExecutions
    output = []
    for s in table.find({"Status": "Scheduled"}):
        output.append(s)
        # output.append({'Start Date': str(s['StartDate']), 'End Date': str(s['EndDate']), 'WorkflowID': str(s['WorkflowID']), 'Status': s['Status'], 'Inputs': s['Inputs'], 'Outputs': s['Outputs'], 'ExecutionLog': str(s['ExecutionLog'])})
    return jsonify({'result': output})


@app.route('/workflowexecutions/Created')
def get_workflowexecutionWithStatusCreated():
    table = mongo.db.WorkflowExecutions
    output = []
    for s in table.find({"Status": "Created"}):
        output.append(s)
        # output.append({'Start Date': str(s['StartDate']), 'End Date': str(s['EndDate']), 'WorkflowID': str(s['WorkflowID']), 'Status': s['Status'], 'Inputs': s['Inputs'], 'Outputs': s['Outputs'], 'ExecutionLog': str(s['ExecutionLog'])})
    return jsonify({'result': output})


@app.route('/workflowexecutions/Running')
def get_workflowexecutionWithStatusRunning():
    table = mongo.db.WorkflowExecutions
    output = []
    for s in table.find({"Status": "Running"}):
        output.append(s)
        # output.append({'Start Date': str(s['StartDate']), 'End Date': str(s['EndDate']), 'WorkflowID': str(s['WorkflowID']), 'Status': s['Status'], 'Inputs': s['Inputs'], 'Outputs': s['Outputs'], 'ExecutionLog': str(s['ExecutionLog'])})
    return jsonify({'result': output})


@app.route('/workflowexecutions/<objectid:weid>/inputs')
def get_workflowexecutioninputs(weid):
    table = mongo.db.WorkflowExecutions
    wi = table.find_one({"_id": weid})
    w_id = wi['Inputs']
    output = []
    if w_id is not None:
        inp = mongo.db.IOData.find_one({'_id': wi['Inputs']})
        # print(inp)
        output = inp['DataSet']
    return jsonify({'result': output})


@app.route('/workflowexecutions/<objectid:weid>/outputs')
def get_workflowexecutionoutputs(weid):
    table = mongo.db.WorkflowExecutions
    wi = table.find_one({"_id": weid})
    w_id = wi['Outputs']
    output = []

    if w_id is not None:
        inp = mongo.db.IOData.find_one({'_id': wi['Outputs']})
        # print(inp)
        output = inp['DataSet']

    return jsonify({'result': output})


@app.route('/workflowexecutions/init/<wid>')
def initWorkflowExecution(wid):
    # generate new execution record
    # schedule execution
    c = mupifDB.workflowmanager.WorkflowExecutionContext.create(mongo.db, wid, 'sulcstanda@seznam.cz')
    return jsonify({'result': c.executionID})


@app.route('/workflowexecutions/<objectid:weid>/modify')
def modifyWorkflowExecution(weid):
    for key, value in request.args.items():
        mongo.db.WorkflowExecutions.update_one({'_id': weid}, {"$set": {key: value}})
    return jsonify({'result': True})


@app.route('/workflowexecutions/<objectid:weid>/set')
def setWorkflowExecutionParameter(weid):
    c = mupifDB.workflowmanager.WorkflowExecutionContext(mongo.db, weid)
    print(c)
    inp = c.getIODataDoc('Inputs')
    # print(inp)
    for key, value in request.args.items():
        m = nameObjectIDpair.match(key)
        mnone = nameObjectIDpairNone.match(key)
        if m:
            name = m.group(1)
            objid = m.group(2)
            print(f'Setting {name}({objid}):{value}')
            inp.set(name, value, obj_id=int(objid))
        elif mnone:
            name = mnone.group(1)
            print(f'Setting {name}:{value}')
            inp.set(name, value)
        else:
            print(f'Setting {key}:{value}')
            inp.set(key, value)
    return jsonify({'result': c.executionID})


@app.route('/workflowexecutions/<objectid:weid>/get')
def getWorkflowExecutionParameter(weid):
    c = mupifDB.workflowmanager.WorkflowExecutionContext(mongo.db, weid)
    orec = c.getIODataDoc('Outputs')
    output = []
    for key, value in request.args.items():
        m = nameObjectIDpair.match(key)
        if m:
            name = m.group(1)
            objid = m.group(2)
            print(f'Getting {name}({objid})')
            output.append(orec.getRec(name, obj_id=int(objid)))
        else:
            print(f'Getting {key}:{value}')
            output.append(orec.getRec(key, obj_id=None))
    return jsonify({'result': output})


@app.route('/executeworkflow/<objectid:weid>')
def executeworkflow(weid):
    # print(id)
    user = request.headers.get('From')
    remoteAddr = request.remote_addr
    print("Execution request by %s from %s" % (user, remoteAddr))

    c = mupifDB.workflowmanager.WorkflowExecutionContext(mongo.db, weid)
    c.execute(mongo.db)
    return redirect(url_for("get_workflowexecution", id=weid))
    # return (id)


# --------------------------------------------------
# Files
# --------------------------------------------------

def getFile(fid):
    fs = gridfs.GridFS(mongo.db)
    with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
        import bson
        import io
        foundfile = fs.get(bson.objectid.ObjectId(fid))
        wfile = io.BytesIO(foundfile.read())
        fn = foundfile.filename
        fullpath = tempDir + '/' + fn
        with open(fullpath, "wb") as f:
            f.write(wfile.read())
            f.close()
            return send_from_directory(directory=tempDir, path=fn)


def getFilename(fid):
    fs = gridfs.GridFS(mongo.db)
    import bson
    foundfile = fs.get(bson.objectid.ObjectId(fid))
    fn = foundfile.filename
    return jsonify({'result': fn})


def allowed_file(filename):
    ALLOWED_EXTENSIONS = {'py', 'zip'}
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route("/upload_and_zip", methods=['GET', 'POST'])
def uploadFileAndZip():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'myfile' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['myfile']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            mf = io.BytesIO()

            with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
                myfile = open(tempDir + "/" + file.filename, mode="wb")
                myfile.write(file.read())
                myfile.close()

                fs = gridfs.GridFS(mongo.db)
                with zipfile.ZipFile(mf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.write(tempDir + "/" + file.filename, arcname=file.filename)
                    zf.close()

                    sourceID = fs.put(mf.getvalue(), filename="workflow.zip")
                    return jsonify({'result': sourceID})

    return jsonify({'result': None})


@app.route("/upload", methods=['GET', 'POST'])
def uploadFile():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'myfile' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['myfile']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            mf = io.BytesIO()

            with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
                myfile = open(tempDir + "/" + file.filename, mode="wb")
                myfile.write(file.read())
                myfile.close()

                fs = gridfs.GridFS(mongo.db)
                sourceID = fs.put(mf.getvalue(), filename=file.filename)
                return jsonify({'result': sourceID})

    return jsonify({'result': None})


# --------------------------------------------------
# Stat
# --------------------------------------------------

@app.route("/status")
def status():
    output = []
    mupifDBStatus = 'OK'
    schedulerStatus = 'OK'

    pidfile = 'mupifDB_scheduler_pidfile'
    if not os.path.exists(pidfile):
        schedulerStatus = 'Failed'
    else:
        with open(pidfile, "r") as f:
            try:
                pid = int(f.read())
            except (OSError, ValueError):
                schedulerStatus = 'Failed'

        if not psutil.pid_exists(pid):
            schedulerStatus = 'Failed'

    # get some scheduler stats
    stat = mupifDB.schedulerstat.getGlobalStat(mongo.db)
    schedulerstat = mongo.db.Stat.find_one()['scheduler']
    output.append({'mupifDBStatus': mupifDBStatus, 'schedulerStatus': schedulerStatus, 'totalStat': stat, 'schedulerStat': schedulerstat})
    return jsonify({'result': output})


@app.route('/stat')
def get_statScheduler():
    table = mongo.db.Stat
    output = {}
    for s in table.find():
        keys = ["runningTasks", "scheduledTasks", "load", "processedTasks"]
        for k in keys:
            if k in s["scheduler"]:
                output[k] = s["scheduler"][k]
        break
    return jsonify({'result': output})


@app.route('/stat/set')
def set_statScheduler():
    for key, value in request.args.items():
        print(key, value)
        if key in ["scheduler.runningTasks", "scheduler.scheduledTasks", "scheduler.load", "scheduler.processedTasks"]:
            mongo.db.Stat.update_one({}, {"$set": {key: value}})
    return jsonify({'result': True})


@app.route("/schedulerStats/weekly.svg")
def schedulerStatWeekly():
    return send_file("static/images/scheduler_weekly_stat.svg", cache_timeout=60)


@app.route("/schedulerStats/hourly.svg")
def schedulerStatHourly():
    return send_file("static/images/scheduler_hourly_stat.svg", cache_timeout=60)


@app.route("/schedulerStats/loadsmall.svg")
def schedulerStatSmall():
    return send_file('static/images/scheduler_hourly_stat_small.svg', cache_timeout=60)


@app.route("/schedulerStats/loadsmall.png")
def schedulerStatSmallPng():
    return send_file('static/images/scheduler_hourly_stat_small.png', cache_timeout=60)


# --------------------------------------------------
# Stat
# --------------------------------------------------

@app.route("/main", methods=['GET', 'POST'])
def main():
    args = {}
    for key, value in request.args.items():
        args[key] = value
    print(args)

    if "action" in args:
        action = str(args["action"])
        print("Request for action %s" % action)

        if action == "get_file":
            if "file_id" in args:
                return getFile(args["file_id"])
            else:
                return jsonify({'error': "Param 'file_id' not specified."})

        if action == "get_filename":
            if "file_id" in args:
                return getFilename(args["file_id"])
            else:
                return jsonify({'error': "Param 'file_id' not specified."})

        print("action:")
        print(args["action"])

        return jsonify({'error': "Action '%s' not found." % action})

    return jsonify({'error': "Param 'action' not specified."})


if __name__ == '__main__':
    app.run(debug=True)
