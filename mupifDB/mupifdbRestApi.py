import gridfs
import re
import psutil
import tempfile
import io
import zipfile
import bson
import json
from flask import Flask, redirect, url_for, send_file, send_from_directory, flash, request, jsonify
from flask_pymongo import PyMongo
from flask_cors import CORS
from pymongo import ReturnDocument
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")
import mupifDB
from mongoflask import MongoJSONEncoder, ObjectIdConverter
import table_structures


path_of_this_file = os.path.dirname(os.path.abspath(__file__))

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
    Stat = mongo.db["Stat"]
    Stat.insert_one({"scheduler": {"load": 0, "processedTasks": 0, "runningTasks": 0, "scheduledTasks": 0}})
    # force creation of empty collections
    mongo.db.create_collection("UseCases")
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
    <tr><td><a href="/main?action=get_status">/main?action=get_status</a></td><td>MupifDB status</td></tr>
    <tr><td><a href="/main?action=get_usecases">/main?action=get_usecases</a></td><td>List of UseCases</td></tr>
    <tr><td><a href="/main?action=get_workflows">/main?action=get_workflows</a></td><td>List of Workflows</td></tr>
    <tr><td><a href="/main?action=get_executions">/main?action=get_executions</a></td><td>List of Workflow Executions</td></tr>
    </table>

    <br>Advanced REST API services:</br>
    <table>
    <tr>
    <th>Service</th><th>Description</th>
    </tr>
    <tr><td>/main?action=get_usecase&id=ID</td><td> Details of usecase with given ID</td></tr>
    <tr><td>/main?action=get_workflows_for_usecase&usecase=ID</td><td> Workflows available for Usecase ID</td></tr>
    <tr><td>/main?action=get_workflow&wid=ID</td><td> Details of workflow ID</td></tr>
    <tr><td>/main?action=insert_new_execution&wid=ID&version=Version</td><td>Initialize a new execution of workflow with ID and Version, returns workflowexecutionID</td></tr>
    <tr><td>/main?action=get_execution&id=ID</td><td>Get record of workflow execution with given ID</td></tr>
    <tr><td>/main?action=get_execution_inputs&id=ID</td><td>Show inputs for execution ID </td></tr>
    <tr><td>/main?action=get_execution_outputs&id=ID</td><td>Show outputs for execution ID </td></tr>
    <tr><td>/main?action=set_execution_input&id=WEID&name=NAME&value=VALUE&obj_id=OBJ_ID</td><td>Sets input parameter for workflow execution with id WEID. The input is specified by its NAME and OBJ_ID. The VALUE is string with format depending on input type. If type is mupif.Property then value should be string convertible to tuple such as "(5.0,)".</td></tr>
    <!--<tr><td style="color:red;">/main?action=get_execution_output&id=WEID&name=NAME&value=VALUE&obj_id=OBJ_ID</td><td>Gets output parameter for workflow execution with id WEID, NAME is string in the form "Name{obj_ID}", where curly brackes are optional and are used to set object_id</td></tr>-->
    <tr><td>/main?action=schedule_execution&id=WEID</td><td>Schedule workflow execution specified by id WEID. Note that workflow execution can be scheduled only once from "Created" state to state "Pending", for another computation one has to create a new execution and set inputs.</td></tr>
    <tr><td style="color:red;">/uploads/filenamepath</td><td>Uploads file where filenamepath is file URL into gridfs</td></tr>
    <tr><td style="color:red;">/uploads/filenamepath", methods=["POST"]</td><td></td></tr>
    <tr><td style="color:red;">/gridfs/ID</td><td>Show stored file with given ID</td></tr>
    </table>

    <br><b><u>Demo - uses an existing workflow record in the database</u></b><br>
    <ul>
    <li> Get list of available workflows for specific usecase: <a href="/main?action=get_workflows_for_usecase&usecase=Demo_UseCase">/main?action=get_workflows_for_usecase&usecase=Demo_UseCase</a></li>
    <li> Get info on specific workflow: <a href="/main?action=get_workflow&wid=workflow_13">/main?action=get_workflow&wid=workflow_13</a></li>
    <li> Create new execution of a workflow: <a href="/main?action=insert_new_execution&wid=workflow_13&version=1">/main?action=insert_new_execution&wid=workflow_13&version=1</a>, returns workflow execution id (WEID)</li>
    <li>For given execution id (WEID):<ul>
            <li> Get workflow execution record: <a href="/main?action=get_execution&id=WEID">/main?action=get_execution&id=WEID</a></li>
            <li> Get workflow execution inputs: <a href="/main?action=get_execution_inputs&id=WEID">/main?action=get_execution_inputs&id=WEID</a></li>
            <li> Setting workflow execution input #1: <a href="/main?action=set_execution_input&id=WEID&name=Value_1&value=(7.,)&obj_id=1">/main?action=set_execution_input&id=WEID&name=Value_1&value=(7.,)&obj_id=1</a></li>
            <li> Setting workflow execution input #2: <a href="/main?action=set_execution_input&id=WEID&name=Value_2&value=(2.5,)&obj_id=2">/main?action=set_execution_input&id=WEID&name=Value_2&value=(2.5,)&obj_id=2</a></li>
            <li> (inputs can be set only for execution with status 'Created')</li>
            <li> Schedule workflow execution (it can be done only if the status is 'Created'): <a href="/main?action=schedule_execution&id=WEID">/main?action=schedule_execution&id=WEID</a></li>
            <li> Get workflow execution outputs: <a href="/main?action=get_execution_outputs&id=WEID">/main?action=get_execution_outputs&id=WEID</a></li>
            </ul></li>
    </ul>

    """

    return ans


# --------------------------------------------------
# Users
# --------------------------------------------------

def get_user_by_IP(ip):
    table = mongo.db.Users
    res = table.find_one({'IP': ip})
    return jsonify({'result': res})


# --------------------------------------------------
# Usecases
# --------------------------------------------------

def get_usecases():
    table = mongo.db.UseCases
    output = []
    for s in table.find():
        output.append(s)
    return jsonify({'result': output})


def get_usecase(ucid):
    table = mongo.db.UseCases
    output = []
    for s in table.find({"ucid": ucid}):
        output.append(s)
    return jsonify({'result': output})


def insert_usecaseRecord(ucid, description):
    table = mongo.db.UseCases
    res = table.insert_one({"ucid": ucid, "Description": description})
    return jsonify({'result': res.inserted_id})


# --------------------------------------------------
# Workflows
# --------------------------------------------------

def get_workflows():
    table = mongo.db.Workflows
    output = []
    for s in table.find():
        output.append(s)
    return jsonify({'result': output})


def get_workflows_with_usecase(usecase):
    table = mongo.db.Workflows
    output = []
    for s in table.find({"UseCase": usecase}):
        output.append(s)
    return jsonify({'result': output})


def get_workflow(wid):
    table = mongo.db.Workflows
    output = []
    for s in table.find({"wid": wid}):
        output.append(s)
    return jsonify({'result': output})


def modifyWorkflow(wid, key, value):
    mongo.db.Workflows.update_one({'wid': wid}, {"$set": {key: value}})
    return jsonify({'result': True})


def insert_workflow(data):
    table = mongo.db.Workflows
    res = table.insert_one(data)
    return jsonify({'result': res.inserted_id})


def update_workflow(data):
    table = mongo.db.Workflows
    res = table.find_one_and_update({'wid': data['wid']}, {'$set': data}, return_document=ReturnDocument.AFTER)
    return jsonify({'result': res['_id']})


# --------------------------------------------------
# Workflows history
# --------------------------------------------------

def get_workflowFromHistory(wid, version):
    table = mongo.db.WorkflowsHistory
    output = []
    for s in table.find({"wid": wid, "Version": int(version)}):
        # output.append(s)
        output.append({'_id': s['_id'], 'wid': s['wid'], 'Description': s['Description'], 'UseCase': s['UseCase'], 'IOCard': s['IOCard'], 'Version': s.get('Version', 1)})
    return jsonify({'result': output})


def insert_workflow_history(data):
    table = mongo.db.WorkflowsHistory
    res = table.insert_one(data)
    return jsonify({'result': res.inserted_id})


# --------------------------------------------------
# Executions
# --------------------------------------------------

def get_workflowexecutions(we_status=None, workflow_id=None, workflow_version=None, label=None, num_limit=None):
    filter_dict = {}
    if we_status in ["Created", "Pending", "Scheduled", "Running", "Finished", "Failed"]:
        filter_dict["Status"] = we_status
    if workflow_id is not None and workflow_id != '':
        filter_dict["WorkflowID"] = workflow_id
    if workflow_version is not None:
        filter_dict["WorkflowVersion"] = int(workflow_version)
    if label is not None and label != '':
        filter_dict["label"] = label
    if num_limit is not None:
        num_limit = int(num_limit)
    else:
        num_limit = 999999

    table = mongo.db.WorkflowExecutions
    output = []
    for s in table.find(filter_dict).limit(num_limit):
        output.append(table_structures.extendRecord(s, table_structures.tableExecution))
    return jsonify({'result': output})


def get_workflowexecution(weid):
    table = mongo.db.WorkflowExecutions
    output = []
    print(str(weid))
    for s in table.find({"_id": bson.objectid.ObjectId(weid)}):
        output.append(table_structures.extendRecord(s, table_structures.tableExecution))
    return jsonify({'result': output})


def get_workflowexecutionInputs(weid):
    table = mongo.db.WorkflowExecutions
    wi = table.find_one({"_id": bson.objectid.ObjectId(weid)})
    output = []
    if wi['Inputs'] is not None:
        inp = mongo.db.IOData.find_one({'_id': bson.objectid.ObjectId(wi['Inputs'])})
        output = inp['DataSet']
    return jsonify({'result': output})


def get_workflowexecutionOutputs(weid):
    table = mongo.db.WorkflowExecutions
    wi = table.find_one({"_id": bson.objectid.ObjectId(weid)})
    output = []
    if wi['Outputs'] is not None:
        inp = mongo.db.IOData.find_one({'_id': bson.objectid.ObjectId(wi['Outputs'])})
        # print(inp)
        output = inp['DataSet']
    return jsonify({'result': output})


def insert_execution(wid, version, ip):
    c = mupifDB.workflowmanager.WorkflowExecutionContext.create(workflowID=wid, workflowVer=int(version), requestedBy='', ip=ip)
    return jsonify({'result': c.executionID})


def insert_executionRecord(data):
    table = mongo.db.WorkflowExecutions
    res = table.insert_one(data)
    return jsonify({'result': res.inserted_id})


def modifyWorkflowExecution(weid, key, value):
    mongo.db.WorkflowExecutions.update_one({'_id': bson.objectid.ObjectId(weid)}, {"$set": {key: value}})
    return jsonify({'result': True})


def scheduleExecution(weid):
    # print(id)
    user = request.headers.get('From')
    remoteAddr = request.remote_addr
    print("Execution request by %s from %s" % (user, remoteAddr))

    c = mupifDB.workflowmanager.WorkflowExecutionContext(weid)
    if(c.execute()):
        return jsonify({'result': "OK"})


# --------------------------------------------------
# IO Data
# --------------------------------------------------

def get_IOData(iod_id):
    table = mongo.db.IOData
    output = []
    for s in table.find({"_id": bson.objectid.ObjectId(iod_id)}):
        output.append(s)
    return jsonify({'result': output})

    # table = mongo.db.WorkflowExecutions
    # wi = table.find_one({"_id": bson.objectid.ObjectId(weid)})
    # output = []
    # if wi['Outputs'] is not None:
    #     inp = mongo.db.IOData.find_one({'_id': wi['Outputs']})
    #     # print(inp)
    #     output = inp['DataSet']
    # return jsonify({'result': output})


def insert_IODataRecord(data):
    table = mongo.db.IOData
    res = table.insert_one(data)
    return jsonify({'result': res.inserted_id})


def isIntable(val):
    try:
        val_int = int(val)
    except ValueError:
        return False
    return True


def set_execution_input(weid, name, value, obj_id):
    s = mongo.db.WorkflowExecutions.find_one({"_id": bson.objectid.ObjectId(weid)})
    execution_record = table_structures.extendRecord(s, table_structures.tableExecution)

    table = mongo.db.IOData
    res = table.update_one({'_id': bson.objectid.ObjectId(execution_record['Inputs'])}, {'$set': {"DataSet.$[r].%s" % "Value": value}}, array_filters=[{"r.Name": name, "r.ObjID": str(obj_id)}])
    if res.matched_count == 1:
        return jsonify({'result': "OK"})
    return jsonify({'error': "Value was not updated."})


def set_execution_input_link(weid, name, obj_id, link_eid, link_name, link_obj_id):
    s = mongo.db.WorkflowExecutions.find_one({"_id": bson.objectid.ObjectId(weid)})
    execution_record = table_structures.extendRecord(s, table_structures.tableExecution)

    table = mongo.db.IOData
    res = table.update_one({'_id': bson.objectid.ObjectId(execution_record['Inputs'])}, {'$set': {"DataSet.$[r].Link": {'ExecID': link_eid, 'Name': link_name, 'ObjID': link_obj_id}}}, array_filters=[{"r.Name": name, "r.ObjID": str(obj_id)}])
    if res.matched_count == 1:
        return jsonify({'result': "OK"})
    return jsonify({'error': "Link was not updated."})


def set_execution_output(weid, name, obj_id, value=None, file_id=None):
    s = mongo.db.WorkflowExecutions.find_one({"_id": bson.objectid.ObjectId(weid)})
    execution_record = table_structures.extendRecord(s, table_structures.tableExecution)
    setval = None
    setkey = "None"
    if value is not None:
        setval = value
        setkey = "Value"
    elif file_id is not None:
        setval = file_id
        setkey = "FileID"

    table = mongo.db.IOData
    res = table.update_one({'_id': bson.objectid.ObjectId(execution_record['Outputs'])}, {'$set': {"DataSet.$[r].%s" % setkey: setval}}, array_filters=[{"r.Name": name, "r.ObjID": str(obj_id)}])
    if res.matched_count == 1:
        return jsonify({'result': "OK"})
    return jsonify({'error': "Output was not updated."})


def get_execution_input(weid, name, obj_id):
    s = mongo.db.WorkflowExecutions.find_one({"_id": bson.objectid.ObjectId(weid)})
    execution_record = table_structures.extendRecord(s, table_structures.tableExecution)
    print(execution_record)
    iodata = mongo.db.IOData.find_one({"_id": bson.objectid.ObjectId(execution_record['Inputs'])})
    print(iodata)
    for dt in iodata['DataSet']:
        print('checking ' + str(dt))
        if dt['Name'] == name:
            if str(obj_id) == 'None' and str(dt['ObjID']) == 'None':
                return jsonify({'result': dt['Value']})
            if dt['ObjID'] == str(obj_id) or dt['ObjID'] == int(obj_id):
                return jsonify({'result': dt['Value']})
    return jsonify({'result': 'None'})


def get_execution_output(weid, name, obj_id):
    s = mongo.db.WorkflowExecutions.find_one({"_id": bson.objectid.ObjectId(weid)})
    execution_record = table_structures.extendRecord(s, table_structures.tableExecution)
    print(execution_record)
    iodata = mongo.db.IOData.find_one({"_id": bson.objectid.ObjectId(execution_record['Outputs'])})
    print(iodata)
    for dt in iodata['DataSet']:
        print('checking ' + str(dt))
        if dt['Name'] == name:
            if str(obj_id) == 'None' and str(dt['ObjID']) == 'None':
                return jsonify({'result': dt['Value']})
            if dt['ObjID'] == str(obj_id) or dt['ObjID'] == int(obj_id):
                return jsonify({'result': dt['Value']})
    return jsonify({'result': 'None'})


# --------------------------------------------------
# Files
# --------------------------------------------------

def getFile(fid):
    fs = gridfs.GridFS(mongo.db)
    with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
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
    foundfile = fs.get(bson.objectid.ObjectId(fid))
    fn = foundfile.filename
    return jsonify({'result': fn})


@app.route("/upload", methods=['GET', 'POST'])  # todo
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
        if file:
            mf = io.BytesIO()

            with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
                myfile = open(tempDir + "/" + file.filename, mode="wb")
                myfile.write(file.read())
                myfile.close()

                fs = gridfs.GridFS(mongo.db)
                sourceID = fs.put(open(r'%s' % tempDir + "/" + file.filename, 'rb'), filename=file.filename)
                return jsonify({'result': sourceID})

    return jsonify({'result': None})


# --------------------------------------------------
# Stat
# --------------------------------------------------

def get_status():
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
    stat = mupifDB.schedulerstat.getGlobalStat()
    schedulerstat = mongo.db.Stat.find_one()['scheduler']
    output = {'mupifDBStatus': mupifDBStatus, 'schedulerStatus': schedulerStatus, 'totalStat': stat, 'schedulerStat': schedulerstat}
    return jsonify({'result': output})


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


def set_statScheduler(key, value):
    if key in ["scheduler.runningTasks", "scheduler.scheduledTasks", "scheduler.load", "scheduler.processedTasks"]:
        mongo.db.Stat.update_one({}, {"$set": {key: int(value)}})
        return jsonify({'result': True})
    return jsonify({'error': "Given value was not saved."})


def update_statScheduler(key, value):
    if key in ["scheduler.runningTasks", "scheduler.scheduledTasks", "scheduler.load", "scheduler.processedTasks"]:
        mongo.db.Stat.update_one({}, {"$inc": {key: int(value)}})
        return jsonify({'result': True})
    return jsonify({'error': "Given value was not saved."})


# --------------------------------------------------
# MAIN
# --------------------------------------------------


def getNoneIfParamNotDefined(args, param):
    if param in args:
        return args[param]
    return None


@app.route("/main", methods=['GET', 'POST'])
def main():
    args = {}
    for key, value in request.args.items():
        args[key] = value
    print(args)

    if "action" in args:
        action = str(args["action"])
        print("Request for action %s" % action)

        # --------------------------------------------------
        # Users
        # --------------------------------------------------

        if action == "get_user_by_ip":
            if "ip" in args:
                return get_user_by_IP(args["ip"])
            else:
                return jsonify({'error': "Param 'ip' not specified."})

        # --------------------------------------------------
        # Usecases
        # --------------------------------------------------

        if action == "get_usecases":
            return get_usecases()

        if action == "get_usecase":
            if "id" in args:
                return get_usecase(args["id"])
            else:
                return jsonify({'error': "Param 'id' not specified."})

        if action == "insert_usecase":
            if "ucid" in args and "description" in args:
                return insert_usecaseRecord(args["ucid"], args["description"])
            else:
                return jsonify({'error': "Param 'ucid' or 'description' not specified."})

        # --------------------------------------------------
        # Workflows
        # --------------------------------------------------

        if action == "get_workflows":
            return get_workflows()

        if action == "get_workflow":
            if "wid" in args:
                return get_workflow(args["wid"])
            else:
                return jsonify({'error': "Param 'wid' not specified."})

        if action == "get_workflows_for_usecase":
            if "usecase" in args:
                return get_workflows_with_usecase(args["usecase"])
            else:
                return jsonify({'error': "Param 'wid' not specified."})

        if action == "modify_workflow":
            if "wid" in args and "key" in args and "value" in args:
                return modifyWorkflow(args["wid"], args["key"], args["value"])
            else:
                return jsonify({'error': "Param 'wid' or 'key' or 'value' not specified."})

        if action == "insert_workflow":
            return insert_workflow(json.loads(request.get_data()))

        if action == "update_workflow":
            return update_workflow(json.loads(request.get_data()))

        # --------------------------------------------------
        # Workflows history
        # --------------------------------------------------

        if action == "get_workflow_from_history":
            if "wid" in args and "version" in args:
                return get_workflowFromHistory(args["wid"], args["version"])
            else:
                return jsonify({'error': "Param 'wid' or 'version' not specified."})

        if action == "insert_workflow_history":
            return insert_workflow_history(json.loads(request.get_data()))

        # --------------------------------------------------
        # Executions
        # --------------------------------------------------

        if action == "get_executions":
            workflow_id = getNoneIfParamNotDefined(args, 'workflow_id')
            workflow_version = getNoneIfParamNotDefined(args, 'workflow_version')
            label = getNoneIfParamNotDefined(args, 'label')
            num_limit = getNoneIfParamNotDefined(args, 'num_limit')
            status = getNoneIfParamNotDefined(args, 'status')
            return get_workflowexecutions(
                we_status=status,
                workflow_id=workflow_id,
                workflow_version=workflow_version,
                label=label,
                num_limit=num_limit
            )

        if action == "get_execution":
            if "id" in args:
                return get_workflowexecution(args["id"])
            else:
                return jsonify({'error': "Param 'id' not specified."})

        if action == "modify_execution":
            if "id" in args and "key" in args and "value" in args:
                return modifyWorkflowExecution(args["id"], args["key"], args["value"])
            else:
                return jsonify({'error': "Param 'id' or 'key' or 'value' not specified."})

        if action == "schedule_execution":
            if "id" in args:
                return scheduleExecution(args["id"])
            else:
                return jsonify({'error': "Param 'id' not specified."})

        if action == "insert_new_execution":
            if "wid" in args and "version" in args and "ip" in args:
                return insert_execution(args["wid"], args["version"], args["ip"])
            else:
                return jsonify({'error': "Param 'wid' or 'version' not specified."})

        if action == "insert_execution_data":
            return insert_executionRecord(json.loads(request.get_data()))

        if action == "get_execution_inputs":
            if "id" in args:
                return get_workflowexecutionInputs(args["id"])
            else:
                return jsonify({'error': "Param 'id' not specified."})

        if action == "get_execution_outputs":
            if "id" in args:
                return get_workflowexecutionOutputs(args["id"])
            else:
                return jsonify({'error': "Param 'id' not specified."})

        # --------------------------------------------------
        # IO Data
        # --------------------------------------------------

        if action == "get_iodata":
            if "id" in args:
                return get_IOData(args["id"])
            else:
                return jsonify({'error': "Param 'id' not specified."})

        if action == "insert_iodata":
            data = request.get_data()
            data = json.loads(data)
            return insert_IODataRecord(data)

        if action == "set_execution_input_link":
            if "id" in args and "name" in args and "obj_id" in args and "link_eid" in args and "link_name" in args and "link_obj_id" in args:
                return set_execution_input_link(args["id"], args["name"], args["obj_id"], args["link_eid"], args["link_name"], args["link_obj_id"])
            else:
                return jsonify({'error': "Param 'id' or 'name' or 'value' or 'obj_id' not specified."})

        if action == "set_execution_input":
            if "id" in args and "name" in args and "value" in args and "obj_id" in args:
                return set_execution_input(args["id"], args["name"], args["value"], args["obj_id"])
            else:
                return jsonify({'error': "Param 'id' or 'name' or 'value' or 'obj_id' not specified."})

        if action == "set_execution_output":
            if "id" in args and "name" in args and "obj_id" in args and ("value" in args or "file_id" in args):
                if "value" in args:
                    return set_execution_output(weid=args["id"], name=args["name"], value=args["value"], obj_id=args["obj_id"])
                elif "file_id" in args:
                    return set_execution_output(weid=args["id"], name=args["name"], file_id=args["file_id"], obj_id=args["obj_id"])
            else:
                return jsonify({'error': "Param 'id' or 'name' or 'obj_id' or ('value' or 'file_id') not specified."})

        if action == "get_execution_input":
            if "id" in args and "name" in args and "obj_id" in args:
                return get_execution_input(args["id"], args["name"], args["obj_id"])
            else:
                return jsonify({'error': "Param 'id' or 'name' or 'value' or 'obj_id' not specified."})

        if action == "get_execution_output":
            if "id" in args and "name" in args and "obj_id" in args:
                return get_execution_output(args["id"], args["name"], args["obj_id"])
            else:
                return jsonify({'error': "Param 'id' or 'name' or 'value' or 'obj_id' not specified."})

        # --------------------------------------------------
        # Files
        # --------------------------------------------------

        if action == "get_file":
            if "id" in args:
                return getFile(args["id"])
            else:
                return jsonify({'error': "Param 'id' not specified."})

        if action == "get_filename":
            if "id" in args:
                return getFilename(args["id"])
            else:
                return jsonify({'error': "Param 'id' not specified."})

        # --------------------------------------------------
        # Stat
        # --------------------------------------------------

        if action == "get_status":
            return get_status()

        if action == "get_scheduler_stat":
            return get_statScheduler()

        if action == "set_scheduler_stat":
            if "key" in args and "value" in args:
                return set_statScheduler(args["key"], args["value"])
            else:
                return jsonify({'error': "Param 'key' or 'value' not specified."})

        if action == "update_scheduler_stat":
            if "key" in args and "value" in args:
                return update_statScheduler(args["key"], args["value"])
            else:
                return jsonify({'error': "Param 'key' or 'value' not specified."})

        # --------------------------------------------------
        # No action
        # --------------------------------------------------

        return jsonify({'error': "Action '%s' not found." % action})

    return jsonify({'error': "Param 'action' not specified."})


if __name__ == '__main__':
    app.run(debug=True)
