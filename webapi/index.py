import copy
import importlib
import math
import zipfile
import tempfile

import mupif
from flask import Flask, render_template, Markup, escape, redirect, url_for, send_from_directory, jsonify
from flask import request
from flask_cors import CORS
import sys
import os
import inspect
import mupif as mp
import requests
from ast import literal_eval

path_of_this_file = os.path.dirname(os.path.abspath(__file__))

sys.path.append(path_of_this_file+"/..")
sys.path.append(path_of_this_file+"/.")
sys.path.append(path_of_this_file+"/../mupifDB")


from mupifDB import restApiControl
import mupifDB


app = Flask(__name__)
CORS(app, resources={r"/static/*": {"origins": "*"}})


# unless overridden by the environment, use 127.0.0.1:5000
RESTserver = os.environ.get('MUPIFDB_REST_SERVER', "http://127.0.0.1:5000/")

# RESTserver *must* have trailing /, fix if not
if not RESTserver[-1] == '/':
    RESTserver += '/'


def statusColor(val):
    if val == 'Finished':
        return 'color:green;'
    if val == 'Failed':
        return 'color:red;'
    if val == 'Running':
        return 'color:blue;'
    return 'color:gray;'

# server (that is, our URL) is obtained within request handlers as flask.request.host_url+'/'


@app.after_request
def add_header(r):
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    r.headers['Cache-Control'] = 'public, max-age=0'
    return r


def getUserIPAddress():
    return request.remote_addr


def getRightsOfCurrentUser():
    user = restApiControl.getUserByIP(getUserIPAddress())
    if user is not None:
        if 'Rights' in user:
            return int(user['Rights'])
    return 0


def getUserHasAdminRights():
    return True if getRightsOfCurrentUser() >= 10 else False


def my_render_template(*args,**kw):
    'Wraps render_template and ads a few common keywords'
    return render_template(*args,title='MuPIFDB web interface',server=request.host_url,RESTserver=RESTserver,**kw)


@app.route('/')
def homepage():
    return render_template('basic.html', title="MuPIFDB web interface", body=Markup("<h3>Welcome to MuPIFDB web interface</h3>"))


@app.route('/about')
def about():
    msg = """
        <h3>Welcome to MuPIFDB web interface</h3>
        <p><a href=\"http://www.mupif.org\">MuPIF</a> is open-source, modular, object-oriented integration platform allowing to create complex, distributed, multiphysics simulation workflows across the scales and processing chains by combining existing simulation tools. <a href=\"https://github.com/mupif/mupifDB\">MuPIFDB</a> is database layer (based on MongoDB) and workflow manager/scheduler for MuPIF with REST API.</p>
        <p>The MuPIFDB web interface allows to use MupifDB REST API from web browser in a user friendly way, allowing to inspect all the stored data and to intialize, schedule and monitor individual workflow executions.</p> 
    """
    return my_render_template('basic.html', body=Markup(msg))


@app.route('/status')
def status():
    data = restApiControl.getStatus()
    stat = data['totalStat']
    msg = "<div><div>"
    msg += "<dl><dt>MupifDBStatus:"+data['mupifDBStatus']+"</dt>"
    msg += "<dt>SchedulerStatus:"+data['schedulerStatus']+"</dt>"
    msg += "    <dd>Total executions:"+str(stat['totalExecutions'])+"</dd>"
    msg += "    <dd>Finished executions:"+str(stat['finishedExecutions'])+"</dd>"
    msg += "    <dd>Failed executions:"+str(stat['failedExecutions'])+"</dd>"
    msg += "</dl></div>"
    return my_render_template('stat.html', body=Markup(msg))


@app.route("/schedulerStats/weekly.svg")
def schedulerStatWeekly():
    # https://stackoverflow.com/questions/67591467/flask-shows-typeerror-send-from-directory-missing-1-required-positional-argum
    # https://flask.palletsprojects.com/en/2.0.x/api/#flask.send_from_directory
    return send_from_directory(directory=path_of_this_file + "/static/images", path="scheduler_weekly_stat.svg")


@app.route("/schedulerStats/hourly.svg")
def schedulerStatHourly():
    return send_from_directory(directory=path_of_this_file + "/static/images", path="scheduler_hourly_stat.svg")


@app.route('/contact')
def contact():
    msg = """
        <p>MuPIF and MuPIFDB have been developped at <a href=\"https://www.cvut.cz/en\">Czech Technical University in Prague</a> by a research team at the Department of Mechanics of the <a href=\"https://web.fsv.cvut.cz/en/\">Faculty of Civil Engineering</a>.</p>
        <p>For more information and help please contact Borek Patzak (borek.patzak@fsv.cvut.cz)</p>  
    """
    return my_render_template('basic.html', body=Markup(msg))


@app.route('/usecases')
def usecases():
    admin_rights = getUserHasAdminRights()

    data = restApiControl.getUsecaseRecords()

    html = '<h3>UseCases:</h3>'
    html += '<table>'
    html += '<tr><th>ID</th><th>Description</th><th></th><th></th></tr>'
    for uc in data:
        html += '<tr>'
        html += '<td>' + uc['ucid'] + '</td>'
        html += '<td>' + uc['Description'] + '</td>'
        html += '<td><a href="/usecases/' + uc['ucid'] + '/workflows">List of workflows</a></td>'
        html += '<td>'
        if admin_rights:
            html += '<a href="/workflow_add/' + uc['ucid'] + '">Register new workflow</a>'
        html += '</td>'
        html += '</tr>'
    html += '</table>'
    if admin_rights:
        html += '<br><a href="/usecase_add">Register new UseCase</a>'
    return my_render_template('basic.html', body=Markup(html))


@app.route('/usecase_add', methods=('GET', 'POST'))
def addUseCase():
    admin_rights = getUserHasAdminRights()

    message = ''
    usecase_id = ''
    usecase_description = ''
    new_usecase_id = None
    if request.form and admin_rights:
        usecase_id = request.form['usecase_id']
        usecase_description = request.form['usecase_description']
        if usecase_id is not None and usecase_description is not None:
            found_usecase = restApiControl.getUsecaseRecord(usecase_id)
            if found_usecase is None:
                new_usecase_id = restApiControl.insertUsecaseRecord(ucid=usecase_id, description=usecase_description)
            else:
                message += '<h5 style="color:red;">This UseCase ID already exists</h5>'

        if new_usecase_id is not None:
            html = '<h5 style="color:green;">UseCase has been registered</h5>'
            html += '<a href="/usecases">Go back to UseCases</a>'
            return my_render_template('basic.html', body=Markup(html))
        else:
            message += '<h5 style="color:red;">UseCase was not registered</h5>'
    if new_usecase_id is None:
        html = message
        html += "<h3>Add new UseCase:</h3>"
        if admin_rights:
            html += "<table>"
            html += '<tr><td>UseCase ID (string)</td><td><input type="text" name="usecase_id" value="'+str(usecase_id)+'"></td></tr>'
            html += '<tr><td>UseCase Description (string)</td><td><input type="text" name="usecase_description" value="'+str(usecase_description)+'"></td></tr>'
            html += "</table>"
            html += "<input type=\"submit\" value=\"Submit\" />"
        else:
            html += "<h5>You don't have permission to visit this page.</h5>"
        return my_render_template('form.html', form=html)


@app.route('/usecases/<ucid>/workflows')
def usecaseworkflows(ucid):
    data = restApiControl.getWorkflowRecordsWithUsecase(ucid)
    return my_render_template('workflows.html', items=data)


@app.route('/workflows')
def worflows():
    data = restApiControl.getWorkflowRecords()
    return my_render_template('workflows.html', items=data)


@app.route('/workflows/<wid>')
def workflowNoVersion(wid):
    return workflow(wid, -1)


@app.route('/workflows/<wid>/<version>')
def workflow(wid, version):
    wdata = restApiControl.getWorkflowRecordGeneral(wid=wid, version=int(version))
    html = '<table>'
    html += '<tr><td>WorkflowID:</td><td>'+str(wdata['wid'])+'</td></tr>'
    html += '<tr><td>Version:</td><td>'+str(wdata['Version'])+'</td></tr>'
    html += '<tr><td>UseCase:</td><td>'+str(wdata['UseCase'])+'</td></tr>'
    html += '<tr><td>Description:</td><td>'+str(wdata['Description'])+'</td></tr>'
    html += '</table>'

    html += '<br><a href="/workflowexecutions/init/'+str(wid)+'/'+str(wdata['Version'])+'">Initialize new execution record</a>'
    html += '<br><br>Inputs'
    html += '<table>'
    html += '<thead><th>Name</th><th>Type</th><th>TypeID</th><th>Description</th><th>Units</th><th>ObjID</th><th>Compulsory</th><th>SetAt</th></thead>'
    for item in wdata["IOCard"]["Inputs"]:
        html += '<tr>'
        html += '<td class="c1">'+str(item['Name'])+'</td>'
        html += '<td class="c2">'+str(item['Type'])+'</td>'
        html += '<td class="c3">'+str(item['TypeID'])+'</td>'
        html += '<td class="c4">'+str(item['Description'])+'</td>'
        html += '<td class="c5">'+str(item['Units'])+'</td>'
        html += '<td class="c6">'+str(item['ObjID'])+'</td>'
        html += '<td class="c7">'+str(item['Compulsory'])+'</td>'
        html += '<td class="c7">'+str(item.get('Set_At', ''))+'</td>'
        html += '</tr>'
    html += '</table>'

    html += '<br>Outputs'
    html += '<table>'
    html += '<thead><th>Name</th><th>Type</th><th>TypeID</th><th>Description</th><th>Units</th><th>ObjID</th></thead>'
    for item in wdata["IOCard"]["Outputs"]:
        html += '<tr>'
        html += '<td class="c1">' + str(item['Name']) + '</td>'
        html += '<td class="c2">' + str(item['Type']) + '</td>'
        html += '<td class="c3">' + str(item['TypeID']) + '</td>'
        html += '<td class="c4">' + str(item['Description']) + '</td>'
        html += '<td class="c5">' + str(item['Units']) + '</td>'
        html += '<td class="c6">' + str(item['ObjID']) + '</td>'
        html += '</tr>'
    html += '</table>'
    # html += '<br><br>All versions of this workflow:'
    html += ''
    html += ''

    html += '<br><br><a href="/workflowexecutions?filter_workflow_id='+str(wdata['wid'])+'&filter_workflow_version='+str(wdata['Version'])+'">Executions of this workflow</a>'

    admin_rights = getUserHasAdminRights()
    if admin_rights:
        html += '<br><br><a href="'+RESTserver+'main?action=get_file&id='+str(wdata['GridFSID'])+'" target="_blank">Download file</a>'

    return my_render_template('basic.html', body=Markup(html))


def allowed_file(filename):
    ALLOWED_EXTENSIONS = {'py'}
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/workflow_add/<usecaseid>', methods=('GET', 'POST'))
def addWorkflow(usecaseid):
    admin_rights = getUserHasAdminRights()

    message = ''
    success = False
    new_workflow_id = None
    fileID = None
    wid = None
    useCase = str(usecaseid)
    if request.form and admin_rights:
        print(request.files)
        workflowInputs = None
        workflowOutputs = None
        description = None
        classname = None
        zip_filename = "files.zip"
        modulename = ""
        with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
            zip_full_path = tempDir + "/" + zip_filename
            zf = zipfile.ZipFile(zip_full_path, mode="w", compression=zipfile.ZIP_DEFLATED)
            filenames = ['file_add_1', 'file_add_2', 'file_add_3', 'file_add_4', 'file_add_5', 'file_workflow']
            for filename in filenames:
                print("checking file " + filename)
                if filename in request.files:
                    file = request.files[filename]
                    if file.filename != '':
                        print(filename + " file given")
                        if file and (allowed_file(file.filename) or filename != 'file_workflow'):
                            myfile = open(tempDir + "/" + file.filename, mode="wb")
                            myfile.write(file.read())
                            myfile.close()
                            zf.write(tempDir + "/" + file.filename, arcname=file.filename)

                            if filename == 'file_workflow':
                                print("analyzing workflow file")
                                modulename = file.filename.replace(".py", "")
                                sys.path.append(tempDir)
                                moduleImport = importlib.import_module(modulename)

                                classes = []
                                for name, obj in inspect.getmembers(moduleImport):
                                    if inspect.isclass(obj):
                                        if obj.__module__ == modulename:
                                            classes.append(obj.__name__)

                                if len(classes) == 1:
                                    classname = classes[0]
                                    workflowClass = getattr(moduleImport, classname)
                                    workflow_instance = workflowClass()
                                    wid = workflow_instance.getMetadata('ID')
                                    workflowInputs = workflow_instance.getMetadata('Inputs')
                                    workflowOutputs = workflow_instance.getMetadata('Outputs')
                                    description = workflow_instance.getMetadata('Description')
                                    models_md = workflow_instance.getMetadata('Models')
                                else:
                                    print("File does not contain only one class")
                    else:
                        print(filename + " file NOT provided")
            zf.close()
            if wid is not None and workflowInputs is not None and workflowOutputs is not None and description is not None and classname is not None:
                new_workflow_id = mupifDB.workflowmanager.insertWorkflowDefinition(
                    wid=wid,
                    description=description,
                    source=zip_full_path,
                    useCase=useCase,
                    workflowInputs=workflowInputs,
                    workflowOutputs=workflowOutputs,
                    modulename=modulename,
                    classname=classname,
                    models_md=models_md
                )

    if new_workflow_id is not None:
        html = '<h3>Workflow has been registered</h3>'
        html += '<a href="/workflows/'+str(wid)+'">Go to workflow detail</a>'
        return my_render_template('basic.html', body=Markup(html))
    else:
        # generate input form
        html = message
        html += "<h3>Add new workflow:</h3>"
        if admin_rights:
            html += "<h5>(The workflow module file should contain only one class implementation.):</h5>"
            html += "<table>"

            html += '<input type="hidden" name="somedata" value="">'

            html += '<tr><td>Workflow module file</td><td><input type="file" name="file_workflow"></td></tr>'
            for add_file in range(1, 6):
                html += '<tr><td>Additional file #%d</td><td><input type="file" name="file_add_%d"></td></tr>' % (add_file, add_file)

            html += "</table>"
            html += "<input type=\"submit\" value=\"Submit\" />"
        else:
            html += "<h5>You don't have permission to visit this page.</h5>"
        return my_render_template('form.html', form=html)


@app.route('/workflowexecutions')
def executions():
    filter_workflow_id = ''
    filter_workflow_version = ''
    filter_label = ''
    filter_num_lim = '100'
    filter_status = ''

    args = {}
    for key, value in request.args.items():
        args[key] = value

    if 'filter_workflow_id' in args:
        filter_workflow_id = str(args['filter_workflow_id'])
    if 'filter_workflow_version' in args:
        filter_workflow_version = str(args['filter_workflow_version'])
    if 'filter_label' in args:
        filter_label = str(args['filter_label'])
    if 'filter_num_lim' in args:
        filter_num_lim = str(args['filter_num_lim'])
    if 'filter_status' in args:
        filter_status = str(args['filter_status'])

    html = '<h3>List of workflow executions:</h3>'
    html += '<form id="filtering_form" action="" style="font-size:12px;" method="get">'
    html += 'WorkflowID: <input type="text" name="filter_workflow_id" value="' + filter_workflow_id + '" style="width:100px;"> '
    html += 'ver.: <input type="text" name="filter_workflow_version" value="' + filter_workflow_version + '" style="width:20px;"> '
    html += 'label: <input type="text" name="filter_label" value="' + filter_label + '" style="width:100px;"> '
    html += 'status: <select name="filter_status">'
    html += '<option value="">Any</option>'
    status_list = ['Created', 'Pending', 'Scheduled', 'Running', 'Finished', 'Failed']
    for st in status_list:
        selected = ' selected' if filter_status == st else ''
        html += '<option value="' + st + '"' + selected + '>' + st + '</option>'
    html += '</select> '
    html += 'number of records: <input type="text" name="filter_num_lim" value="' + filter_num_lim + '" style="width:40px;"> '
    html += '<input type="submit" value="filter">'
    html += '</form><br>'

    html += '<table><tr><td>Status</td><td></td><td>Workflow</td><td>CreatedDate</td><td>SubmittedDate</td><td>StartDate</td><td>EndDate</td></tr>'
    param_filter_workflow_id = filter_workflow_id if filter_workflow_id != '' else None
    param_filter_workflow_version = filter_workflow_version if filter_workflow_version != '' else None
    param_filter_label = filter_label if filter_label != '' else None
    data = restApiControl.getExecutionRecords(workflow_id=param_filter_workflow_id, workflow_version=param_filter_workflow_version, label=param_filter_label, num_limit=filter_num_lim, status=filter_status)
    for execution in data:
        html += '<tr>'
        html += '<td style="'+statusColor(execution['Status'])+'">'+execution['Status']+'</td>'
        html += '<td><a href="'+request.host_url+'workflowexecutions/'+execution['_id']+'" target="_blank">link</a></td>'
        html += '<td>'+execution['WorkflowID']+'(v'+str(execution['WorkflowVersion'])+')</td>'
        html += '<td style="font-size:12px;">'+str(execution['CreatedDate']).replace('None', '')[:19]+'</td>'
        html += '<td style="font-size:12px;">'+str(execution['SubmittedDate']).replace('None', '')[:19]+'</td>'
        html += '<td style="font-size:12px;">'+str(execution['StartDate']).replace('None', '')[:19]+'</td>'
        html += '<td style="font-size:12px;">'+str(execution['EndDate']).replace('None', '')[:19]+'</td>'
        html += '</tr>'

    html += '</table>'
    return my_render_template('basic.html', body=Markup(html))


@app.route('/workflowexecutions/init/<wid>/<version>')
def initexecution(wid, version):
    we_record = restApiControl.getWorkflowRecordGeneral(wid, int(version))
    if we_record is not None:
        weid = restApiControl.insertExecution(wid, int(version), ip=getUserIPAddress())
        return redirect(url_for("executionStatus", weid=weid))
    else:
        return my_render_template('basic.html', body=Markup('<h5>Workflow with given ID and version was not found.</h5>'))


@app.route('/workflowexecutions/<weid>')
def executionStatus(weid):
    data = restApiControl.getExecutionRecord(weid)
    logID = data.get('ExecutionLog')
    html = ''
    html += '<script type="text/javascript">window.execution_id = "' + weid + '";</script>'
    html += '<script type="text/javascript" src="/main.js"></script>'
    if data['Status'] == 'Pending' or data['Status'] == 'Running' or data['Status'] == 'Scheduled':
        html += '<script type="text/javascript">let timer_refresh = setInterval(reloadIfExecStatusIsChanged, 15000);</script>'
    html += '<table style="font-size:14px;">'
    html += '<tr><td>Execution record ID:</td><td>' + str(weid) + '</td></tr>'
    html += '<tr><td>Workflow ID:</td><td>' + str(data['WorkflowID']) + '</td></tr>'
    html += '<tr><td>Workflow version:</td><td>' + str(data['WorkflowVersion']) + '</td></tr>'
    html += '<tr><td>Task ID:</td><td>' + str(data['Task_ID']) + '</td></tr>'
    html += '<tr><td>Label:</td><td>' + str(data['label']) + '</td></tr>'
    html += '<tr><td>E-mail address:</td><td>' + str(data['RequestedBy']) + '</td></tr>'

    html += '<tr><td colspan="2" style="height:10px;"></td></tr>'

    html += '<tr><td>Status:</td><td>' + str(data['Status']) + '</td></tr>'
    html += '<tr><td>Start Date:</td><td>' + str(data['StartDate']) + '</td></tr>'
    html += '<tr><td>End Date:</td><td>' + str(data['EndDate']) + '</td></tr>'
    html += '</table>'
    html += '<br>'
    html += 'Actions:<br>'
    html += '<ul>'
    html += '<li> <a href="' + request.host_url + 'workflowexecutions/' + weid + '/inputs">' + ('Set inputs and Task_ID' if data['Status'] == 'Created' else 'Inputs') + '</a></li>'
    if data['Status'] == 'Created':
        if mupifDB.workflowmanager.checkInputs(weid):
            _workflow = restApiControl.getWorkflowRecordGeneral(data['WorkflowID'], data['WorkflowVersion'])
            if mp.Workflow.checkModelRemoteResourcesByMetadata(_workflow['Models']):
                html += '<li> <a href="' + request.host_url + 'executeworkflow/' + weid + '">Schedule execution</a></li>'
            else:
                html += '<li>Some resources are not available. Cannot be scheduled.</li>'
        else:
            html += '<li>Some inputs are not defined propertly. Cannot be scheduled.</li>'
    if data['Status'] == 'Finished':
        html += '<li> <a href="' + request.host_url + 'workflowexecutions/' + weid + '/outputs">Discover outputs</a></li>'
    if (data['Status'] == 'Finished' or data['Status'] == 'Failed') and logID is not None:
        html += '<li> <a href="' + RESTserver + 'main?action=get_file&id=' + str(logID) + '"> Execution log</a></li>'
    html += '</ul>'

    return my_render_template('basic.html', body=Markup(html))


@app.route('/executeworkflow/<weid>')
def executeworkflow(weid):
    restApiControl.scheduleExecution(weid)
    data = restApiControl.getExecutionRecord(weid)
    logID = data['ExecutionLog']
    return redirect(url_for("executionStatus", weid=weid))


@app.route('/workflowexecutions/<weid>/inputs', methods=('GET', 'POST'))
def setExecutionInputs(weid):
    execution_record = restApiControl.getExecutionRecord(weid)
    wid = execution_record["WorkflowID"]
    execution_inputs = restApiControl.getExecutionInputRecord(weid)
    workflow_record = restApiControl.getWorkflowRecord(wid)
    winprec = workflow_record["IOCard"]["Inputs"]
    if request.form:
        if execution_record["Status"] == "Created":
            restApiControl.setExecutionParameter(execution_record['_id'], 'Task_ID', request.form['Task_ID'])
            restApiControl.setExecutionParameter(execution_record['_id'], 'label', request.form['label'])
            restApiControl.setExecutionParameter(execution_record['_id'], 'RequestedBy', request.form['RequestedBy'])

            # process submitted data
            msg = ""
            c = 0
            for i in execution_inputs:
                name = i['Name']
                objID = i['ObjID']
                value = request.form.get('Value_%d' % c, '')
                units = i['Units']

                # set Link to output data
                c_eid = request.form.get('c_eid_%d' % c, '')
                c_name = request.form.get('c_name_%d' % c, '')
                c_objid = request.form.get('c_objid_%d' % c, '')
                if c_eid != "" and c_name != "":
                    restApiControl.setExecutionInputLink(weid, name, objID, c_eid, c_name, c_objid)
                    restApiControl.setExecutionInputObject(weid, name, objID, {})
                else:
                    restApiControl.setExecutionInputLink(weid, name, objID, '', '', '')
                    if i['Type'] == 'mupif.Property':
                        msg += 'Setting %s (ObjID %s) to %s [%s]</br>' % (name, objID, value, units)

                        object_dict = {
                            'ClassName': 'ConstantProperty',
                            'ValueType': i['ValueType'],
                            'DataID': i['TypeID'].replace('mupif.DataID.', ''),
                            'Unit': i['Units'],
                            'Value': literal_eval(value),
                            'Time': None
                        }
                        restApiControl.setExecutionInputObject(weid, name, objID, object_dict)
                    elif i['Type'] == 'mupif.String':
                        msg += 'Setting %s (ObjID %s) to %s</br>' % (name, objID, value)

                        object_dict = {
                            'ClassName': 'String',
                            'DataID': i['TypeID'].replace('mupif.DataID.', ''),
                            'Value': str(value)
                        }
                        restApiControl.setExecutionInputObject(weid, name, objID, object_dict)
                    else:
                        print("Unknown data type")

                c = c+1
            msg += "</br><a href=\"/workflowexecutions/"+weid+"\">Back to Execution record "+weid+"</a>"
            return my_render_template("basic.html", body=Markup(msg))
    else:      
        # generate input form
        form = ""

        form += "<h3>Workflow: %s</h3><br>" % wid

        form += "Task_ID: "
        if execution_record["Status"] == "Created":
            form += "<input type=\"text\" name=\"Task_ID\" value=\"%s\" /><br>" % execution_record["Task_ID"]
        else:
            form += "%s<br>" % execution_record["Task_ID"]

        form += "Label: "
        if execution_record["Status"] == "Created":
            form += "<input type=\"text\" name=\"label\" value=\"%s\" /><br>" % execution_record["label"]
        else:
            form += "%s<br>" % execution_record["label"]

        form += "E-mail address: "
        if execution_record["Status"] == "Created":
            form += "<input type=\"text\" name=\"RequestedBy\" value=\"%s\" /><br>" % execution_record["RequestedBy"]
        else:
            form += "%s<br>" % execution_record["RequestedBy"]

        form += "<br>Input record for weid %s<table>" % weid
        form += "<tr><th>Name</th><th>Type</th><th>ValueType</th><th>DataID</th><th>Description</th><th>ObjID</th><th>Value</th><th>Units</th><th>Link_EID</th><th>Link_Name</th><th>Link_ObjID</th></tr>"
        c = 0
        for i in execution_inputs:
            print(i)
            name = i['Name']
            # get description from workflow rec
            description = ""
            for ii in winprec:
                if ii["Name"] == name:
                    description = ii.get("Description")
                    break

            input_type = i['Type']
            if i.get('Compulsory', False):
                required = "required"
            else:
                required = ""

            form += '<tr>'

            if input_type == "mupif.Property":
                form += '<td>' + str(i['Name']) + '</td>'
                form += '<td>' + str(i['Type']) + '</td>'
                form += '<td>' + str(i.get('ValueType', '')) + '</td>'
                form += '<td>' + str(i.get('TypeID', '[unknown]')).replace('mupif.DataID.', '') + '</td>'
                form += '<td>' + str(description) + '</td>'
                form += '<td>' + str(i['ObjID']) + '</td>'
                form += '<td>'
                if execution_record["Status"] == "Created":
                    try:
                        prop = mupif.ConstantProperty.from_db_dict(i['Object'])
                        ival = prop.quantity.inUnitsOf(i['Units']).value.tolist()
                    except:
                        ival = None
                    form += "<input type=\"text\" name=\"Value_%d\" value=\"%s\" %s/>" % (c, str(ival), required)
                else:
                    form += str(i['Object']['Value'])
                form += "</td>"
                form += '<td>' + str(i.get('Units')) + '</td>'

            elif input_type == "mupif.String":
                form += '<td>' + str(i['Name']) + '</td>'
                form += '<td>' + str(i['Type']) + '</td>'
                form += '<td>' + str(i.get('ValueType', '')) + '</td>'
                form += '<td>' + str(i.get('TypeID', '[unknown]')).replace('mupif.DataID.', '') + '</td>'
                form += '<td>' + str(description) + '</td>'
                form += '<td>' + str(i['ObjID']) + '</td>'
                form += '<td>'
                if execution_record["Status"] == "Created":
                    try:
                        prop = mupif.String.from_db_dict(i['Object'])
                        ival = prop.getValue()
                    except:
                        ival = ''
                    form += "<input type=\"text\" name=\"Value_%d\" value=\"%s\" %s/>" % (c, str(ival), required)
                else:
                    form += str(i['Object']['Value'])
                form += "</td>"
                form += '<td>' + str(i.get('Units')) + '</td>'

            else:
                form += '<td>' + str(i['Name']) + '</td>'
                form += '<td>' + str(i['Type']) + '</td>'
                form += '<td>' + str(i.get('ValueType', '')) + '</td>'
                form += '<td>' + str(i.get('TypeID', '[unknown]')).replace('mupif.DataID.', '') + '</td>'
                form += '<td>' + str(description) + '</td>'
                form += '<td>' + str(i['ObjID']) + '</td>'
                form += '<td>' + str(i.get('Object', {}).get('Value', '')) + '</td>'
                form += '<td>' + str(i.get('Units')) + '</td>'

            if execution_record["Status"] == "Created":
                form += "<td><input type=\"text\" name=\"c_eid_%d\" value=\"%s\" style=\"width:100px;\" /></td>" % (c, i['Link']['ExecID'])
                form += "<td><input type=\"text\" name=\"c_name_%d\" value=\"%s\" style=\"width:60px;\" /></td>" % (c, i['Link']['Name'])
                form += "<td><input type=\"text\" name=\"c_objid_%d\" value=\"%s\" style=\"width:60px;\" /></td>" % (c, i['Link']['ObjID'])
            else:
                form += "<td>" + str(i['Link']['ExecID']) + "</td>"
                form += "<td>" + str(i['Link']['Name']) + "</td>"
                form += "<td>" + str(i['Link']['ObjID']) + "</td>"

            form += "</tr>"
            c += 1

        form += "</table>"
        form += "<br>"
        form += "<input type=\"hidden\" name=\"eid\" value=\"%s\"/>" % weid
        if execution_record["Status"] == "Created":
            form += "<input type=\"submit\" value=\"Submit\" />"

        return my_render_template('form.html', form=form)


@app.route("/workflowexecutions/<weid>/outputs")
def getExecutionOutputs(weid):
    execution_record = restApiControl.getExecutionRecord(weid)
    wid = execution_record["WorkflowID"]
    execution_outputs = restApiControl.getExecutionOutputRecord(weid)
    workflow_record = restApiControl.getWorkflowRecord(wid)
    # winprec = workflow_record["IOCard"]["Outputs"]

    # generate result table form
    form = "<h3>Workflow: %s</h3>Output record for weid %s<table>" % (wid, weid)
    form += "<tr><th>Name</th><th>Type</th><th>ValueType</th><th>DataID</th><th>ObjID</th><th>Units</th><th>Value</th></tr>"
    for i in execution_outputs:
        val = '<i>unable to display</i>'

        if i['Type'] == 'mupif.Property':
            if i['Object'].get('FileID') is not None and i['Object'].get('FileID') != '':
                val = '<a href="/property_array_view/' + str(i['Object'].get('FileID')) + '/1">link</a>'
            else:
                prop = mupif.ConstantProperty.from_db_dict(i['Object'])
                val = prop.inUnitsOf(i.get('Units', '')).getValue()

        if i['Type'] == 'mupif.String':
            prop = mupif.String.from_db_dict(i['Object'])
            val = prop.getValue()

        form += '<tr>'
        form += '<td>' + str(i['Name']) + '</td>'
        form += '<td>' + str(i['Type']) + '</td>'
        form += '<td>' + str(i.get('ValueType', '')) + '</td>'
        form += '<td>' + str(i.get('TypeID', '[unknown]')).replace('mupif.DataID.', '') + '</td>'
        form += '<td>' + str(i['ObjID']) + '</td>'
        form += '<td>' + str(escape(i.get('Units'))) + '</td>'
        form += '<td>' + str(val) + '</td>'
    form += "</table>"
    form += "</br><a href=\"/workflowexecutions/" + weid + "\">Back to Execution record " + weid + "</a>"
    # print (form)
    return my_render_template('basic.html', body=Markup(form))


@app.route("/property_array_view/<file_id>/<page>")
def propertyArrayView(file_id, page):
    page = int(page)
    html = '<h3>Content of mupif.Property stored in file id %s</h3>' % file_id

    pfile, fn = restApiControl.getBinaryFileByID(file_id)
    with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
        full_path = tempDir + "/file.h5"
        f = open(full_path, 'wb')
        f.write(pfile)
        f.close()
        prop = mp.ConstantProperty.loadHdf5(full_path)
        propval = prop.getValue()

        html += '<table style="font-size:14px;">'
        html += '<tr><td>Type_ID:</td><td>' + str(prop.propID) + '</td></tr>'
        html += '<tr><td>Units:</td><td>' + str(prop.getUnit().to_string()) + '</td></tr>'
        html += '<tr><td>ValueType:</td><td>' + str(prop.valueType) + '</td></tr>'
        html += '</table>'

        tot_elems = propval.shape[0]
        per_page = 100
        maxpage = math.ceil(tot_elems/per_page)
        if page < 1:
            page = 1
        if page > maxpage:
            page = maxpage

        id_start = int((page - 1) * per_page)
        id_end = int(page * per_page)

        if maxpage > 1:
            html += '<h4>'
            if page > 1:
                html += '&nbsp;&nbsp;<a href="/property_array_view/' + file_id + '/' + str(page - 1) + '"><</a>'
            html += '&nbsp;&nbsp;&nbsp;page ' + str(page) + '&nbsp;/&nbsp;' + str(maxpage) + '&nbsp;&nbsp;&nbsp;'
            if page < maxpage:
                html += '<a href="/property_array_view/' + file_id + '/' + str(page + 1) + '">></a>'
            html += '</h4>'

        html += '<table style="font-size:12px;margin-top:10px;">'
        html += '<td></td>'
        num_cols = 1
        if len(propval[0].shape) > 0:
            num_cols = propval[0].shape[0]
        for col_id in range(num_cols):
            html += '<td style="text-align:center;color:gray;"><i>[' + str(col_id+1) + ']</i></td>'
        sub_propval = propval[id_start:id_end]
        row_id = id_start + 1
        for elem in sub_propval:
            html += '<tr><td><i style="color:gray;">[' + str(row_id) + ']</i></td>'
            if len(elem.shape) == 0:
                html += '<td>%.3e</td>' % elem
            else:
                for subelem in elem:
                    if len(subelem.shape) == 0:
                        html += '<td>%.3e</td>' % subelem
                    else:
                        html += '<td>' + str(subelem) + '</td>'
            html += '</tr>'
            row_id += 1
        html += '<table><br><br><br><br><br><br><br><br><br><br>'

    return my_render_template('basic.html', body=Markup(html))


@app.route('/hello/<name>')
def hello(name=None):
    return my_render_template('hello.html', name=name, content="Welcome to MuPIFDB web interface")


@app.route('/main.js')
def mainjs():
    return send_from_directory(directory='./', path='main.js')


@app.route('/api/')
def restapi():
    full_url = str(request.url)
    args_str = full_url.split('?')[1]
    full_rest_url = RESTserver + "main?" + args_str
    print(full_rest_url)
    response = requests.get(full_rest_url)
    return jsonify(response.json())


@app.route('/workflow_check', methods=('GET', 'POST'))
def workflow_check():
    html = ''
    html += "<h3>Testing workflow implementation</h3>"

    success = False
    new_workflow_id = None
    fileID = None
    wid = None
    if request.form:

        html += "<h5><a href="">Back</a></h5>"

        noproblem = False
        print(request.files)
        workflowInputs = None
        workflowOutputs = None
        description = None
        classname = None
        zip_filename = "files.zip"
        modulename = ""
        with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
            zip_full_path = tempDir + "/" + zip_filename
            zf = zipfile.ZipFile(zip_full_path, mode="w", compression=zipfile.ZIP_DEFLATED)
            filenames = ['file_add_1', 'file_add_2', 'file_add_3', 'file_add_4', 'file_add_5', 'file_workflow']
            for filename in filenames:
                print("checking file " + filename)
                if filename in request.files:
                    file = request.files[filename]
                    if file.filename != '':
                        print(filename + " file given")
                        if file and (allowed_file(file.filename) or filename != 'file_workflow'):
                            myfile = open(tempDir + "/" + file.filename, mode="wb")
                            myfile.write(file.read())
                            myfile.close()
                            zf.write(tempDir + "/" + file.filename, arcname=file.filename)

                            if filename == 'file_workflow':
                                print("analyzing workflow file")
                                modulename = file.filename.replace(".py", "")
                                sys.path.append(tempDir)
                                moduleImport = importlib.import_module(modulename)

                                classes = []
                                for name, obj in inspect.getmembers(moduleImport):
                                    if inspect.isclass(obj):
                                        if obj.__module__ == modulename:
                                            classes.append(obj.__name__)

                                if len(classes) == 1:
                                    classname = classes[0]
                                    workflowClass = getattr(moduleImport, classname)
                                    workflow_instance = workflowClass()

                                    noproblem = True

                                    schema = copy.deepcopy(mp.workflow.WorkflowSchema)
                                    schema['required'].remove('Dependencies')
                                    schema['required'].remove('Execution')
                                    schema['properties'].pop('Execution', None)
                                    try:
                                        workflow_instance.validateMetadata(schema)
                                    except:
                                        noproblem = False
                                        html += '<h5 style="color:red;">Metadata validation was not successful.</h5>'

                                    # TODO do more checks
                                    # wid = workflow_instance.getMetadata('ID')
                                    # workflowInputs = workflow_instance.getMetadata('Inputs')
                                    # workflowOutputs = workflow_instance.getMetadata('Outputs')
                                    # description = workflow_instance.getMetadata('Description')
                                    #
                                    # if wid is None:
                                    #     noproblem = False
                                    # if workflowInputs is None:
                                    #     noproblem = False
                                    # if workflowOutputs is None:
                                    #     noproblem = False
                                    # if description is None:
                                    #     noproblem = False
                                    # if classname is None:
                                    #     noproblem = False

                                else:
                                    print("File does not contain only one class")
                                    html += '<h5 style="color:red;">The workflow file doesn\'t contain only one class.</h5>'
                    elif filename == 'file_workflow':
                        html += '<h5 style="color:red;">The workflow file was not provided.</h5>'
            zf.close()

        if noproblem is True:
            html += '<h5 style="color:green;">No problems found in the workflow implementation.</h5>'

        return my_render_template('basic.html', body=Markup(html))
    else:
        # generate input form
        html = ''

        html += "<h4>Upload the workflow Python file (and consecutive Python modules):</h4>"
        html += "<h5>(The workflow module file should contain only one class implementation.):</h5>"
        html += "<table>"

        html += '<input type="hidden" name="somedata" value="">'

        html += '<tr><td>Workflow module file</td><td><input type="file" name="file_workflow"></td></tr>'
        for add_file in range(1, 6):
            html += '<tr><td>Additional file #%d</td><td><input type="file" name="file_add_%d"></td></tr>' % (add_file, add_file)

        html += "</table>"
        html += "<input type=\"submit\" value=\"Submit\" />"

        return my_render_template('form.html', form=html)


if __name__ == '__main__':
    app.run(debug=True)
