import importlib
import zipfile
import tempfile
from flask import Flask, render_template, Markup, escape, redirect, url_for, send_from_directory
from flask import request
from flask_cors import CORS
import sys
import os

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

# server (that is, our URL) is obtained within request handlers as flask.request.host_url+'/'


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
    return my_render_template('basic.html',body=Markup(msg))


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
    msg += ""
    return my_render_template('stat.html', body=Markup(msg))


@app.route("/schedulerStats/weekly.svg")
def schedulerStatWeekly():
    print("ggg")
    return send_from_directory(directory=path_of_this_file + "/static/images", path="scheduler_weekly_stat.svg")


@app.route("/schedulerStats/hourly.svg")
def schedulerStatHourly():
    print("ggg")
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
    data = restApiControl.getUsecaseRecords()
    return my_render_template('usecases.html', items=data)


@app.route('/usecase_add', methods=('GET', 'POST'))
def addUseCase():
    message = ''
    usecase_id = ''
    usecase_description = ''
    new_usecase_id = None
    if request.form:
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
        html += "<table>"
        html += '<tr><td>UseCase ID (string)</td><td><input type="text" name="usecase_id" value="'+str(usecase_id)+'"></td></tr>'
        html += '<tr><td>UseCase Description (string)</td><td><input type="text" name="usecase_description" value="'+str(usecase_description)+'"></td></tr>'
        html += "</table>"
        html += "<input type=\"submit\" value=\"Submit\" />"
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
def workflow(wid):
    wdata = restApiControl.getWorkflowRecord(wid)
    return my_render_template(
        'workflow.html',
        wid=wid, id=wdata['_id'], UseCase=wdata["UseCase"], Description=wdata["Description"],
        inputs=wdata["IOCard"]["Inputs"], outputs=wdata["IOCard"]["Outputs"],
        version=wdata.get("Version", 1)
    )


def allowed_file(filename):
    ALLOWED_EXTENSIONS = {'py'}
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/workflow_add/<usecaseid>', methods=('GET', 'POST'))
def addWorkflow(usecaseid):
    message = ''
    success = False
    new_workflow_id = None
    fileID = None
    classname = ""
    wid = None
    useCase = str(usecaseid)
    if request.form:
        print(request.files)
        workflowInputs = None
        workflowOutputs = None
        description = None
        classname = request.form['classname']
        zip_filename = "files.zip"
        modulename = ""
        with tempfile.TemporaryDirectory(dir="/tmp", prefix='mupifDB') as tempDir:
            zip_full_path = tempDir + "/" + zip_filename
            zf = zipfile.ZipFile(zip_full_path, mode="w", compression=zipfile.ZIP_DEFLATED)
            filenames = ['file_workflow', 'file_add_1', 'file_add_2', 'file_add_3', 'file_add_4', 'file_add_5']
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
                                workflowClass = getattr(moduleImport, classname)
                                workflow_instance = workflowClass()
                                wid = workflow_instance.getMetadata('ID')
                                workflowInputs = workflow_instance.getMetadata('Inputs')
                                workflowOutputs = workflow_instance.getMetadata('Outputs')
                                description = workflow_instance.getMetadata('Description')
                    else:
                        print(filename + " file NOT provided")
            zf.close()
            if wid is not None and workflowInputs is not None and workflowOutputs is not None and description is not None:
                new_workflow_id = mupifDB.workflowmanager.insertWorkflowDefinition(
                    wid=wid,
                    description=description,
                    source=zip_full_path,
                    useCase=useCase,
                    workflowInputs=workflowInputs,
                    workflowOutputs=workflowOutputs,
                    modulename=modulename,
                    classname=classname
                )

    if new_workflow_id is not None:
        html = '<h3>Workflow has been registered</h3>'
        html += '<a href="/workflows/'+str(wid)+'">Go to workflow detail</a>'
        return my_render_template('basic.html', body=Markup(html))
    else:
        # generate input form
        html = message
        html += "<h3>Add new workflow:</h3>"
        html += "<table>"

        html += '<tr><td>Workflow class name</td><td><input type="text" name="classname" value="'+str(classname)+'"></td></tr>'

        html += '<tr><td>Workflow module file</td><td><input type="file" name="file_workflow"></td></tr>'
        for add_file in range(1, 6):
            html += '<tr><td>Additional file #%d</td><td><input type="file" name="file_add_%d"></td></tr>' % (add_file, add_file)

        html += "</table>"
        html += "<input type=\"submit\" value=\"Submit\" />"
        return my_render_template('form.html', form=html)


@app.route('/workflowexecutions')
def executions():
    def statusColor(val):
        if val == 'Finished':
            return 'color:green;'
        if val == 'Failed':
            return 'color:red;'
        if val == 'Running':
            return 'color:blue;'
        return 'color:gray;'

    filter_workflow_id = ''
    filter_label = ''
    filter_num_lim = '100'

    args = {}
    for key, value in request.args.items():
        args[key] = value

    if 'filter_workflow_id' in args:
        filter_workflow_id = str(args['filter_workflow_id'])
    if 'filter_label' in args:
        filter_label = str(args['filter_label'])
    if 'filter_num_lim' in args:
        filter_num_lim = str(args['filter_num_lim'])

    html = '<h3>List of workflow executions:</h3>'
    html += '<form id="filtering_form" action="" style="font-size:12px;">'
    html += 'WorkflowID: <input type="text" name="filter_workflow_id" value="' + filter_workflow_id + '" style="width:100px;"> '
    html += 'label: <input type="text" name="filter_label" value="' + filter_label + '" style="width:100px;"> '
    html += 'number of records: <input type="text" name="filter_num_lim" value="' + filter_num_lim + '" style="width:40px;"> '
    html += '<input type="submit" value="filter">'
    html += '</form><br>'

    html += '<table><tr><td>Status</td><td>WorkflowID</td><td></td><td>CreatedDate</td><td>SubmittedDate</td><td>StartDate</td><td>EndDate</td></tr>'
    param_filter_workflow_id = filter_workflow_id if filter_workflow_id != '' else None
    param_filter_label = filter_label if filter_label != '' else None
    data = restApiControl.getExecutionRecords(workflow_id=param_filter_workflow_id, label=param_filter_label, num_limit=filter_num_lim)
    for execution in data:
        html += '<tr>'
        html += '<td style="'+statusColor(execution['Status'])+'">'+execution['Status']+'</td>'
        html += '<td>'+execution['WorkflowID']+'</td>'
        html += '<td><a href="'+request.host_url+'workflowexecutions/'+execution['_id']+'" target="_blank">link</a></td>'
        html += '<td style="font-size:12px;">'+str(execution['CreatedDate']).replace('None', '')[:19]+'</td>'
        html += '<td style="font-size:12px;">'+str(execution['SubmittedDate']).replace('None', '')[:19]+'</td>'
        html += '<td style="font-size:12px;">'+str(execution['StartDate']).replace('None', '')[:19]+'</td>'
        html += '<td style="font-size:12px;">'+str(execution['EndDate']).replace('None', '')[:19]+'</td>'
        html += '</tr>'

    html += '</table>'
    return my_render_template('basic.html', body=Markup(html))


@app.route('/workflowexecutions/init/<wid>')
def initexecution(wid):
    # c = mupifDB.workflowmanager.WorkflowExecutionContext.create(wid, '')
    # weid = c.executionID
    weid = restApiControl.insertExecution(wid)  # TODO uncomment commented and delete this
    return redirect(url_for("executionStatus", weid=weid))


@app.route('/workflowexecutions/<weid>')
def executionStatus(weid):
    data = restApiControl.getExecutionRecord(weid)
    logID = data.get('ExecutionLog')
    html = ''
    html += 'Execution record ID: ' + weid + '<br>'
    html += 'WorkflowID: ' + data['WorkflowID'] + '<br>'
    html += 'Task ID: ' + data['Task_ID'] + '<br>'
    html += 'Label: ' + data['label'] + '<br>'
    html += 'E-mail address: ' + data['RequestedBy'] + '<br>'

    html += '<dl>'
    html += '<dt>Status:' + str(data['Status']) + '</dt>'
    html += '<dt>Start Date:' + str(data['StartDate']) + '</dt>'
    html += '<dt>End Date:' + str(data['EndDate']) + '</dt>'
    html += '</dl>'
    html += '<br>'
    html += 'Actions:<br>'
    html += '<ul>'
    html += '<li> <a href="' + request.host_url + 'workflowexecutions/' + weid + '/inputs">' + ('Set inputs and Task_ID' if data['Status'] == 'Created' else 'Inputs') + '</a></li>'
    if data['Status'] == 'Created':
        html += '<li> <a href="' + request.host_url + 'executeworkflow/' + weid + '">Schedule execution</a></li>'
    if data['Status'] == 'Finished':
        html += '<li> <a href="' + request.host_url + 'workflowexecutions/' + weid + '/outputs">Discover outputs</a></li>'
    if data['Status'] == 'Finished' or data['Status'] == 'Failed':
        html += '<li> <a href="{' + RESTserver + 'gridfs/' + str(logID) + '"> Execution log</a></li>'
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
            payload = {}
            for i in execution_inputs:
                name = i['Name']
                objID = i['ObjID']
                value = request.form['Value_%d' % c]
                units = i['Units']
                # source = request.form.getvalue('Source_%d'%c)
                # originID  = request.form.getvalue('OriginID_%d'%c)
                msg += 'Setting %s (ObjID %s) to %s [%s]</br>' % (name, objID, value, units)
                payload[name+'{'+str(objID)+'}'] = value
                restApiControl.setIOProperty(execution_record['Inputs'], name, 'Value', value, objID)
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
        form += "<tr><th>Name</th><th>Description</th><th>Type</th><th>ObjID</th><th>Value</th><th>Units</th></tr>"
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
            if input_type == "mupif.Property":
                # float assumed
                floatPattern = "^[-+]?[0-9]*\.?[0-9]*([eE][-+]?[0-9]+)?"
                tuplePattern = "^\([-+]?[0-9]*\.?[0-9]*([eE][-+]?[0-9]+)?(,\s*[-+]?[0-9]*\.?[0-9]*([eE][-+]?[0-9]+)?)*\)"
                pattern = "(%s|%s)" % (floatPattern, tuplePattern)
                form += "<tr><td>#%s</td><td>%s</td><td>%s</td><td>%s</td><td>" % (i['Name'], description, i['Type'], i['ObjID'])
                if execution_record["Status"] == "Created":
                    form += "<input type=\"text\" pattern=\"%s\" name=\"Value_%d\" value=\"%s\" %s/>" % (pattern, c, i['Value'], required)
                else:
                    form += str(i['Value'])
                form += "</td><td>%s</td></tr>" % i.get('Units')
            elif input_type == "mupif.Field":
                form += "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td><input type=\"text\" pattern=\"^\([-+]?[0-9]*\.?[0-9]*([eE][-+]?[0-9]+)?(,[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?)*\)\" name=\"Value_%d\" value=\"%s\" %s/></td><td>%s</td></tr>" % (i['Name'], description, i['Type'], i['ObjID'], c, i['Value'], required, i.get('Units'))
            else:
                # fallback input no check except for required
                form += "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td><input type=\"text\" name=\"Value_%d\" value=\"%s\" %s/></td><td>%s</td></tr>" % (i['Name'], description, i['Type'], i['ObjID'], c, i['Value'], required, i.get('Units'))
            c += 1
        form += "</table>"
        form += "<br>"
        form += "<input type=\"hidden\" name=\"eid\" value=\"%s\"/>" % weid
        if execution_record["Status"] == "Created":
            form += "<input type=\"submit\" value=\"Submit\" />"
        # print (form)
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
    form += "<tr><th>Name</th><th>Type</th><th>ObjID</th><th>Value</th><th>Units</th></tr>"
    for i in execution_outputs:
        # print(i)
        form += "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (i['Name'], i['Type'], i['ObjID'], i['Value'], escape(i.get('Units')))
    form += "</table>"
    form += "</br><a href=\"/workflowexecutions/" + weid + "\">Back to Execution record " + weid + "</a>"
    # print (form)
    return my_render_template('basic.html', body=Markup(form))


@app.route('/hello/<name>')
def hello(name=None):
    return my_render_template('hello.html', name=name, content="Welcome to MuPIFDB web interface")


if __name__ == '__main__':
    app.run(debug=True)
