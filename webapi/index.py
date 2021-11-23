import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/../mupifDB")
from flask import Flask, render_template, Markup, escape, redirect, url_for
# from flask_wtf import FlaskForm
# from wtforms import StringField
from flask import request
from flask_cors import CORS
import requests 
import json
import restApiControl

from mongoflask import ObjectIdConverter

app = Flask(__name__)
CORS(app, resources={r"/static/*": {"origins": "*"}})
app.url_map.converters['objectid'] = ObjectIdConverter


# unless overridden by the environment, use 127.0.0.1:5000
RESTserver = os.environ.get('MUPIFDB_REST_SERVER', "http://127.0.0.1:5000/")

# RESTserver *must* have trailing /, fix if not
if not RESTserver[-1] == '/':
    RESTserver += '/'

# server (that is, our URL) is obtained within request handlers as flask.request.host_url+'/'
# server = "http://127.0.0.1:5555/"
# server = "http://127.0.0.1:5555/"


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
    return render_template('basic.html', title="MuPIFDB web interface", body=Markup(msg))


@app.route('/status')
def status():
    data = restApiControl.getStatus()
    stat = data['totalStat']
    msg = "<div><div>"
    msg += "<dl><dt>MupifDBStatus:"+data['mupifDBStatus']+"</dt>"
    msg += "<dt>SchedulerStatus:"+data['schedulerStatus']+"</dt>"
    msg += "    <dd>Total    executions:"+str(stat['totalExecutions'])+"</dd>"
    msg += "    <dd>Finished executions:"+str(stat['finishedExecutions'])+"</dd>"
    msg += "    <dd>Failed   executions:"+str(stat['failedExecutions'])+"</dd>"
    msg += "</dl></div>"
    # msg+= "<div class=\"chart-container\" width=\"500\" height=\"200\">"
    # msg+= "<canvas id=\"updating-chart\" width=\"500\" height=\"200\" ></canvas>"
    # msg+= "</div></div>"
    # msg+= "<div style=\"clear: both\">"
    # msg+= "<a href=\""+RESTserver+"schedulerStats/hourly.svg\">48 hour statistics</a></br>"
    # msg+= "<a href=\""+RESTserver+"schedulerStats/weekly.svg\">52 week statistics</a></div>"
    # msg+= "<div><img src=\""+RESTserver+"schedulerStats/hourly.svg"+"\"></div>"
    msg += ""
    return render_template('stat.html', title="MuPIFDB web interface", server=request.host_url+'/', body=Markup(msg))


@app.route('/contact')
def contact():
    msg = """
        <p>MuPIF and MuPIFDB have been developped at <a href=\"https://www.cvut.cz/en\">Czech Technical University in Prague</a> by a research team at the Department of Mechanics of the <a href=\"https://web.fsv.cvut.cz/en/\">Faculty of Civil Engineering</a>.</p>
        <p>For more information and help please contact Borek Patzak (borek.patzak@fsv.cvut.cz)</p>  
    """
    return render_template('basic.html', title="MuPIFDB web interface", body=Markup(msg))


@app.route('/usecases')
def usecases():
    data = restApiControl.getUsecaseRecords()
    return render_template('usecases.html', title="MuPIFDB web interface", server=request.host_url+'/', items=data)
    # return r.json()


@app.route('/usecases/<ucid>/workflows')
def usecaseworkflows(ucid):
    data = restApiControl.getWorkflowRecordsWithUsecase(ucid)
    return render_template('workflows.html', title="MuPIFDB web interface", server=request.host_url+'/', items=data)


@app.route('/workflows')
def worflows():
    data = restApiControl.getWorkflowRecords()
    return render_template('workflows.html', title="MuPIFDB web interface", server=request.host_url+'/', items=data)


@app.route('/workflows/<wid>')
def workflow(wid):
    wdata = restApiControl.getWorkflowRecord(wid)
    return render_template(
        'workflow.html', title="MuPIFDB web interface", server=request.host_url+'/',
        wid=wid, id=wdata['_id'], UseCas=wdata["UseCase"], Description=wdata["Description"],
        inputs=wdata["IOCard"]["Inputs"], outputs=wdata["IOCard"]["Outputs"],
        version=wdata.get("Version", 1)
    )


@app.route('/workflowexecutions/init/<wid>')
def initexecution(wid):
    weid = restApiControl.insertExecutionRecord(wid)
    return redirect(url_for("executionStatus", weid=weid))


@app.route('/workflowexecutions/<weid>')
def executionStatus(weid):
    data = restApiControl.getExecutionRecord(weid)
    logID = data.get('ExecutionLog')
    return render_template('workflowexecution.html', title="MuPIFDB web interface", server=request.host_url+'/', RESTserver=RESTserver, wid=data['WorkflowID'], id=weid, logID=logID, data=data)


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
        # process submitted data
        msg = ""
        # print (request.form.get('eid'))
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
            # try:
            #    inp.set(name, value, objID)
            #    print (' OK<br>')
            # except Exception as e:
            #    print(' Failed<br>')
            #    print (e)
            # set source and origin
            c = c+1
        r = requests.get(url=RESTserver+"workflowexecutions/"+weid+'/set', params=payload)
        msg += str(r.json())
        msg += "</br><a href=\"/workflowexecutions/"+weid+"\">Continue to Execution record "+weid+"</a>"
        return render_template("basic.html", body=Markup(msg))
    else:      
        # generate input form
        form = "<h3>Workflow: %s</h3>Input record for weid %s<table>" % (wid, weid)
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
                form += "<tr><td>#%s</td><td>%s</td><td>%s</td><td>%s</td><td><input type=\"text\" pattern=\"%s\" name=\"Value_%d\" value=\"%s\" %s/></td><td>%s</td></tr>" % (i['Name'], description, i['Type'], i['ObjID'], pattern, c, i['Value'], required, i.get('Units'))
            elif input_type == "mupif.Field":
                form += "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td><input type=\"text\" pattern=\"^\([-+]?[0-9]*\.?[0-9]*([eE][-+]?[0-9]+)?(,[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?)*\)\" name=\"Value_%d\" value=\"%s\" %s/></td><td>%s</td></tr>" % (i['Name'], description, i['Type'], i['ObjID'], c, i['Value'], required, i.get('Units'))
            else:
                # fallback input no check except for required
                form += "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td><input type=\"text\" name=\"Value_%d\" value=\"%s\" %s/></td><td>%s</td></tr>" % (i['Name'], description, i['Type'], i['ObjID'], c, i['Value'], required, i.get('Units'))
            c += 1
        form += "</table>"
        form += "<input type=\"hidden\" name=\"eid\" value=\"%s\"/>" % weid
        form += "<input type=\"submit\" value=\"Submit\" />"
        # print (form)
        return render_template('form.html', title="MuPIFDB web interface", form=form)


@app.route("/workflowexecutions/<weid>/outputs")
def getExecutionOutputs(weid):
    # get we record
    r = requests.get(url=RESTserver+"workflowexecutions/"+str(weid))
    we = r.json()["result"][0]
    wid = we["WorkflowID"]
    # get execution input record (to access inputs)
    r = requests.get(url=RESTserver+"workflowexecutions/"+weid+'/outputs')
    outrec = r.json()["result"]

    # generate result table form
    form = "<h3>Workflow: %s</h3>Output record for weid %s<table>" % (wid, weid)
    form += "<tr><th>Name</th><th>Type</th><th>ObjID</th><th>Value</th><th>Units</th></tr>"
    for i in outrec:
        # print(i)
        form += "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (i['Name'], i['Type'], i['ObjID'], i['Value'], escape(i.get('Units')))
    form += "</table>"
    form += "</br><a href=\"/workflowexecutions/" + weid + "\">Continue to Execution record " + weid + "</a>"
    # print (form)
    return render_template('basic.html', title="MuPIFDB web interface", body=Markup(form))


@app.route('/hello/<name>')
def hello(name=None):
    return render_template('hello.html', name=name, content="Welcome to MuPIFDB web interface")


if __name__ == '__main__':
    app.run(debug=True)
