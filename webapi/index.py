from flask import Flask, render_template, Markup
#from flask_wtf import FlaskForm
#from wtforms import StringField
from flask import request
import requests 
import json
import sys
sys.path.insert(0,'..')

from mongoflask import ObjectIdConverter

app = Flask(__name__)
app.url_map.converters['objectid'] = ObjectIdConverter

RESTserver="http://172.30.0.1:5000/"
server = "http://127.0.0.1:5000/"

@app.route('/')
def homepage():
    return render_template('basic.html', title="MuPIFDB web interface", body="Welcome to MuPIFDB web interface")

@app.route('/about')
def about():
    return render_template('basic.html', title="MuPIFDB web interface", body="Welcome to MuPIFDB web interface")

@app.route('/status')
def status():
    r = requests.get(url=RESTserver+"status")
    data=r.json()['result'][0]
    msg = "<dl><dt>mupifDBStatus:"+data['mupifDBStatus']+"</dt>"
    msg+= "<dl>schedulerStatus:"+data['schedulerStatus']+"</dl>"
    msg+= "</dt>"
    msg+= "<img src=\""+RESTserver+"schedulerStats/total2.svg"+"\">"
    return render_template('basic.html', title="MuPIFDB web interface", body=Markup(msg))

@app.route('/contact')
def contact():
    return render_template('basic.html', title="MuPIFDB web interface", body="Welcome to MuPIFDB web interface")


@app.route('/usecases')
def usecases():
    r = requests.get(url=RESTserver+"usecases/any")
    print (type(r.json()))
    data = r.json()
    return render_template('usecases.html', title="MuPIFDB web interface", server=server, items=data["result"])
    #return r.json()

@app.route('/usecases/<id>/workflows')
def usecaseworkflows (id):
    r = requests.get(url=RESTserver+"usecases/"+id+"/workflows")
    rdata = r.json()
    #print (rdata)
    data = []
    for wid in rdata["result"]:
        #print (wid)
        wr = requests.get(url=RESTserver+"workflows/"+wid["id"])
        #print (wr.json())
        wdata = wr.json()["result"][0]
        data.append(wdata)
    #print (data)
    return render_template('workflows.html', title="MuPIFDB web interface", server=server, items=data)

@app.route('/workflows')
def worflows():
    r = requests.get(url=RESTserver+"workflows")
    rdata = r.json()
    #print (rdata)
    data = []
    for wid in rdata["result"]:
        #print (wid)
        wr = requests.get(url=RESTserver+"workflows/"+wid["id"])
        #print (wr.json())
        wdata = wr.json()["result"][0]
        data.append(wdata)
    #print (data)
    return render_template('workflows.html', title="MuPIFDB web interface", server=server, items=data)

@app.route('/workflows/<id>')
def workflow (id):
    wr = requests.get(url=RESTserver+"workflows/"+id)
    wdata = wr.json()["result"][0]
    return render_template('workflow.html', title="MuPIFDB web interface", server=server, 
    id=id, UseCase=wdata["UseCases"], Description = wdata["Description"], inputs=wdata["IOCard"]["Inputs"], outputs=wdata["IOCard"]["Outputs"])

@app.route('/workflowexecutions/init/<id>')
def initexecution(id):
    r = requests.get(url=RESTserver+"workflowexecutions/init/"+id)
    data = r.json()["result"]
    return render_template('workflowexecution.html', title="MuPIFDB web interface", server=server, wid=id, id = data, status=data)

@app.route('/workflowexecutions/<id>')
def executionStatus(id):
    r = requests.get(url=RESTserver+"workflowexecutions/"+str(id))
    data = r.json()["result"][0]
    return render_template('workflowexecution.html', title="MuPIFDB web interface", server=server, wid=data['WorkflowID'], id = id, status=data)

@app.route('/executeworkflow/<id>')
def executeworkflow(id):
    r = requests.get(url=RESTserver+'executeworkflow/'+id)
    r = requests.get(url=RESTserver+"workflowexecutions/"+str(id))
    data = r.json()["result"][0]
    return render_template('workflowexecution.html', title="MuPIFDB web interface", server=server, wid=data['WorkflowID'], id = id, status=data)



@app.route('/workflowexecutions/<id>/inputs', methods=('GET', 'POST'))
def setExecutionInputs(id):
    # get we record
    r = requests.get(url=RESTserver+"workflowexecutions/"+str(id))
    we=r.json()["result"][0]
    print(we)
    wid = we["WorkflowID"]
    print(wid)
    # get execution input record (to access inputs)
    r = requests.get(url=RESTserver+"workflowexecutions/"+id+'/inputs')
    inprec = r.json()["result"]
    print(inprec)

    if (request.form):
        #process submitted data
        msg = ""
        print (request.form.get('eid'))
        c=0
        payload = {}
        for i in inprec:
            name = i['Name']
            objID = i['ObjID']
            value = request.form['Value_%d'%c]
            units = i['Units']
            #source = request.form.getvalue('Source_%d'%c)
            #originID  = request.form.getvalue('OriginID_%d'%c)
            msg += 'Setting %s (ObjID %s) to %s [%s]</br>'%(name, objID, value, units)
            payload[name]=value
            #try:
            #    inp.set(name, value, objID)
            #    print (' OK<br>')
            #except Exception as e:
            #    print(' Failed<br>')
            #    print (e)
            #set source and origin 
            c = c+1
        r = requests.get(url=RESTserver+"workflowexecutions/"+id+'/set', params=payload)
        msg += str(r.json())
        msg += "</br><a href=\""+server+"workflowexecutions/"+id+"\">Continue to Execution record "+id+"</a>"
        return render_template("basic.html", body=Markup(msg))
    else:      
        # generate input form
        form = "<h3>Workflow: %s</h3>Input record for weid %s<table>"%(wid, id)
        form+="<tr><th>Name</th><th>Type</th><th>ObjID</th><th>Value</th><th>Unit</th></tr>"
        c = 0
        print("huhuh")
        for i in inprec:
            print(i)
            form += "<tr><td>%s</td><td>%s</td><td>%s</td><td><input type=\"text\" name=\"Value_%d\" value=\"%s\" /></td><td>%s</td></tr>"%(i['Name'], i['Type'],i['ObjID'],c, i['Value'], i.get('Units'))
            c+= 1
        form+="</table>"
        form += "<input type=\"hidden\" name=\"eid\" value=\"%s\"/>"%id
        form += "<input type=\"submit\" value=\"Submit\" />"
        print (form)
        return render_template('form.html', title="MuPIFDB web interface", form=form)





@app.route("/workflowexecutions/<id>/outputs")
def getExecutionOutputs(id):
    # get we record
    r = requests.get(url=RESTserver+"workflowexecutions/"+str(id))
    we=r.json()["result"][0]
    wid = we["WorkflowID"]
    # get execution input record (to access inputs)
    r = requests.get(url=RESTserver+"workflowexecutions/"+id+'/outputs')
    outrec = r.json()["result"]

    # generate result table form
    form = "<h3>Workflow: %s</h3>Output record for weid %s<table>"%(wid, id)
    form+="<tr><th>Name</th><th>Type</th><th>ObjID</th><th>Value</th><th>Unit</th></tr>"
    for i in outrec:
        print(i)
        form += "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"%(i['Name'], i['Type'],i['ObjID'], i['Value'], i.get('Units'))
    form+="</table>"
    form+="</br><a href=\""+server+"workflowexecutions/"+id+"\">Continue to Execution record "+id+"</a>"
    print (form)
    return render_template('basic.html', title="MuPIFDB web interface", body=Markup(form))


@app.route('/hello/<name>')
def hello(name=None):
    return render_template('hello.html', name=name, content="Welcome to MuPIFDB web interface")


if __name__ == '__main__':
    app.run(debug=True)