import copy
import importlib
import math
import zipfile
import tempfile
import json
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
from flask_login import (
    LoginManager,
    current_user,
    login_required,
    login_user,
    logout_user,
    UserMixin
)
import logging
log=logging.getLogger(__name__)
from oauthlib.oauth2 import WebApplicationClient
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017")
db = client.MuPIF

def fix_id(record):
    if record:
        if '_id' in record:
            record['_id'] = str(record['_id'])
    return record

def get_user(user_id: str):
    if user_id:
        res = db.Users.find_one({'id': user_id})
        if res:
            return fix_id(res)
    return None

def get_user_by_email(user_email: str, user_id: str):
    if user_email:
        res = db.Users.find_one({'email': user_email, 'id': None})
        if res:
            db.Users.update_one({'email': user_email, 'id': None}, { "$set": { 'id': user_id } })
            return fix_id(res)
    return None

def update_user_picture_url(user_id: str, val):
    db.Users.update_one({'id': user_id}, { "$set": { 'profile_pic': val } })

def update_user_name(user_id: str, val):
    db.Users.update_one({'id': user_id}, { "$set": { 'name': val } })

login_config = {}
googleConfigPath = os.path.expanduser("/var/lib/mupif/persistent/google_auth_config.json")
if os.path.exists(googleConfigPath):
    with open(googleConfigPath) as config_json:
        login_config = json.load(config_json)
else: log.error("File '{googleConfigPath}' does not exist, login will be broken.")
GOOGLE_CLIENT_ID = login_config.get("GOOGLE_CLIENT_ID", None)
GOOGLE_CLIENT_SECRET = login_config.get("GOOGLE_CLIENT_SECRET", None)
GOOGLE_REDIRECT_URI = login_config.get("GOOGLE_REDIRECT_URI", None)
GOOGLE_DISCOVERY_URL = login_config.get("GOOGLE_DISCOVERY_URL", None)
AUTH_APP_SECRET_KEY = login_config.get("AUTH_APP_SECRET_KEY", None) or os.urandom(24)

path_of_this_file = os.path.dirname(os.path.abspath(__file__))

sys.path.append(path_of_this_file+"/..")
sys.path.append(path_of_this_file+"/.")
sys.path.append(path_of_this_file+"/../mupifDB")


from mupifDB import restApiControl
import mupifDB


app = Flask(__name__)
app.secret_key = AUTH_APP_SECRET_KEY
CORS(app, resources={r"/static/*": {"origins": "*"}})
login_manager = LoginManager()
login_manager.init_app(app)
client = WebApplicationClient(GOOGLE_CLIENT_ID)


class User(UserMixin):
    def __init__(self, id_, email, name, profile_pic, rights):
        self.id = id_
        self.email = email
        self.name = name
        self.profile_pic = profile_pic
        self.rights = rights
        super(User).__init__()

    @staticmethod
    def get(user_id, user_email=None):
        u = get_user(user_id)
        if u is None and user_email is not None:
            # for the first login by email to remember the user's id
            u = get_user_by_email(user_email, user_id)
            u = get_user(user_id)
        if u is not None:
            user = User(
                id_=u.get('id', None),
                name=u.get('name', 'Unknown'),
                email=u.get('email', 'Unknown'),
                profile_pic=u.get('profile_pic', 'Unknown'),
                rights=u.get('Rights', 0)
            )
            return user
        return None


@login_manager.user_loader
def load_user(user_id):
    return User.get(user_id)


# unless overridden by the environment, use 127.0.0.1:8005
RESTserver = os.environ.get('MUPIFDB_REST_SERVER', "http://127.0.0.1:8005/")
RESTserver = RESTserver.replace('5000', '8005')

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
    if current_user.is_authenticated:
        return current_user.rights
    return 0


def getUserHasAdminRights():
    return True if getRightsOfCurrentUser() >= 10 else False


def my_render_template(*args,**kw):
    'Wraps render_template and ads a few common keywords'
    return render_template(*args,title='MuPIFDB web interface',server=request.host_url,RESTserver=RESTserver,**kw)


def login_header_html():
    html = ''
    if current_user.is_authenticated:
        html += f'<div style="display:flex;flex-direction: column;gap: 2px;align-items: flex-start;"><div style="font-size: 12px;line-height: 14px;">{current_user.name}</div><div style="font-size: 12px;line-height: 14px;">{current_user.email}</div><a style="font-size: 12px;line-height: 14px;border: 1px solid gray;background-color:silver;color: black;text-decoration: none;border-radius: 3px;padding: 3px 6px;" href="/logout">Logout</a></div>'
        html = '<div style="display:flex;flex-direction: row; gap: 10px;">' + html + f'<img src="{current_user.profile_pic}" style="height: 54px;border-radius:27px;"></div>'
    else:
        html += '<a style="font-size: 12px;line-height: 14px;border: 1px solid gray;background-color:silver;color: black;text-decoration: none;border-radius: 3px;padding: 3px 6px;" href="/login">Login'+(' <b>(unavailable)</b> ' if GOOGLE_DISCOVERY_URL is None else '')+'</a>'
    return Markup(html)


@app.route('/')
def homepage():
    html = "<h3>Welcome to MuPIFDB web interface</h3><br>"
    if not current_user.is_authenticated:
        html += '<p>You need to authenticate to use this application.</p>'

    return render_template('basic.html', title="MuPIFDB web interface", body=Markup(html), login=login_header_html())


@app.login_manager.unauthorized_handler
def unauth_handler():
    html = '<p>You need to authenticate with </p><a class="button" href="/login">Google Login</a>'
    return render_template('basic.html', title="MuPIFDB web interface", body=Markup(html), login=login_header_html())


def get_google_provider_cfg():
    return requests.get(GOOGLE_DISCOVERY_URL).json()

@app.route("/login")
def login():
    if GOOGLE_DISCOVERY_URL is None:
        return '<HTML><head><title>NOT IMPLEMENTED</title></head><body><h1>Login not configured</h1><p>This instance of MuPIF container is missing login configuration, it is a problem on the server side. Sorry.</p></body></HTML>',501
    google_provider_cfg = get_google_provider_cfg()
    authorization_endpoint = google_provider_cfg["authorization_endpoint"]
    request_uri = client.prepare_request_uri(
        authorization_endpoint,
        redirect_uri=GOOGLE_REDIRECT_URI,#request.base_url
        scope=["openid", "email", "profile"],
    )
    return redirect(request_uri)

@app.route("/login/callback")
def callback():
    code = request.args.get("code")
    google_provider_cfg = get_google_provider_cfg()
    token_endpoint = google_provider_cfg["token_endpoint"]
    token_url, headers, body = client.prepare_token_request(
        token_endpoint,
        authorization_response=request.url,
        redirect_url=GOOGLE_REDIRECT_URI,  # request.base_url str(GOOGLE_REDIRECT_URI)
        code=code
    )
    token_response = requests.post(
        token_url,
        headers=headers,
        data=body,
        auth=(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET),
    )

    client.parse_request_body_response(json.dumps(token_response.json()))

    userinfo_endpoint = google_provider_cfg["userinfo_endpoint"]
    uri, headers, body = client.add_token(userinfo_endpoint)
    userinfo_response = requests.get(uri, headers=headers, data=body)
    user = None
    if userinfo_response.json().get("email_verified"):
        unique_id = userinfo_response.json()["sub"]
        users_email = userinfo_response.json()["email"]
        picture = userinfo_response.json()["picture"]
        users_name = userinfo_response.json()["name"]
        if unique_id is not None and len(unique_id) > 0:
            user = User.get(unique_id, users_email)
            if user is not None and user.id == unique_id:
                login_user(user)
                if user.name != users_name:
                    update_user_name(unique_id, users_name)
                    login_user(user)
                if user.profile_pic != picture:
                    update_user_picture_url(unique_id, picture)
                    login_user(user)

    if user is None:
        return "User email not available or not verified by Google.", 400

    # return redirect("/")

    return homepage()


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect("/")


@app.route('/about')
def about():
    msg = """
        <h3>Welcome to MuPIFDB web interface</h3>
        <p><a href=\"http://www.mupif.org\">MuPIF</a> is open-source, modular, object-oriented integration platform allowing to create complex, distributed, multiphysics simulation workflows across the scales and processing chains by combining existing simulation tools. <a href=\"https://github.com/mupif/mupifDB\">MuPIFDB</a> is database layer (based on MongoDB) and workflow manager/scheduler for MuPIF with REST API.</p>
        <p>The MuPIFDB web interface allows to use MupifDB REST API from web browser in a user friendly way, allowing to inspect all the stored data and to intialize, schedule and monitor individual workflow executions.</p> 
    """
    return my_render_template('basic.html', body=Markup(msg), login=login_header_html())


@app.route('/status')
@login_required
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
    return my_render_template('stat.html', body=Markup(msg), login=login_header_html())


@app.route("/schedulerStats/weekly.svg")
@login_required
def schedulerStatWeekly():
    # https://stackoverflow.com/questions/67591467/flask-shows-typeerror-send-from-directory-missing-1-required-positional-argum
    # https://flask.palletsprojects.com/en/2.0.x/api/#flask.send_from_directory
    return send_from_directory(directory=path_of_this_file + "/static/images", path="scheduler_weekly_stat.svg")


@app.route("/schedulerStats/hourly.svg")
@login_required
def schedulerStatHourly():
    return send_from_directory(directory=path_of_this_file + "/static/images", path="scheduler_hourly_stat.svg")


@app.route('/contact')
def contact():
    msg = """
        <p>MuPIF and MuPIFDB have been developped at <a href=\"https://www.cvut.cz/en\">Czech Technical University in Prague</a> by a research team at the Department of Mechanics of the <a href=\"https://web.fsv.cvut.cz/en/\">Faculty of Civil Engineering</a>.</p>
        <p>For more information and help please contact Borek Patzak (borek.patzak@fsv.cvut.cz)</p>  
    """
    return my_render_template('basic.html', body=Markup(msg), login=login_header_html())


@app.route('/usecases')
@login_required
def usecases():
    admin_rights = getUserHasAdminRights()

    data = restApiControl.getUsecaseRecords()

    html = '<h3>UseCases:</h3>'
    html += '<table class=\"tableType1\">'
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
    return my_render_template('basic.html', body=Markup(html), login=login_header_html())


@app.route('/usecase_add', methods=('GET', 'POST'))
@login_required
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
            return my_render_template('basic.html', body=Markup(html), login=login_header_html())
        else:
            message += '<h5 style="color:red;">UseCase was not registered</h5>'
    if new_usecase_id is None:
        html = message
        html += "<h3>Add new UseCase:</h3>"
        if admin_rights:
            html += "<table class=\"tableType1\">"
            html += '<tr><td>UseCase ID (string)</td><td><input type="text" name="usecase_id" value="'+str(usecase_id)+'"></td></tr>'
            html += '<tr><td>UseCase Description (string)</td><td><input type="text" name="usecase_description" value="'+str(usecase_description)+'"></td></tr>'
            html += "</table>"
            html += "<input type=\"submit\" value=\"Submit\" />"
        else:
            html += "<h5>You don't have permission to visit this page.</h5>"
        return my_render_template('form.html', form=html, login=login_header_html())


@app.route('/usecases/<ucid>/workflows')
@login_required
def usecaseworkflows(ucid):
    data = restApiControl.getWorkflowRecordsWithUsecase(ucid)
    return my_render_template('workflows.html', items=data, login=login_header_html())


@app.route('/workflows')
@login_required
def worflows():
    data = restApiControl.getWorkflowRecords()
    return my_render_template('workflows.html', items=data, login=login_header_html())


@app.route('/workflows/<wid>')
@login_required
def workflowNoVersion(wid):
    return workflow(wid, -1)


@app.route('/workflows/<wid>/<version>')
@login_required
def workflow(wid, version):
    wdata = restApiControl.getWorkflowRecordGeneral(wid=wid, version=int(version))
    html = '<table class=\"tableType1\">'
    html += '<tr><td>WorkflowID:</td><td>'+str(wdata['wid'])+'</td></tr>'
    html += '<tr><td>Version:</td><td>'+str(wdata['Version'])+'</td></tr>'
    html += '<tr><td>UseCase:</td><td>'+str(wdata['UseCase'])+'</td></tr>'
    html += '<tr><td>Description:</td><td>'+str(wdata['Description'])+'</td></tr>'
    html += '</table>'

    html += '<br><a href="/workflowexecutions/init/'+str(wid)+'/'+str(wdata['Version'])+'">Initialize new execution record</a>'
    if len(wdata.get('EDMMapping', [])):
        html += '<a style="margin-left:50px;" href="/workflowexecutions/init/' + str(wid) + '/' + str(wdata['Version']) + '?no_onto">Optionally without EDM objects</a>'
    html += '<br><br>Inputs'
    html += '<table class=\"tableType1\">'
    html += '<thead><th>Name</th><th>Type</th><th>TypeID</th><th>Description</th><th>Units</th><th>ObjID</th><th>Compulsory</th><th>SetAt</th><th>EDMPath</th></thead>'
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
        html += '<td class="c7">'+str(item.get('EDMPath', ''))+'</td>'
        html += '</tr>'
    html += '</table>'

    html += '<br>Outputs'
    html += '<table class=\"tableType1\">'
    html += '<thead><th>Name</th><th>Type</th><th>TypeID</th><th>Description</th><th>Units</th><th>ObjID</th><th>EDMPath</th></thead>'
    for item in wdata["IOCard"]["Outputs"]:
        html += '<tr>'
        html += '<td class="c1">' + str(item.get('Name')) + '</td>'
        html += '<td class="c2">' + str(item.get('Type')) + '</td>'
        html += '<td class="c3">' + str(item.get('TypeID')) + '</td>'
        html += '<td class="c4">' + str(item.get('Description')) + '</td>'
        html += '<td class="c5">' + str(item.get('Units')) + '</td>'
        html += '<td class="c6">' + str(item.get('ObjID')) + '</td>'
        html += '<td class="c6">' + str(item.get('EDMPath', '')) + '</td>'
        html += '</tr>'
    html += '</table>'
    # html += '<br><br>All versions of this workflow:'
    html += ''
    html += ''

    OBO = wdata.get('EDMMapping', [])
    if len(OBO):
        html += "<br>EDM Mapping:"
        html += "<table class=\"tableType1\">"
        html += "<tr><th>Name</th><th>Type</th><th>DBName</th><th>createFrom</th></tr>"
        for obo in OBO:
            html += '<tr>'
            html += '<td>' + obo.get('Name', '') + '</td>'
            html += '<td>' + obo.get('EDMEntity', '') + '</td>'
            html += '<td>' + obo.get('DBName', '') + '</td>'
            html += '<td>' + obo.get('createFrom', '') + '</td>'
            html += '</tr>'
        html += "</table>"

    html += '<br><br><a href="/workflowexecutions?filter_workflow_id='+str(wdata['wid'])+'&filter_workflow_version='+str(wdata['Version'])+'">Executions of this workflow</a>'

    admin_rights = getUserHasAdminRights()
    if admin_rights:
        html += '<br><br><a href="'+RESTserver+'file/'+str(wdata['GridFSID'])+'" target="_blank">Download file</a>'

    return my_render_template('basic.html', body=Markup(html), login=login_header_html())


def allowed_file(filename):
    ALLOWED_EXTENSIONS = {'py'}
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/workflow_add/<usecaseid>', methods=('GET', 'POST'))
@login_required
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
        models_md = None
        EDM_Mapping = None
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
                                    EDM_Mapping = workflow_instance.getMetadata('EDMMapping', [])
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
                    models_md=models_md,
                    EDM_Mapping=EDM_Mapping
                )

    if new_workflow_id is not None:
        html = '<h3>Workflow has been registered</h3>'
        html += '<a href="/workflows/'+str(wid)+'">Go to workflow detail</a>'
        return my_render_template('basic.html', body=Markup(html), login=login_header_html())
    else:
        # generate input form
        html = message
        html += "<h3>Add new workflow:</h3>"
        if admin_rights:
            html += "<h5>(The workflow module file should contain only one class implementation.):</h5>"
            html += "<table class=\"tableType1\">"

            html += '<input type="hidden" name="somedata" value="">'

            html += '<tr><td>Workflow module file</td><td><input type="file" name="file_workflow"></td></tr>'
            for add_file in range(1, 6):
                html += '<tr><td>Additional file #%d</td><td><input type="file" name="file_add_%d"></td></tr>' % (add_file, add_file)

            html += "</table>"
            html += "<input type=\"submit\" value=\"Submit\" />"
        else:
            html += "<h5>You don't have permission to visit this page.</h5>"
        return my_render_template('form.html', form=html, login=login_header_html())


@app.route('/workflowexecutions')
@login_required
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

    html = '<h3>List of workflow executions</h3>'
    html += '<form id="filtering_form" action="" style="font-size:12px;" method="get">'
    html += 'WorkflowID: <input type="text" name="filter_workflow_id" value="' + filter_workflow_id + '" style="width:100px;"> '
    html += 'version: <input type="text" name="filter_workflow_version" value="' + filter_workflow_version + '" style="width:20px;"> '
    html += 'label: <input type="text" name="filter_label" value="' + filter_label + '" style="width:100px;"> '
    html += 'status: <select name="filter_status">'
    html += '<option value="">Any</option>'
    status_list = ['Created', 'Pending', 'Scheduled', 'Running', 'Finished', 'Failed']
    for st in status_list:
        selected = ' selected' if filter_status == st else ''
        html += '<option value="' + st + '"' + selected + '>' + st + '</option>'
    html += '</select> '
    html += 'limit: <input type="text" name="filter_num_lim" value="' + filter_num_lim + '" style="width:40px;"> '
    html += '<input type="submit" value="filter">'
    html += '</form><br>'

    html += '<table class=\"tableType1\"><tr><td>Status</td><td></td><td>Workflow</td><td>CreatedDate</td><td>StartDate</td><td>EndDate</td></tr>'
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
        # html += '<td style="font-size:12px;">'+str(execution['SubmittedDate']).replace('None', '')[:19]+'</td>'
        html += '<td style="font-size:12px;">'+str(execution['StartDate']).replace('None', '')[:19]+'</td>'
        html += '<td style="font-size:12px;">'+str(execution['EndDate']).replace('None', '')[:19]+'</td>'
        html += '</tr>'

    html += '</table>'
    return my_render_template('basic.html', body=Markup(html), login=login_header_html())


@app.route('/workflowexecutions/init/<wid>/<version>')
@login_required
def initexecution(wid, version, methods=('GET')):
    disable_onto = 'no_onto' in request.args
    we_record = restApiControl.getWorkflowRecordGeneral(wid, int(version))
    if we_record is not None:
        weid = restApiControl.createExecution(wid, int(version), ip=getUserIPAddress(), no_onto=disable_onto)
        return redirect(url_for("executionStatus", weid=weid))
    else:
        return my_render_template('basic.html', body=Markup('<h5>Workflow with given ID and version was not found.</h5>'), login=login_header_html())


@app.route('/workflowexecutions/<weid>')
@login_required
def executionStatus(weid):
    data = restApiControl.getExecutionRecord(weid)
    logID = data.get('ExecutionLog')
    html = ''
    html += '<script type="text/javascript">window.execution_id = "' + weid + '";</script>'
    html += '<script type="text/javascript" src="/main.js"></script>'
    if data['Status'] == 'Pending' or data['Status'] == 'Running' or data['Status'] == 'Scheduled':
        html += '<script type="text/javascript">let timer_refresh = setInterval(reloadIfExecStatusIsChanged, 15000);</script>'
    html += '<table style="font-size:14px;" class=\"tableType1\">'
    html += '<tr><td>Execution ID:</td><td>' + str(weid) + '</td></tr>'
    html += '<tr><td>Workflow ID:</td><td>' + str(data['WorkflowID']) + '</td></tr>'
    html += '<tr><td>Workflow version:</td><td>' + str(data['WorkflowVersion']) + '</td></tr>'
    html += '<tr><td>Task ID:</td><td>' + str(data['Task_ID']) + '</td></tr>'
    html += '<tr><td>Label:</td><td>' + str(data['label']) + '</td></tr>'
    html += '<tr><td>E-mail address:</td><td>' + str(data['RequestedBy']) + '</td></tr>'

    html += '<tr><td colspan="2" style="height:10px;"></td></tr>'

    html += '<tr><td>Status:</td><td>' + str(data['Status']) + '</td></tr>'
    html += '<tr><td>Start Date:</td><td>' + str(data['StartDate']).replace('None', '')[:19] + '</td></tr>'
    html += '<tr><td>End Date:</td><td>' + str(data['EndDate']).replace('None', '')[:19] + '</td></tr>'
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
        html += '<li> <a href="' + RESTserver + 'file/' + str(logID) + '"> Execution log</a></li>'
    html += '</ul>'

    return my_render_template('basic.html', body=Markup(html), login=login_header_html())


@app.route('/executeworkflow/<weid>')
@login_required
def executeworkflow(weid):
    restApiControl.scheduleExecution(weid)
    data = restApiControl.getExecutionRecord(weid)
    logID = data['ExecutionLog']
    return redirect(url_for("executionStatus", weid=weid))


@app.route('/workflowexecutions/<weid>/inputs', methods=('GET', 'POST'))
@login_required
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
                if i.get('EDMPath', None) is None:
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
                            valuetype = i.get('ValueType', 'Scalar')
                            strval = str(value)
                            if valuetype == 'Vector':
                                if strval.find('[') >= 0:
                                    strval = literal_eval(strval)
                                else:
                                    strval = strval.split(' ')

                            object_dict = {
                                'ClassName': 'String',
                                'DataID': i['TypeID'].replace('mupif.DataID.', ''),
                                'Value': strval,
                                'ValueType': valuetype
                            }
                            restApiControl.setExecutionInputObject(weid, name, objID, object_dict)
                        else:
                            print("Unknown data type")

                c = c+1

            OBO = execution_record.get('EDMMapping', [])
            for obo in OBO:
                if obo.get('createFrom', None) is None:
                    obo_id = request.form.get('obo_id_' + obo.get('Name', ''), None)
                    if obo_id is not None:
                        restApiControl.setExecutionOntoBaseObjectID(weid, name=obo.get('Name', ''), value=obo_id)

            msg += "</br><a href=\"/workflowexecutions/"+weid+"\">Back to Execution detail</a>"
            # return my_render_template("basic.html", body=Markup(msg), login=login_header_html())

    execution_record = restApiControl.getExecutionRecord(weid)
    wid = execution_record["WorkflowID"]
    execution_inputs = restApiControl.getExecutionInputRecord(weid)
    workflow_record = restApiControl.getWorkflowRecord(wid)
    winprec = workflow_record["IOCard"]["Inputs"]
    # generate input form
    form = "<a href=\"/workflowexecutions/"+weid+"\">Back to Execution detail</a><br>"

    # form += f"<h3>Execution inputs: {wid}</h3><br>"

    form += "<table style=\"margin-top: 16px;\" class=\"tableType1\">"
    form += "<tr>"
    form += "<td>Workflow_ID:</td>"
    form += "<td>"
    form += str(wid)
    form += "</td>"
    form += "</tr>"

    form += "<tr>"
    form += "<td>Task_ID:</td>"
    form += "<td>"
    if execution_record["Status"] == "Created":
        form += "<input type=\"text\" name=\"Task_ID\" value=\"%s\" /><br>" % execution_record["Task_ID"]
    else:
        form += "%s<br>" % execution_record["Task_ID"]
    form += "</td>"
    form += "</tr>"

    form += "<td>Label:</td>"
    form += "<td>"
    if execution_record["Status"] == "Created":
        form += "<input type=\"text\" name=\"label\" value=\"%s\" /><br>" % execution_record["label"]
    else:
        form += "%s<br>" % execution_record["label"]
    form += "</td>"
    form += "</tr>"

    form += "<td>E-mail address:</td>"
    form += "<td>"
    if execution_record["Status"] == "Created":
        form += f"<input type=\"text\" name=\"RequestedBy\" value=\"{execution_record['RequestedBy']}\" /><br>"
    else:
        form += f"{execution_record['RequestedBy']}<br>"
    form += "</td>"
    form += "</tr>"

    form += "</table>"

    args = {}
    for key, value in request.args.items():
        args[key] = value
    any_edm_path = False
    any_execution_link = False
    for i in execution_inputs:
        if i.get('EDMPath', None):
            any_edm_path = True
        if i.get('Link', {}).get('ExecID', None) or i.get('Link', {}).get('Name', None) or i.get('Link', {}).get('ObjID', None):
            any_execution_link = True

    show_execution_links = any_execution_link or args.get('show_execution_links', False)

    form += "<h3>Execution inputs</h3>"
    form += "<table class=\"tableType1\">"
    form += "<tr><th>Type</th><th>ValueType</th><th>DataID</th><th>Value</th><th>Units</th><th>Name</th><th>Description</th><th>ObjID</th>"
    if show_execution_links:
        form += "<th>Link_EID</th><th>Link_Name</th><th>Link_ObjID</th>"
    if any_edm_path:
        form += "<th>EDMPath</th>"
    form += "</tr>"

    c = 0
    for i in execution_inputs:
        name = i['Name']
        # get description from workflow rec
        description = ""
        for ii in winprec:
            # print(ii)
            if ii["Name"] == name:
                description = ii.get("Description")
                break

        input_type = i['Type']
        if i.get('Compulsory', False):
            required = "required"
        else:
            required = ""

        form += '<tr>'
        form += '<td>' + str(i['Type']).replace('mupif.', '') + '</td>'
        form += '<td>' + str(i.get('ValueType', '')) + '</td>'
        form += '<td>' + str(i.get('TypeID', '[unknown]')).replace('mupif.DataID.', '') + '</td>'

        if input_type == "mupif.Property":

            form += '<td>'
            if execution_record["Status"] == "Created" and i.get('EDMPath', None) is None:
                try:
                    prop = mupif.ConstantProperty.from_db_dict(i['Object'])
                    ival = prop.quantity.inUnitsOf(i['Units']).value.tolist()
                except:
                    ival = None
                form += "<input type=\"text\" name=\"Value_%d\" value=\"%s\" %s/>" % (c, str(ival), required)
            else:
                if i.get('EDMPath', None) is not None:
                    onto_path = i.get('EDMPath')
                    onto_base_objects = execution_record.get('EDMMapping', [])

                    splitted = onto_path.split('.', 1)
                    base_object_name = splitted[0]
                    object_path = splitted[1]

                    # find base object info
                    info = {}
                    for ii in onto_base_objects:
                        if ii['Name'] == base_object_name:
                            info = ii

                    # get the desired object
                    onto_data = restApiControl.getEDMData(info.get('DBName', ''), info.get('EDMEntity', ''), info.get('id', ''), object_path)
                    if onto_data is not None and type(onto_data) is dict:
                        value = onto_data.get('value', None)
                        unit = onto_data.get('unit', '')
                        if value is not None:
                            form += "<input type=\"text\" name=\"Value_%d\" value=\"%s\" %s/>" % (c, str(value), required)
                            # form += str(value) + ' ' + str(unit)

                else:
                    if i['Object'].get('Value', None) is not None:
                        form += str(i['Object']['Value'])
            form += "</td>"

        elif input_type == "mupif.String":
            form += '<td>'
            if execution_record["Status"] == "Created" and i.get('EDMPath', None) is None:
                try:
                    prop = mupif.String.from_db_dict(i['Object'])
                    ival = prop.getValue()
                except:
                    if i.get('ValueType', 'Scalar') == 'Vector':
                        ival = []
                    else:
                        ival = ''
                if i.get('ValueType', 'Scalar') == 'Vector':
                    sval = str(list(ival))
                else:
                    sval = str(ival)
                form += "<input type=\"text\" name=\"Value_%d\" value=\"%s\" %s/>" % (c, sval, required)
            else:
                if i.get('EDMPath', None) is not None:
                    onto_path = i.get('EDMPath')
                    onto_base_objects = execution_record.get('EDMMapping', [])

                    splitted = onto_path.split('.', 1)
                    base_object_name = splitted[0]
                    object_path = splitted[1]

                    # find base object info
                    info = {}
                    for ii in onto_base_objects:
                        if ii['Name'] == base_object_name:
                            info = ii

                    # get the desired object
                    onto_data = restApiControl.getEDMData(info.get('DBName', ''), info.get('EDMEntity', ''), info.get('id', ''), object_path)
                    if onto_data is not None and type(onto_data) is dict:
                        value = onto_data.get('value', None)
                        if value is not None:
                            form += str(value)
                else:
                    val = i['Object'].get('Value', None)
                    if val is not None:
                        if i.get('ValueType', 'Scalar') == 'Vector':
                            form += str(list(val))
                        else:
                            form += str(val)

            form += "</td>"

        else:
            form += '<td>' + str(i.get('Object', {}).get('Value', '')) + '</td>'

        form += '<td>' + str(i.get('Units')) + '</td>'
        form += '<td>' + str(i['Name']) + '</td>'

        form += '<td>' + str(description) + '</td>'
        form += '<td>' + str(i['ObjID']) + '</td>'

        if show_execution_links:
            if execution_record["Status"] == "Created" and i.get('EDMPath', None) is None:
                form += "<td><input type=\"text\" name=\"c_eid_%d\" value=\"%s\" style=\"width:100px;\" /></td>" % (c, i['Link']['ExecID'])
                form += "<td><input type=\"text\" name=\"c_name_%d\" value=\"%s\" style=\"width:60px;\" /></td>" % (c, i['Link']['Name'])
                form += "<td><input type=\"text\" name=\"c_objid_%d\" value=\"%s\" style=\"width:60px;\" /></td>" % (c, i['Link']['ObjID'])
            else:
                form += "<td>" + str(i['Link']['ExecID']) + "</td>"
                form += "<td>" + str(i['Link']['Name']) + "</td>"
                form += "<td>" + str(i['Link']['ObjID']) + "</td>"

        if any_edm_path:
            form += '<td>' + str(i.get('EDMPath', '')).replace('None', '') + '</td>'

        form += "</tr>"
        c += 1

    form += "</table>"
    form += f"<input type=\"hidden\" name=\"eid\" value=\"{weid}\"/>"

    OBO = execution_record.get('EDMMapping', [])
    if len(OBO):
        form += "<br>EDM Base Objects:"
        form += "<table class=\"tableType1\">"
        form += "<tr><th>Name</th><th>Type</th><th>DBName</th><th>ID</th><th>createFrom</th><th>inspect</th></tr>"
        for obo in OBO:

            form += '<tr>'
            form += '<td>' + obo.get('Name', '') + '</td>'
            form += '<td>' + obo.get('EDMEntity', '') + '</td>'
            form += '<td>' + obo.get('DBName', '') + '</td>'
            obo_id = obo.get('id', '')
            obo_ids = obo.get('ids', [])
            if obo_id is None:
                obo_id = ''
            if obo_ids is None:
                obo_ids = []
            if execution_record.get('Status') == 'Created' and obo.get('createFrom', None) is None and obo.get('createNew', None) is None:
                if obo.get('EDMList', False) is False:
                    # form += '<td><input type="text" value="' + obo_id + '" name="obo_id_' + obo.get('Name', '') + '"></td>'
                    form += '<td>'
                    form += '<select name="obo_id_' + obo.get('Name', '') + '" onchange="this.form.submit()">'
                    form += '<option value="">-</option>'
                    options = restApiControl.getEDMEntityIDs(obo.get('DBName', ''), obo.get('EDMEntity', ''), obo.get('OptionsFilter', None))
                    if len(options) > 100:
                        options = options[0:100]
                    for option in options:
                        form += '<option value="' + option + '" ' + ('selected' if obo_id == option else '') + '>' + option + '</option>'
                    form += '</select>'
                    form += '</td>'
            else:
                if obo.get('EDMList', False) is False:
                    form += '<td>' + obo_id + '</td>'
                else:
                    if len(obo_ids) <= 10:
                        form += '<td>' + str(obo_ids) + '</td>'
                    else:
                        form += '<td>' + str(obo_ids[0:5]) + ' ...(total ' + str(len(obo_ids)) + ')</td>'
            form += '<td>' + obo.get('createFrom', '') + '</td>'
            form += '<td>'
            if obo_id != '':
                form += '<a href="/entity_browser/' + obo.get('DBName', '') + '/' + obo.get('EDMEntity', '') + '/' + obo_id + '/" target="_blank">inspect</a>'
            form += '</td>'
            form += '</tr>'
        form += "</table>"

    if execution_record["Status"] == "Created":
        form += "<br><input type=\"submit\" value=\"Save\" />"
        if show_execution_links:
            form += f"<br><br><a href=\"/workflowexecutions/{weid}/inputs\">Hide execution output links</a>"
        else:
            form += f"<br><br><a href=\"/workflowexecutions/{weid}/inputs?show_execution_links=1\">Show execution output links</a>"


    return my_render_template('form.html', form=form, login=login_header_html())


@app.route("/workflowexecutions/<weid>/outputs")
@login_required
def getExecutionOutputs(weid):
    execution_record = restApiControl.getExecutionRecord(weid)
    wid = execution_record["WorkflowID"]
    execution_outputs = restApiControl.getExecutionOutputRecord(weid)
    workflow_record = restApiControl.getWorkflowRecord(wid)
    # winprec = workflow_record["IOCard"]["Outputs"]

    # generate result table form

    form = "<a href=\"/workflowexecutions/" + weid + "\">Back to Execution detail</a>"

    form += "<h3>Execution outputs</h3>"
    form += "<table class=\"tableType1\">"
    form += "<tr><th>Type</th><th>ValueType</th><th>DataID</th><th>Value</th><th>Units</th><th>Name</th><th>ObjID</th><th>EDMPath</th></tr>"
    for i in execution_outputs:
        val = ''

        if i['Type'] == 'mupif.Property':
            if i['Object'].get('FileID') is not None and i['Object'].get('FileID') != '':
                val = '<a href="/property_array_view/' + str(i['Object'].get('FileID')) + '/1">link</a>'
            else:
                if i.get('EDMPath', None) is not None:
                    onto_path = i.get('EDMPath')
                    onto_base_objects = execution_record.get('EDMMapping', [])

                    splitted = onto_path.split('.', 1)
                    base_object_name = splitted[0]
                    object_path = splitted[1]

                    # find base object info
                    info = {}
                    for ii in onto_base_objects:
                        if ii['Name'] == base_object_name:
                            info = ii

                    # get the desired object
                    onto_data = restApiControl.getEDMData(info.get('DBName', ''), info.get('EDMEntity', ''), info.get('id', ''), object_path)
                    if onto_data is not None:
                        value = onto_data.get('value', None)
                        unit = onto_data.get('unit', '')
                        if value is not None:
                            val = str(value)# + ' ' + str(unit)
                else:
                    try:
                        prop = mupif.ConstantProperty.from_db_dict(i['Object'])
                        val = prop.inUnitsOf(i.get('Units', '')).getValue()
                    except:
                        pass

        if i['Type'] == 'mupif.TemporalProperty':
            val = '[...]'

        if i['Type'] == 'mupif.String':
            if i.get('EDMPath', None) is not None:
                onto_path = i.get('EDMPath')
                onto_base_objects = execution_record.get('EDMMapping', [])

                splitted = onto_path.split('.', 1)
                base_object_name = splitted[0]
                object_path = splitted[1]

                # find base object info
                info = {}
                for ii in onto_base_objects:
                    if ii['Name'] == base_object_name:
                        info = ii

                # get the desired object
                onto_data = restApiControl.getEDMData(info.get('DBName', ''), info.get('EDMEntity', ''), info.get('id', ''), object_path)
                if onto_data is not None:
                    value = onto_data.get('value', None)
                    if value is not None:
                        val = str(value)
            else:
                try:
                    prop = mupif.String.from_db_dict(i['Object'])
                    val = prop.getValue()
                except:
                    pass

        if i['Type'] == 'mupif.Field':
            try:
                if i['Object'].get('FileID') is not None and i['Object'].get('FileID') != '':
                    val = '<a href="' + RESTserver + 'field_as_vtu/' + str(i['Object'].get('FileID')) + '">file.vtu</a>'
            except:
                pass

        if i['Type'] == 'mupif.PyroFile':
            try:
                if i['Object'].get('FileID') is not None and i['Object'].get('FileID') != '':
                    val = '<a href="' + RESTserver + 'file/' + str(i['Object'].get('FileID')) + '">download</a>'
            except:
                pass

        form += '<tr>'
        form += '<td>' + str(i['Type']) + '</td>'
        form += '<td>' + str(i.get('ValueType', '')) + '</td>'
        form += '<td>' + str(i.get('TypeID', '[unknown]')).replace('mupif.DataID.', '') + '</td>'
        form += '<td>' + str(val) + '</td>'
        form += '<td>' + str(escape(i.get('Units'))) + '</td>'
        form += '<td>' + str(i['Name']) + '</td>'
        form += '<td>' + str(i['ObjID']) + '</td>'
        form += '<td>' + str(i.get('EDMPath', '')) + '</td>'
    form += "</table>"

    OBO = execution_record.get('EDMMapping', [])
    if len(OBO):
        form += "<br>EDM Base Objects:"
        form += "<table class=\"tableType1\">"
        form += "<tr><th>Name</th><th>Type</th><th>DBName</th><th>ID</th><th>createFrom</th><th>inspect</th></tr>"
        for obo in OBO:

            form += '<tr>'
            form += '<td>' + obo.get('Name', '') + '</td>'
            form += '<td>' + obo.get('EDMEntity', '') + '</td>'
            form += '<td>' + obo.get('DBName', '') + '</td>'
            obo_id = obo.get('id', '')
            if obo.get('EDMList', False) is True:
                obo_id = obo.get('ids', [])
            # if obo_id is None:
            #     obo_id = ''
            form += '<td>' + str(obo_id) + '</td>'
            form += '<td>' + obo.get('createFrom', '') + '</td>'
            if obo.get('EDMList', False) is True:
                form += '<td></td>'
            else:
                form += '<td><a href="/entity_browser/' + obo.get('DBName', '') + '/' + obo.get('EDMEntity', '') + '/' + obo_id + '/" target="_blank">inspect</a></td>'
            form += '</tr>'
        form += "</table>"

    return my_render_template('basic.html', body=Markup(form), login=login_header_html())


@app.route("/entity_browser/<DB>/<Name>/<ID>/")
@login_required
def entity_browser(DB, Name, ID):
    obj = restApiControl.getEDMData(DB, Name, ID, '')
    html = json.dumps(obj, indent=4)
    html = html.replace('\r\n', '<br>').replace('\n', '<br>').replace('\r', '<br>').replace('  "', '  "<b>').replace('":', '</b>":').replace(' ', '&nbsp;')
    return my_render_template('basic.html', body=Markup(html), login=login_header_html())


@app.route("/property_array_view/<file_id>/<page>")
@login_required
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

        html += '<table style="font-size:14px;" class=\"tableType1\">'
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

        html += '<table style="font-size:12px;margin-top:10px;" class=\"tableType1\">'
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
        html += '</table><br><br><br><br><br><br><br><br><br><br>'

    return my_render_template('basic.html', body=Markup(html), login=login_header_html())


@app.route('/main.js')
@login_required
def mainjs():
    return send_from_directory(directory='./', path='main.js')


@app.route('/api/')
@login_required
def restapi():
    full_url = str(request.url)
    print(full_url)
    args_str = full_url.split('/api/?', 1)[1]
    full_rest_url = RESTserver + args_str
    print(full_rest_url)
    response = requests.get(full_rest_url)
    return jsonify(response.json())


@app.route('/workflow_check', methods=('GET', 'POST'))
@login_required
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

        return my_render_template('basic.html', body=Markup(html), login=login_header_html())
    else:
        # generate input form
        html = ''

        html += "<h4>Upload the workflow Python file (and consecutive Python modules):</h4>"
        html += "<h5>(The workflow module file should contain only one class implementation.):</h5>"
        html += "<table class=\"tableType1\">"

        html += '<input type="hidden" name="somedata" value="">'

        html += '<tr><td>Workflow module file</td><td><input type="file" name="file_workflow"></td></tr>'
        for add_file in range(1, 6):
            html += '<tr><td>Additional file #%d</td><td><input type="file" name="file_add_%d"></td></tr>' % (add_file, add_file)

        html += "</table>"
        html += "<input type=\"submit\" value=\"Submit\" />"

        return my_render_template('form.html', form=html, login=login_header_html())


if __name__ == '__main__':
    app.run(debug=True)
