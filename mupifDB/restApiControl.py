import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")

from datetime import datetime
import requests


rest_api_url = 'http://127.0.0.1:5000/'


# --------------------------------------------------
# Workflows
# --------------------------------------------------

def getWorkflowRecord(wid):
    response = requests.get(rest_api_url + "workflows/" + wid)
    response_json = response.json()
    for record in response_json['result']:
        return record
    return None


def getWorkflowRecordFromHistory(wid, version):
    response = requests.get(rest_api_url + "workflowshistory/" + wid + "/" + version)
    response_json = response.json()
    for record in response_json['result']:
        return record
    return None


# --------------------------------------------------
# Executions
# --------------------------------------------------

def getExecutionRecord(weid):
    response = requests.get(rest_api_url + "workflowexecutions/" + weid)
    response_json = response.json()
    for record in response_json['result']:
        return record
    return None


def getScheduledExecutions():
    executions = []
    response = requests.get(rest_api_url + "workflowexecutions?Status=Scheduled")
    response_json = response.json()
    for record in response_json['result']:
        executions.append(record)
    return executions


def getPendingExecutions():
    executions = []
    response = requests.get(rest_api_url + "workflowexecutions?Status=Pending")
    response_json = response.json()
    for record in response_json['result']:
        executions.append(record)
    return executions


def setExecutionStatusScheduled(execution_id):
    response = requests.get(rest_api_url + "workflowexecutions/" + str(execution_id) + "/modify?Status=Scheduled&ScheduledDate=" + str(datetime.now()))


def setExecutionStatusRunning(execution_id):
    response = requests.get(rest_api_url + "workflowexecutions/" + str(execution_id) + "/modify?Status=Running&StartDate=" + str(datetime.now()))


def setExecutionStatusFinished(execution_id, log_id):
    response = requests.get(rest_api_url + "workflowexecutions/" + str(execution_id) + "/modify?Status=Finished&EndDate=" + str(datetime.now()) + "&ExecutionLog=" + str(log_id))


def setExecutionStatusFailed(execution_id, log_id):
    response = requests.get(rest_api_url + "workflowexecutions/" + str(execution_id) + "/modify?Status=Failed&EndDate=" + str(datetime.now()) + "&ExecutionLog=" + str(log_id))


# --------------------------------------------------
# Stat
# --------------------------------------------------

def getStatScheduler():
    response = requests.get(rest_api_url + "stat")
    response_json = response.json()
    keys = ["runningTasks", "scheduledTasks", "load", "processedTasks"]
    for k in keys:
        if k not in response_json['result']:
            response_json['result'][k] = None
    return response_json['result']


def setStatScheduler(runningTasks=None, scheduledTasks=None, load=None, processedTasks=None):
    if runningTasks is not None:
        response = requests.get(rest_api_url + "stat/set?scheduler.runningTasks=" + str(runningTasks))
    if scheduledTasks is not None:
        response = requests.get(rest_api_url + "stat/set?scheduler.scheduledTasks=" + str(scheduledTasks))
    if load is not None:
        response = requests.get(rest_api_url + "stat/set?scheduler.load=" + str(load))
    if processedTasks is not None:
        response = requests.get(rest_api_url + "stat/set?scheduler.processedTasks=" + str(processedTasks))
