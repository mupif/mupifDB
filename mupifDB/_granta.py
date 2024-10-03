import tempfile
import json
import sys
import os
import logging
import importlib
from typing import List,Optional,Literal


from .restApiControl import api_type, RESTserver, getAuthToken, rGet, rPost, rPatch, rPut, rDelete
from .restApiControl import getWorkflowRecordGeneral, getExecutionRecord
from . import table_structures, models

def fix_json(val):

    import re
    val = re.sub(r",[ \t\r\n]+}", "}", val)
    val = re.sub(r",[ \t\r\n]+\]", "]", val)
    val = val.replace("False", "false").replace("True", "true")
    val
    return val





def _getGrantaWorkflowRecordGeneral(wid, version: int):
    url = RESTserver + 'templates/' + str(wid)
    token = getAuthToken()
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8', 'Authorization': f'Bearer {token["access_token"]}'}
    r = rGet(url=url, headers=headers)
    r_json = r.json()
    workflow = table_structures.extendRecord({}, table_structures.tableWorkflow)
    workflow['_id'] = r_json['guid']
    workflow['wid'] = r_json['guid']
    workflow['WorkflowVersion'] = 1

    workflow['modulename'] = 'unknown'
    workflow['classname'] = 'unknown'
    workflow['GridFSID'] = 'unknown'
    workflow['UseCase'] = ''
    workflow['metadata'] = ''

    fid = None
    gmds = r_json['metadata']

    for gmd in gmds:
        if gmd['name'] == 'muPIF metadata':
            md = json.loads(fix_json(gmd['value']))
            # print(md)
            workflow['metadata'] = md
            workflow['classname'] = md['ClassName']
            workflow['modulename'] = md['ModuleName']

        if gmd['name'] == 'workflow python file':
            fid = gmd['value']['url'].split('/')[-1]

    if fid:
        file, filename = _getGrantaBinaryFileByID(fid)
        workflow['GridFSID'] = fid
        workflow['modulename'] = filename.replace('.py', '')

    return workflow


def _getGrantaWorkflowMetadataFromDatabase(wid):
    workflow_record = getWorkflowRecordGeneral(wid, -1)
    return workflow_record.get('metadata', {})


def _getGrantaWorkflowMetadataFromFile(wid, key=None):
    workflow_record = getWorkflowRecordGeneral(wid, -1)
    if workflow_record.get('GridFSID', None) is not None:
        with tempfile.TemporaryDirectory(dir='/tmp', prefix='mupifDB') as tempDir:
            try:
                fc, fn = _getGrantaBinaryFileByID(workflow_record['GridFSID'])
                if fn.split('.')[-1] == 'py':
                    with open(tempDir + '/' + workflow_record['modulename'] + '.py', "wb") as f:
                        f.write(fc)
                        f.close()

                        moduleImport = importlib.import_module(workflow_record['modulename'])
                        workflow_class = getattr(moduleImport, workflow_record["classname"])
                        workflow = workflow_class()
                        if key is None:
                            return workflow.getAllMetadata()
                        else:
                            return workflow.getMetadata(key)
            except Exception as e:
                print(e)
                return {}
    return {}


def _getGrantaExecutionInputItem(eid, name):
    def ObjIDIsIterable(val):
        try:
            a = val[0]
            if not isinstance(val, str):
                return True
        except:
            return False

    url = RESTserver + 'executions/' + str(eid) + '/inputs'
    token = getAuthToken()
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8', 'Authorization': f'Bearer {token["access_token"]}'}
    r = rGet(url=url, headers=headers)

    for inp in r.json():
        if name == inp['name']:
            if inp['type'] == 'float':
                # fint units first :(
                execution_record = getExecutionRecord(eid)
                w_inputs = _getGrantaWorkflowMetadataFromDatabase(execution_record['WorkflowID']).get('Inputs', [])
                units = ''
                data_id = 'Unknown'
                valueType = 'Unknown'
                for w_i in w_inputs:
                    if w_i['Name'] == name:
                        units = w_i.get('Units', 'Unknown')
                        data_id = w_i.get('Type_ID', 'Unknown')
                        valueType = w_i.get('ValueType', 'Scalar')
                return {
                    'Compulsory': True,
                    'Description': '',
                    'Name': inp['name'],
                    'ObjID': inp['name'],
                    'Type': 'mupif.Property',
                    'TypeID': data_id,
                    'Units': units,  # todo
                    'ValueType': valueType,
                    'Link': {},
                    'Object': {
                        'ClassName': 'ConstantProperty',
                        'ValueType': valueType,
                        'DataID': data_id.replace('mupif.DataID.', ''),
                        'Unit': units,  # todo
                        'Value': inp['value'],
                        'Time': None
                    }
                }

            if inp['type'] == 'str':
                # fint units first :(
                execution_record = getExecutionRecord(eid)
                w_inputs = _getGrantaWorkflowMetadataFromDatabase(execution_record['WorkflowID']).get('Inputs', [])
                units = ''
                data_id = 'Unknown'
                valueType = 'Unknown'
                for w_i in w_inputs:
                    if w_i['Name'] == name:
                        units = w_i.get('Units', 'Unknown')
                        data_id = w_i.get('Type_ID', 'Unknown')
                        valueType = w_i.get('ValueType', 'Scalar')
                return {
                    'Compulsory': True,
                    'Description': '',
                    'Name': inp['name'],
                    'ObjID': inp['name'],
                    'Type': 'mupif.String',
                    'TypeID': data_id,
                    'Units': units,  # todo
                    'ValueType': valueType,
                    'Link': {},
                    'Object': {
                        'ClassName': 'String',
                        'DataID': data_id.replace('mupif.DataID.', ''),
                        'Value': inp['value'],
                    'ValueType': valueType
                    }
                }

            if inp['type'] == 'hyperlink':
                execution_record = getExecutionRecord(eid)
                w_inputs = _getGrantaWorkflowMetadataFromDatabase(execution_record['WorkflowID']).get('Inputs', [])
                units = ''
                data_id = 'Unknown'
                valueType = 'Unknown'
                obj_type = ''
                for w_i in w_inputs:
                    if w_i['Name'] == name:
                        units = w_i.get('Units', 'Unknown')
                        data_id = w_i.get('Type_ID', 'Unknown')
                        valueType = w_i.get('ValueType', 'Scalar')
                        obj_type = w_i['Type']

                if obj_type == 'mupif.HeavyStruct':
                    return {
                        'Compulsory': True,
                        'Description': '',
                        'Name': inp['name'],
                        'ObjID': inp['name'],
                        'Type': 'mupif.HeavyStruct',
                        'TypeID': data_id,
                        'Units': '',
                        'ValueType': valueType,
                        'Link': {},
                        'Object': {
                            'FileID': inp['value']['url'].split('/')[-1]
                        }
                    }

                if obj_type == 'mupif.PyroFile':
                    return {
                        'Compulsory': True,
                        'Description': '',
                        'Name': inp['name'],
                        'ObjID': inp['name'],
                        'Type': 'mupif.PyroFile',
                        'TypeID': data_id,
                        'Units': '',
                        'ValueType': valueType,
                        'Link': {},
                        'Object': {
                            'FileID': inp['value'].split('/')[-1]
                        }
                    }

                if obj_type == 'mupif.Field':
                    return {
                        'Compulsory': True,
                        'Description': '',
                        'Name': inp['name'],
                        'ObjID': inp['name'],
                        'Type': 'mupif.Field',
                        'TypeID': data_id,
                        'Units': '',
                        'ValueType': valueType,
                        'Link': {},
                        'Object': {
                            'FileID': inp['value'].split('/')[-1]
                        }
                    }
    return None


def _getGrantaExecutionRecords(workflow_id=None, workflow_version=None, label=None, num_limit=None, status=None) -> List[models.WorkflowExecution_Model]: 
    assert api_type == 'granta'
    url = RESTserver + 'executions/'
    token = getAuthToken()
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8', 'Authorization': f'Bearer {token["access_token"]}'}
    r = rGet(url=url, headers=headers)
    res = []
    for ex in r.json():
        execution = table_structures.extendRecord({}, table_structures.tableExecution)
        execution['_id'] = ex['guid']
        execution['WorkflowID'] = ex['template_guid']
        execution['WorkflowVersion'] = -1
        execution['Status'] = {'Ready':'Pending','On-going':'Running','Completed':'Finished','Completed, to be reviewed':'Finished','Completed & reviewed':'Finished','Cancelled':'Failed'}.get(ex['status'],ex['status'])
        execution['Task_ID'] = ''
        res.append(execution)
    return res


def _getGrantaExecutionRecord(weid: str):
    assert api_type == 'granta'
    url = RESTserver + 'executions/' + str(weid)
    token = getAuthToken()
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8', 'Authorization': f'Bearer {token["access_token"]}'}
    r = rGet(url=url, headers=headers)
    r_json = r.json()
    execution = table_structures.extendRecord({}, table_structures.tableExecution)
    execution['_id'] = r_json['guid']
    execution['WorkflowID'] = r_json['template_guid']
    execution['WorkflowVersion'] = -1
    execution['Status'] = 'unknown'

    execution['Status'] = {'Ready':'Pending','On-going':'Running','Completed':'Finished','Completed, to be reviewed':'Finished','Completed & reviewed':'Finished','Cancelled':'Failed'}.get(r_json['status'],r_json['status'])

    execution['Task_ID'] = ''
    return execution

def _setGrantaExecutionParameter(execution_id, param, value, val_type="str"):
    assert api_type == 'granta'
    if param == 'ExecutionLog':
        url = RESTserver + 'executions/' + str(execution_id)
        token = getAuthToken()
        headers = {'content-type': 'application/json', 'charset': 'UTF-8', 'accept': 'application/json', 'Accept-Charset': 'UTF-8', 'Authorization': f'Bearer {token["access_token"]}'}
        newdata = {"logs": {"url": "https://musicode.grantami.com/musicode/filestore/%s" % str(value), "description": None}}
        r = rPatch(url=url, headers=headers, data=json.dumps(newdata))
        if r.status_code == 200:
            return True
    return None


def _getGrantaPendingExecutions(num_limit=None):
    assert api_type == 'granta'
    url = RESTserver + 'executions/?status=Ready'
    token = getAuthToken()
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8', 'Authorization': f'Bearer {token["access_token"]}'}
    r = rGet(url=url, headers=headers)
    res = []
    for ex in r.json():
        execution = table_structures.extendRecord({}, table_structures.tableExecution)
        execution['_id'] = ex['guid']
        execution['WorkflowID'] = ex['template_guid']
        execution['WorkflowVersion'] = -1
        execution['Status'] = 'Pending'
        execution['Task_ID'] = ''
        res.append(execution)
    return res


def _setGrantaExecutionResults(eid, val_list):
    url = RESTserver + 'executions/' + str(eid)
    token = getAuthToken()
    headers = {'content-type': 'application/json', 'charset': 'UTF-8', 'accept': 'application/json', 'Accept-Charset': 'UTF-8', 'Authorization': f'Bearer {token["access_token"]}'}
    newdata = {"results": val_list}
    r = rPatch(url=url, headers=headers, data=json.dumps(newdata))
    if r.status_code == 200:
        return True
    return False


def _setGrantaExecutionStatus(eid, val):
    url = RESTserver + 'executions/' + str(eid)
    token = getAuthToken()
    headers = {'content-type': 'application/json', 'charset': 'UTF-8', 'accept': 'application/json', 'Accept-Charset': 'UTF-8', 'Authorization': f'Bearer {token["access_token"]}'}
    newdata = {"status": str(val)}
    r = rPatch(url=url, headers=headers, data=json.dumps(newdata))
    if r.status_code == 200:
        return True
    return False





def _getGrantaBinaryFileByID(fid):
    assert api_type == 'granta'
    url = RESTserver.replace('/api/', '/filestore/')
    token = getAuthToken()
    headers = {'Authorization': f'Bearer {token["access_token"]}'}
    response = rGet(url=url + str(fid), headers=headers, allow_redirects=True)
    return response.content, response.headers['content-disposition'].split('filename=')[1].replace('"', '')

def _uploadGrantaBinaryFile(binary_data):
    assert api_type == 'granta'
    url = RESTserver.replace('/api/', '/filestore')
    token = getAuthToken()
    headers = {'Authorization': f'Bearer {token["access_token"]}'}
    response = rPost(url=url, headers=headers, files={"file": binary_data})
    return response.json()['guid']

