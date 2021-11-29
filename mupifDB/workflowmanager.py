import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")
from pymongo import ReturnDocument
import tempfile
import collections
from bson import ObjectId
import re
from ast import literal_eval 
from mupifDB import error

import restApiControl

import mupif


# pool to handle workflow execution requests
# ctx = multiprocessing.get_context('spawn')
# pool = ctx.Pool()
# pool = multiprocessing.Pool()


emptyWorkflowExecutionRecord = {
    'WorkflowID': None,
    'WorkflowVersion:': None,
    'Status': "Created",
    'StartDate': None,
    'EndDate': None,
    'ExecutionLog': None,
    'RequestedBy': None,
    'Inputs': None,
    'Outputs': None
}


def insertWorkflowDefinition(db, wid, description, source, useCases, workflowInputs, workflowOutputs, modulename, classname):
    """
    Inserts new workflow definition into DB. 
    Note there is workflow versioning schema: the current (latest) workflow version are stored in workflows collection.
    The old versions (with the same wid but different version) are stored in workflowsHistory.
    @param db database
    @param wid unique workflow id
    @param description Description
    @param source source URL
    @param useCases tuple of useCase IDs the workflow belongs to
    @param workflowInputs workflow input metadata (list of dicts)
    @param workflowOutputs workflow output metadata (list of dicts)
    @param modulename
    @param classname
    """
    # prepare document to insert
    # save workflow source to gridfs
    # to allow for more general case, workflow should be tar.gz 
    # archive of all workflow inplamentation files
    sourceID = None

    with open(source, 'rb') as f:
        # sourceID = restApiControl.uploadBinaryFileContentAndZip(f)
        sourceID = restApiControl.uploadBinaryFileContent(f)
        f.close()
        print(sourceID)
        if sourceID is not None:
            sourceID = ObjectId(sourceID)

    rec = {'wid': wid, 'Description': description, 'GridFSID': sourceID, 'SourceURL': source, 'UseCases': useCases, 'IOCard': None, 'modulename': modulename, 'classname': classname}
    Inputs = []
    for i in workflowInputs:
        irec = {'Name': i['Name'], 'Description': i.get('Description', None), 'Type': i['Type'], 'TypeID': i['Type_ID'], 'Units': i['Units'], 'ObjID': i.get('Obj_ID', None), 'Compulsory': i['Required']}
        Inputs.append(irec)
    Outputs = []
    for i in workflowOutputs:
        irec = {'Name': i['Name'], 'Description': i.get('Description', None), 'Type': i['Type'], 'TypeID': i['Type_ID'], 'Units': i['Units'], 'ObjID': i.get('Obj_ID', None)}
        Outputs.append(irec)
    rec['IOCard'] = {'Inputs': Inputs, 'Outputs': Outputs}
    
    # first check if workflow with wid already exist in workflows collections
    w_rec = restApiControl.getWorkflowRecord(wid)
    if w_rec is None:  # can safely create a new record in workflows collection
        version = 1
        rec['Version'] = version
        # todo
        result = db.Workflows.insert_one(rec)
        return result.inserted_id 
    else:
        # the workflow already exists, need to make a new version
        # clone latest version to History
        # print(w_rec)
        w_rec.pop('_id')  # remove original document id
        # todo
        db.WorkflowsHistory.insert(w_rec)
        # update the latest document
        version = (1+int(w_rec.get('Version', 1)))
        rec['Version'] = version
        # todo
        result = db.Workflows.find_one_and_update({'wid': wid}, {'$set': rec}, return_document=ReturnDocument.AFTER)
        # print(result)
        if result:
            return result['_id']
        else:
            print("Update failed")


def getWorkflowDoc(wid, version=-1):
    """ 
        Returns workflow document with given wid and version
        @param version workflow version, version == -1 means return the most recent version    
    """
    wdoclatest = restApiControl.getWorkflowRecord(wid)
    if wdoclatest is None:
        raise KeyError("Workflow document with WID" + wid + " not found")
    lastversion = wdoclatest.get('Version', 1)
    if version == -1 or version == lastversion:  # get the latest one
        return wdoclatest
    elif version < lastversion:
        wdoc = restApiControl.getWorkflowRecordFromHistory(wid, version)
        if wdoc is None:
            raise KeyError("Workflow document with WID" + wid + "Version" + str(version) + " not found")
        return wdoc
    else:
        raise KeyError("Workflow document with WID" + wid + "Version" + str(version) + ": bad version")


def updateWorkflowDefinition(db, wid, description, version, source, useCases, workflowInputs, workflowOutputs):
    """
    Updates the workflow definition into DB. 
    Note this affects the exiting workflow executions as they refer to the old version! 
    This to be solved by workflow versiong.
    @param db database
    @param wid unique workflow id
    @param description Description
    @param version Version
    @param source source URL
    @param useCases tuple of useCase IDs the workflow belongs to
    @param workflowInputs workflow input metadata (list of dicts)
    @param workflowOutputs workflow output metadata (list of dicts)
    """
    rec = {'Description': description, 'Version': version, 'Source': source, 'UseCases': useCases, 'IOCard': None}
    Inputs = []
    for i in workflowInputs:
        irec = {'Name': i['Name'], 'Description': i.get('Description', None), 'Type': i['Type'], 'TypeID': i['Type_ID'], 'Units': i['Units'], 'ObjID': i.get('Obj_ID', None), 'Compulsory': i['Required']}
        Inputs.append(irec)
    Outputs = []
    for i in workflowOutputs:
        irec = {'Name': i['Name'], 'Description': i.get('Description', None), 'Type': i['Type'], 'TypeID': i['Type_ID'], 'Units': i['Units'], 'ObjID': i.get('Obj_ID', None)}
        Outputs.append(irec)
    rec['IOCard'] = {'Inputs': Inputs, 'Outputs': Outputs}

    # todo
    result = db.Workflows.update_one({"_id": wid}, {'$set': rec})
    return result 


class WorkflowExecutionIODataSet:
    def __init__(self, db, wec, IOid):
        self.db = db
        self.wec = wec
        self.IOid = IOid
    
    @staticmethod
    def create(workflowID, type='Input', workflowVer=-1):
        rec = {'Type': type, 'DataSet': []}

        wdoc = getWorkflowDoc(workflowID, version=workflowVer)
        if wdoc is None:
            raise KeyError("Workflow document with ID" + workflowID + " not found")

        IOCard = wdoc['IOCard']
        rec = {}
        data = []
        for io in IOCard[type]:  # loop over workflow inputs
            if isinstance(io.get('ObjID', None), collections.Iterable):
                for id in io['ObjID']:
                    # make separate input entry for each obj_id
                    data.append({'Name': io['Name'], 'Type': io['Type'], 'Value': None, 'Units': io['Units'], 'ObjID': id, 'Compulsory': io.get('Compulsory', None), 'Source': None, 'OriginId': None})
            else:  # single obj_id provided
                data.append({'Name': io['Name'], 'Type': io['Type'], 'Value': None, 'Units': io['Units'], 'ObjID': io.get('ObjID', None), 'Compulsory': io.get('Compulsory', None), 'Source': None, 'OriginId': None})
        rec['Type'] = type
        rec['DataSet'] = data
        rec_id = restApiControl.insertIODataRecord(rec)
        return rec_id

    def _getDocument(self):
        """
        Returns workflowExection document corresponding to self.executionID
        """
        iod_record = restApiControl.getIODataRecord(self.IOid)
        if iod_record is None:
            raise KeyError("Document with ID" + self.IOid + " not found")
        return iod_record

    def getRec(self, name, obj_id=None):
        """
        Returns the input record identified by name
        @param: input name
        @return: associated record
        @throws: KeyError if input parameter name not found
        """
        doc = self._getDocument()
    
        for rec in doc['DataSet']:
            if (rec['Name'] == name) and (rec['ObjID'] == obj_id):
                return rec
        raise KeyError("Input parameter " + name + " Obj_ID " + str(obj_id) + " not found")
         
    def get(self, name, obj_id=None):
        """
        Returns the value of input parameter identified by name
        @param: input name
        @return: associated value
        @throws: KeyError if input parameter name not found
        """
        return self.getRec(name, obj_id)['Value']
    
    def set(self, name, value, obj_id=None):
        """
        Sets the value of output parameter identified by name to given value
        @param: name parameter name
        @param: value associated value
        @throws: KeyError if input parameter name not found
        """
        # print(f'WorkflowExecutionIODataSet:set self={self.IOid}: {name}={value}, objid={obj_id}')
        if self.wec.getStatus() == 'Created':
            res = self.db.IOData.update_one({'_id': self.IOid}, {'$set': {"DataSet.$[r].Value": value}}, array_filters=[{"r.Name": name, "r.ObjID": obj_id}])
            if res.matched_count == 1:
                return
            else:
                raise KeyError("Input parameter " + name + " Obj_id " + str(obj_id) + " not found")
        else:
            raise KeyError("Inputs cannot be changed as workflow execution status is not Created")
 
    def setAttributes(self, name, attributes, obj_id=None):
        """
        Sets the value of output parameter attributes identified by name to given value
        @param: name parameter name
        @param: attributes dict of kye, value to set
        @param: value associated value
        @throws: KeyError if input parameter name not found
        """
        ddict = {}
        for key, val in attributes.items():
            ddict["DataSet.$[r].%s" % key] = val

        res = self.db.IOData.update_one({'_id': self.IOid}, {'$set': ddict}, array_filters=[{"r.Name": name, "r.ObjID": obj_id}])
        if res.matched_count == 1:
            return
        else:
            raise KeyError("Input parameter " + name + " Obj_id " + str(obj_id) + " not found")


class WorkflowExecutionContext:

    def __init__(self, db, executionID, **kwargs):
        self.db = db
        self.executionID = executionID

    @staticmethod
    def create(db, workflowID, requestedBy, workflowVer=-1):
        """
        """
        # first query for workflow document
        wdoc = getWorkflowDoc(workflowID, version=workflowVer)
        if wdoc is not None:
            # IOCard = wdoc['IOCard']
            rec = emptyWorkflowExecutionRecord.copy()
            rec['WorkflowID'] = workflowID
            rec['WorkflowVersion'] = wdoc.get('Version', 1)
            rec['RequestedBy'] = requestedBy
            # inputs = []
            # for io in IOCard['Inputs']: #loop over workflow inputs
            #     inputs.append({'Name':io['Name'], 'Type':io['Type'], 'Value': None, 'Source':None, 'OriginId':None })
            # rec['InputDataOData'] = {'Inputs': inputs, 'Outputs': None}

            rec['Inputs'] = WorkflowExecutionIODataSet.create(workflowID, 'Inputs')
            rec['Outputs'] = WorkflowExecutionIODataSet.create(workflowID, 'Outputs')
            new_id = restApiControl.insertExecutionRecord(rec)
            return WorkflowExecutionContext(db, new_id)

        else:
            raise KeyError("Workflow record " + workflowID + ", Version " + str(workflowVer) + " not found")

    def _getWorkflowExecutionDocument(self):
        """
        Returns workflowExection document corresponding to self.executionID
        """
        we_rec = restApiControl.getExecutionRecord(self.executionID)
        if we_rec is None:
            raise KeyError("Record with id=" + self.executionID + " not found")
        return we_rec

    def _getWorkflowDocument(self):
        """
        Returns workflow document corresponding to self.executionID
        """
        doc = self._getWorkflowExecutionDocument()
        wid = doc['WorkflowID']
        version = doc['WorkflowVersion']
        wdoc = getWorkflowDoc(wid, version=version)
        if wdoc is None:
            raise KeyError("Workflow document with ID" + wid + " not found")
        return wdoc

    def set(self, name, value):
        """
        Updates workflow execution attribute identified by name
        """
        doc = self._getWorkflowExecutionDocument()
        if name in doc:
            restApiControl.setExecutionParameter(self.executionID, name, value)

    def get(self, name):
        """
        Returns workflow execution attribute identified by name
        """
        doc = self._getWorkflowExecutionDocument()
        return doc[name]

    def getIODataDoc(self, type='Inputs'):
        doc = self._getWorkflowExecutionDocument()    
        return WorkflowExecutionIODataSet(self.db, self, self.get(type))

    def getStatus(self):
        wed = self._getWorkflowExecutionDocument()
        return wed['Status']
            
    def execute(self):
        wed = self._getWorkflowExecutionDocument()
        wd = self._getWorkflowDocument()
        print('Scheduling the new execution:%s' % self.executionID)
        # return pool.apply_async(self.__executeWorkflow, (wed, wd)).wait()
        # print(wd)

        if wed['Status'] == 'Created':
            # freeze the execution record by setting state to "Pending"
            restApiControl.setExecutionParameter(self.executionID, "Status", "Pending")
            print("Execution %s state changed to Pending" % self.executionID)
        else:
            # raise KeyError("Workflow execution already scheduled/executed")
            raise error.InvalidUsage("Workflow execution already scheduled/executed")

        return 0


def mapInput(app, value, type, typeID, units, compulsory, objectID):
    if value is not None:
        # map value 
        if type == 'mupif.Property':
            print('Mapping %s, units %s, value:%s' % (mupif.DataID[typeID], units, value))
            fvalue = literal_eval(value)
            if isinstance(fvalue, tuple):
                app.setProperty(mupif.ConstantProperty(fvalue, mupif.DataID[typeID], mupif.ValueType.Vector, units), objectID)
            else:
                app.setProperty(mupif.ConstantProperty(float(value), mupif.DataID[typeID], mupif.ValueType.Scalar, units), objectID)
        elif type == 'mupif.Field':
            # assume Field == ConstantField
            print('Mapping %s, units %s, value:%s' % (mupif.DataID[typeID], units, value))
            fvalue = literal_eval(value)
            if isinstance(fvalue, tuple):
                app.setField(mupif.constantfield.ConstantField(None, mupif.DataID[typeID], mupif.ValueType.Scalar, units, 0.0, values=fvalue, objectID=objectID), objectID)
            else:
                raise TypeError('Tuple expected when handling io param of type %s' % type)
        else:
            raise KeyError('Handling of io param of type %s not implemented' % type)
    elif (value is None) and (compulsory is True):
        raise KeyError('Compulsory parameter %s not defined in workflow execution dataset' % typeID)


def mapInputs(app, db, eid):
    # request workflow execution doc
    print('Map Inputs eid %s' % eid)
    wec = WorkflowExecutionContext(db, ObjectId(eid))
    # get worflow doc
    wd = wec._getWorkflowDocument()
    # execution input doc
    inp = wec.getIODataDoc('Inputs')
    # loop over workflow inputs
    for irec in wd['IOCard']['Inputs']:
        name = irec['Name']
        type = irec['Type']
        typeID = irec['TypeID']
        # try to get raw PID from typeID
        match = re.search('\w+\Z', typeID)
        if match:
            typeID = match.group()
        
        objID = irec.get('ObjID', None)
        compulsory = irec['Compulsory']
        units = irec['Units']
        if units == 'None':
            units = mupif.U.none

        if isinstance(objID, collections.Iterable):
            for oid in objID:
                value = inp.get(name, oid)
                mapInput(app, value, type, typeID, units, compulsory, oid)
        else:
            value = inp.get(name, objID)
            mapInput(app, value, type, typeID, units, compulsory, objID)


def generateWEInputCGI(db, eid):
    # request workflow execution doc
    wec = WorkflowExecutionContext(db, ObjectId(eid))
    # execution input doc
    inp = wec.getIODataDoc('Inputs')
    print("<h1>Edit input record for executionID: %s</h1><br />"%eid)
    print("<form action=\"/cgi-bin/demo01b.py\" method=\"post\">")

    # table header
    print("<table><tr><th>Name</th><th>Type</th><th>ObjID</th><th>Value</th><th>Source</th><th>Origin</th></tr>")

    # loop over workflow inputs
    c = 0
    for irec in inp._getDocument()['DataSet']:
        name = irec['Name']
        type = irec['Type']
        # typeID=irec['TypeID']
        objID = irec['ObjID']
        print("<tr><td><b>%s</b></td><td>%s</td><td>%s</td>" % (name, type, objID))
        print("<td><input type=\"text\" name=\"Value_%d\" /></td>" % c)
        print("<td><input type=\"text\" name=\"Source_%d\" /></td>" % c)
        print("<td><input type=\"text\" name=\"OriginID_%d\" /></td></tr>" % c)
        c = c+1
    print("</table>")
    print("<input type=\"hidden\" name=\"eid\" value=\"%s\"/>" % eid)
    print("<input type=\"submit\" value=\"Submit\" /></form>")


def setWEInputCGI(db, eid, form):
    wec = WorkflowExecutionContext(db, ObjectId(eid))
    # execution input doc
    inp = wec.getIODataDoc('Inputs')
    print('Input rec id: %s<br>' % inp.IOid)
    c = 0
    for irec in inp._getDocument()['DataSet']:
        name = irec['Name']
        objID = irec['ObjID']
        value = form.getvalue('Value_%d' % c)
        source = form.getvalue('Source_%d' % c)
        originID = form.getvalue('OriginID_%d' % c)
        print('Setting %s, ObjID %s to %s ' % (name, objID, value))
        try:
            inp.set(name, value, objID)
            print(' OK<br>')
        except Exception as e:
            print(' Failed<br>')
            print(e)
        # set source and origin
        c = c+1


def mapOutput(app, db, name, type, typeID, objectID, eid, tstep):
    wec = WorkflowExecutionContext(db, ObjectId(eid))
    # execution input doc
    out = wec.getIODataDoc('Outputs')
    # map value 
    print('Mapping %s, name:%s' % (typeID, name), flush=True)
    if type == 'mupif.Property':
        print("Requesting %s, objID %s, time %s" % (mupif.DataID[typeID], objectID, tstep.getTargetTime()), flush=True)
        prop = app.getProperty(mupif.DataID[typeID], tstep.getTargetTime(), objectID)
        out.setAttributes(name, {"Value": prop.getValue(), "Units": str(prop.getUnits())}, objectID)
    elif type == 'mupif.Field':
        with tempfile.TemporaryDirectory() as tempDir:
            field = app.getField(mupif.DataID.FID_Temperature, tstep.getTargetTime())  # timestep as None!!
            field.field2VTKData().tofile(tempDir+'/field')
            with open(tempDir+'/field.vtk', 'rb') as f:
                logID = restApiControl.uploadBinaryFileContent(f)
                f.close()
                print("Uploaded field.vtk as id: %s" % logID)
                out.setAttributes(name, {"Value": logID, "Units": str(field.getUnits())}, objectID)

    else:
        raise KeyError('Handling of io param of type %s not implemented' % type)


def mapOutputs(app, db, eid, tstep):
    # request workflow execution doc
    print('Maping Outputs for eid %s' % eid, flush=True)
    wec = WorkflowExecutionContext(db, ObjectId(eid))
    # get worflow doc
    wd = wec._getWorkflowDocument()
    # execution out doc
    inp = wec.getIODataDoc('Outputs')
    # loop over workflow inputs
    for irec in wd['IOCard']['Outputs']:
        name = irec['Name']
        type = irec['Type']
        typeID = irec['TypeID']
        # try to get raw PID from typeID
        match = re.search('\w+\Z', typeID)
        if match:
            typeID = match.group()
        
        objID = irec.get('ObjID', None)
        # compulsory = irec['Compulsory']
        units = irec['Units']

        if isinstance(objID, collections.Iterable):
            for oid in objID:
                value = inp.get(name, oid)
                mapOutput(app, db, name, type, typeID, oid, eid, tstep)
        else:
            value = inp.get(name, objID)
            mapOutput(app, db, name, type, typeID, objID, eid, tstep)
