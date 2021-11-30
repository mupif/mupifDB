import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")

import importlib
from pymongo import MongoClient
import gridfs
import mupifDB
import tempfile
import argparse
import urllib.parse

client = MongoClient()
db = client.MuPIF
fs = gridfs.GridFS(db)

parser = argparse.ArgumentParser()
parser.add_argument('-u', '--usercase', required=True, dest="usercase")
parser.add_argument('-wid', '--workflowid', required=True, dest="wid")
parser.add_argument('-s', '--source', required=True, dest="source")  # url
parser.add_argument('-m', '--module', required=True, dest="module")  
parser.add_argument('-c', '--classname', required=True, dest="classname")
parser.add_argument('-d', '--description', required=False, dest="description")

# The 'module' specifies the python script where the workflow class is defined.
# The database itself accepts a single script or a zip archive where this script is present with additional files.

args = parser.parse_args()
moduleurl = args.source
modulename = args.module
classname = args.classname

tempRoot = '/tmp'
tempDir = tempfile.mkdtemp(dir=tempRoot, prefix='mupifDB_tmp')
print("Tempdir=%s" % tempDir)
# # get basename
path = urllib.parse.urlsplit(moduleurl).path
filename = os.path.basename(path)
print("Modulename=%s" % filename)

# urllib.request.urlretrieve(moduleurl, tempDir+'/'+filename)
# if zipfile.is_zipfile(tempDir+'/'+filename):
#     zipfile.ZipFile(tempDir+'/'+filename, mode='r').extractall(path=tempDir)

mypath = path.replace(filename, "")
sys.path.append(mypath)

sys.path.append(tempDir)
moduleImport = importlib.import_module(modulename)
print(moduleImport)
workflowClass = getattr(moduleImport, classname)
workflow = workflowClass()

rid = mupifDB.workflowmanager.insertWorkflowDefinition(
    wid=args.wid,
    description=args.description,
    source=moduleurl,
    useCase=args.usercase,
    workflowInputs=workflow.getMetadata('Inputs'),
    workflowOutputs=workflow.getMetadata('Outputs'),
    modulename=modulename,
    classname=classname
)
print("workflow "+str(rid)+" registered")
