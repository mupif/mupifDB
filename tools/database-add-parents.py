
dbPort=27017

from pymongo import MongoClient
import pymongo
import pymongo.errors
from bson import ObjectId
import bson, bson.objectid
from rich import print_json
from rich.pretty import pprint
from rich.progress import track, Progress, TextColumn, BarColumn, TimeElapsedColumn
from rich.table import Column
import os.path
import time
import sys

from contextlib import contextmanager
import subprocess
import pydantic
from mupifDB import models

import logging
from rich.logging import RichHandler

logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="", #[%X]",
    handlers=[RichHandler(rich_tracebacks=True)]
)

log=logging.getLogger()
log.setLevel(logging.INFO)

def print_mongo(obj):
    if obj is None:
        pprint(None)
        return
    obj=obj.copy()
    obj['_id']=str(obj['_id'])
    print_json(data=obj)

coll2model={
    'fs.files':models.GridFSFile_Model,
    'WorkflowExecutions':models.WorkflowExecution_Model,
    'Workflows':models.Workflow_Model,
    'UseCases':models.UseCase_Model,
    'IOData':models.IODataRecord_Model,
    'WorkflowsHistory':models.Workflow_Model,
}

from typing import List,Iterator
import socket

def resolve_DbLookup(db, lookup: models.TEMP_DbLookup_Model, progress, task, parentless=False) -> Iterator[models.DbRef_Model]:
    try:
        coll=db.get_collection(lookup.where)
        query={k:v for k,v in zip(lookup.attrs,lookup.values)}
        if parentless:
            if lookup.where=='fs.files': query|={'metadata':{'parent':None}}
        else: query|={'parent':None}
        count=coll.count_documents(query)
        # if count>100: print(f'{lookup=}: {count} results.')
        progress.reset(task,description=f'   ↳…[yellow]{lookup.where}/{",".join(lookup.attrs)}',start=True,visible=True,completed=0,total=count)
        for res in coll.find(query):
            yield models.DbRef_Model(where=lookup.where,id=str(res['_id']))
            progress.advance(task)
            progress.refresh()
    except (pymongo.errors.NetworkTimeout,pymongo.errors.ServerSelectionTimeoutError,TimeoutError):
        log.error(f'Skipping: timeout with {lookup=}')
        return []


def resolve_DbRef(db, ref: models.DbRef_Model):
    rec=db.get_collection(ref.where).find_one({'_id':bson.objectid.ObjectId(ref.id)})
    if rec is None:
        log.warning(f'null result for {ref=}.')
        return None
    return coll2model[ref.where].model_validate(rec)

def set_parent_db(dbColl, child, parent: models.DbRef_Model):
    if child.parent and child.parent!=parent:
        raise RuntimeError(f'Old and new parents differ: {child.parent=} {parent=}')
    child.parent=parent
    querySet=child.TEMP_mongoParentQuerySet()
    child2a=dbColl.find_one_and_update(*querySet,return_document=pymongo.ReturnDocument.AFTER)
    # pprint(child2a)
    if child2a is None: raise RuntimeError('Object to be updated not found?')
    #child2=type(child).model_validate(child2a)
    #assert child.parent==parent

# TODO: this will set dangling reference in the parent object to null
def set_attr_null(dbColl,obj,attr):
    dbColl.find_one_and_update({'_id':bson.objectid.ObjectId(obj.dbID)},{'$set':{attr:None}})


if 1:
    print('Adding parents to all linked items…')
    with Progress(TextColumn('[progress.description]{task.description}',table_column=Column(width=40)),TextColumn('{task.completed}/{task.total}',table_column=Column(width=15)),BarColumn(),TimeElapsedColumn()) as progress:
        col_task=progress.add_task(' [green]…',visible=False)
        doc_task=progress.add_task('  [cyan]…',visible=False)
        chi_task=progress.add_task('   [yellow]…',visible=False)

        parentsAdded=0
        progress.update(col_task,description='↳[green]…',start=False,visible=False)
        progress.update(doc_task,description=' ↳[cyan]…',start=False,visible=False)
        progress.update(chi_task,description='  ↳[yellow]…',start=False,visible=False)

        client = MongoClient(f'mongodb://localhost:{dbPort}')
        db = client.MuPIF
        progress.reset(col_task,start=True)
        progress.update(col_task,description='↳[green]…',total=len(coll2model),completed=0,visible=True)
        progress.update(doc_task,description=' ↳[cyan]…',complete=None,total=None,visible=False)
        for coll,Model in coll2model.items():
            progress.update(col_task,description=f'↳[green]{coll}…')
            progress.reset(doc_task,description=' ↳[cyan](querying)…',start=False,refresh=True,visible=True)
            dbColl=db.get_collection(coll)
            cursor=dbColl.find()
            try:
                progress.update(doc_task,total=dbColl.count_documents(filter={}),description=f' ↳[cyan]{coll}')
            except (TimeoutError,pymongo.errors.ServerSelectionTimeoutError,pymongo.errors.NetworkTimeout):
                log.error(f'Skipping {coll=} due to DB timeout')
                continue
            progress.start_task(doc_task)
            for irec,rec in enumerate(cursor):
                try:
                    obj=Model.model_validate(rec)
                    thisRef=models.DbRef_Model(where=coll,id=obj.dbID)
                    for i,(attr,cref) in enumerate(obj.TEMP_getChildren()):
                            child=resolve_DbRef(db,cref)
                            if child is None:
                                log.error(f'Unresolvable child {cref=}; setting {attr=} to null!')
                                set_attr_null(dbColl,obj,attr)
                                print_mongo(rec)
                                continue
                            if child.parent is None:
                                parentsAdded+=1
                                set_parent_db(db.get_collection(cref.where),child,thisRef)
                    for clook in obj.TEMP_getLookupChildren():
                            for i,cref in enumerate(resolve_DbLookup(db,clook,progress,chi_task,parentless=True)):
                                child=resolve_DbRef(db,cref)
                                if child is None:
                                    log.error(f'Unresolvable child {cref=} {clook=} (setting to null not yet implemented)')
                                    print_mongo(rec)
                                    continue
                                if child.parent is None:
                                    parentsAdded+=1
                                    set_parent_db(db.get_collection(cref.where),child,thisRef)
                            progress.update(chi_task,visible=False)
                except pydantic.ValidationError:
                    print_mongo(rec)
                    raise
                progress.advance(doc_task)
            progress.advance(col_task)
        progress.refresh()
    print(f'Number of parents added: {parentsAdded=}\n\n')

if 1:
    print('Loooking for parent-less items')
    noParent=[]
    with Progress(TextColumn('[progress.description]{task.description}',table_column=Column(width=40)),TextColumn('{task.completed}/{task.total}',table_column=Column(width=15)),BarColumn(),TimeElapsedColumn()) as progress:
        col_task=progress.add_task(' [green]…',visible=False)
        doc_task=progress.add_task('  [cyan]…',visible=False)
        progress.update(col_task,description='↳[green]…',start=False,visible=False)
        progress.update(doc_task,description=' ↳[cyan]…',start=False,visible=False)
        client = MongoClient(f'mongodb://localhost:27017')
        # if 1:
        #    client = MongoClient(f'mongodb://localhost:{dbPort}?timeoutMS=20000')
        db = client.MuPIF
        progress.reset(col_task,start=True)
        progress.update(col_task,description='↳[green]…',total=len(coll2model),completed=0,visible=True)
        progress.update(doc_task,description=' ↳[cyan]…',complete=None,total=None,visible=False)
        for coll,Model in coll2model.items():
            progress.update(col_task,description=f'↳[green]{coll}…')
            progress.reset(doc_task,description=' ↳[cyan](querying)…',start=False,refresh=True,visible=True)
            dbColl=db.get_collection(coll)
            cursor=dbColl.find()
            progress.update(doc_task,total=dbColl.count_documents(filter={}),description=f' ↳[cyan]{coll}')
            progress.start_task(doc_task)
            for irec,rec in enumerate(cursor):
                obj=Model.model_validate(rec)
                if obj.parent is None: noParent.append((coll,obj.dbID))
                    # print(coll,obj.dbID)
                progress.advance(doc_task)
                # if irec>1000: break
            progress.advance(col_task)

    print(f'parentless items:')
    pprint({coll:sum([1 for np in noParent if np[0]==coll]) for coll in set([np[0] for np in noParent])})
    print('\n')
