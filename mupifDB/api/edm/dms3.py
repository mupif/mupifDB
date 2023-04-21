import pydantic 
from typing import Literal,Optional,Union,Tuple,List,Set,Dict,Any
import json
import typing
import re
import sys
import os.path
import astropy.units as au
from rich.pretty import pprint

import pymongo
import bson
import builtins
import string
import itertools
import gridfs
import shutil

import attrdict


import fastapi.responses
from fastapi import FastAPI, APIRouter

# uvicorn with reload re-imports the module without __name__=='__main__'
# thus use dms3.py (executable name) to check whether we are being run directly
if __name__=='__main__' or sys.argv[0]=='dms3.py':
    router=FastAPI()
    @router.exception_handler(Exception)
    async def validation_exception_handler(request, err):
        from fastapi.responses import JSONResponse
        import traceback
        return JSONResponse(status_code=400, content={
            "type": type(err).__name__,
            "message": f"{err}".replace('"',"'"),
            "url": str(request.url),
            "method": str(request.method),
            "traceback": traceback.format_exc().replace('"',"'").split("\n"),
        })

else:
    router=APIRouter(prefix='/EDM',tags=['EDM'])

@router.get('/')
def root(): return 'ok'


###
### SCHEMA
###

class ItemSchema(pydantic.BaseModel):
    'One data item in database schema'
    dtype: Literal['f','i','?','str','bytes','object']='f'
    unit: Optional[str]=None
    shape: pydantic.conlist(item_type=int,min_items=0,max_items=5)=[]
    link: Optional[str]=None

    def is_a_quantity(self):
        return self.dtype in ('f','i','?')

    @pydantic.validator("unit")
    def unit_valid(cls,v):
        if v is None: return
        try: au.Unit(v)
        except BaseException as e:
            print('Error validating unit {v} but exception not propagated??')
            raise
        return v

    @pydantic.root_validator
    def link_shape(cls,attrs):
        if attrs['link'] is not None:
            if len(attrs['shape'])>1: raise ValueError('links must be either scalar (shape=[]) or 1d array (shape=[num]).')
            if attrs['unit'] is not None: raise ValueError('unit not permitted with links')
        return attrs

class SchemaSchema(pydantic.BaseModel):
    'Schema of the schema itself; read via parse_obj'
    __root__: typing.Dict[str,typing.Dict[str,ItemSchema]]

    @pydantic.root_validator(pre=False)
    def _links_valid(cls, values):
        root=values['__root__']
        for T,fields in root.items():
            # must handle repeated validation
            if 'meta' in fields:
                if fields['meta']!=ItemSchema(dtype='object'): raise ValueError(f'{T}: "meta" field may not be specified in schema (is added automatically).')
            else:
                assert 'meta' not in fields
                fields['meta']=ItemSchema(dtype='object')
            for f,i in fields.items():
                if i.link is None: continue
                if i.link not in root.keys(): raise ValueError(f'{T}.{f}: link to undefined collection {i.link}.')
        return values




##
## VARIOUS HELPER FUNCTIONS
##

# characters used in bson IDs
_setLowercaseNum=set(string.ascii_lowercase+string.digits)

def _is_object_id(o):
    '''Desides whether *o* is string representation of bson.objectid.ObjectId'''
    return isinstance(o,str) and len(o)==24 and set(o)<=_setLowercaseNum
    len(o)==24

def _apply_link(item,o,func):
    '''Applies *func* to scalar link or list link, and returns the result (as scalar or list, depending on the schema)'''
    assert item.link is not None
    assert len(item.shape) in (0,1)
    if len(item.shape)==1: return [func(obj=o_,index=ix) for ix,o_ in enumerate(o)]
    return func(obj=o,index=None)

from collections.abc import Iterable
def _flatten(items, ignore_types=(str, bytes)):
    '''Flattens possibly nested sequence'''
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, ignore_types): yield from _flatten(x, ignore_types)
        else: yield x

import numpy as np
import sys
# backwards-compatible Sequence generic
# https://stackoverflow.com/a/71610402
if sys.version_info<(3,9): from typing import Sequence
else: from collections.abc import Sequence
Seq=Sequence

from pydantic import StrictInt, StrictFloat

# validation with plain int (coming before float, as before) will corrode float to int
# see https://pydantic-docs.helpmanual.io/usage/models/#data-conversion
# use StrictFloat and StrictInt instead
@pydantic.validate_arguments()
def _validated_quantity_2(
        itemName: str,
        item: ItemSchema,
        value: Union[
            StrictFloat,StrictInt,
            Seq[StrictFloat],Seq[StrictInt],
            Seq[Seq[StrictFloat]],Seq[Seq[StrictInt]],
            Seq[Seq[Seq[StrictFloat]]],Seq[Seq[Seq[StrictInt]]],
            Seq[Seq[Seq[Seq[StrictFloat]]]],Seq[Seq[Seq[Seq[StrictInt]]]]
        ],
        unit: Optional[str]=None
    ):
    '''
    Converts value and optional unit to either np.array or astropy.units.Quantity — depending on whether the schema has unit or not. Checks 
    
    * dtype compatibility (won't accept floats/complex into an integer array)
    * dimension compatibility (will reject 2d array where schema specifies scalar or 1d array, and similar)
    * shape compatibility (will reject 4-vector where schema specified 3-vector; schema may use -1 in dimension where no check will be done; i.e. 3×? array has shape [3,-1])
    * unit compatibility: whether *unit* and schema unit are compatible; and will convert value to the schema unit before returning
    
    Returns np.array (no unit in the schema) or astropy.unit.Quantity (in schema units).
    '''
    assert item.link is None
    # 1. create np.array
    # 1a. check numeric type convertibility (must be done item-by-item; perhaps can be optimized later?)
    if isinstance(value,Sequence):
        for it in _flatten(value):
            if not np.can_cast(it,item.dtype,casting='same_kind'): raise ValueError(f'{itemName}: type mismatch: item {it} cannot be cast to dtype {item.dtype} (using same_kind)')
    np_val=np.array(value,dtype=item.dtype)
    # 1b. check shape
    if len(item.shape) is not None:
        if len(item.shape)!=np_val.ndim: raise ValueError(f'{itemName}: dimension mismatch: {np_val.ndim} (shape {np_val.shape}), should be {len(item.shape)} (shape {item.shape})')
        for d in range(np_val.ndim):
            if item.shape[d]>0 and np_val.shape[d]!=item.shape[d]: raise ValueError(f'{itemName}: shape mismatch: axis {d}: {np_val.shape[d]} (should be {item.shape[d]})')
    # 2. handle units
    # 2a. schema has unit, data does not; or vice versa
    if (unit is None)!=(item.unit is None): raise ValueError(f'{itemName}: unit mismatch: unit is "{unit}" but schema unit is "{item.unit}".')
    
    # 2b. no unit, return np_val only
    if item.unit is None: return np_val
    # 2c. has unit, convert to schema unit (will raise exception is units are not compatible) and return au.Quantity
    return (np_val*au.Unit(unit)).to(item.unit)
                     
def _validated_quantity(itemName: str, item: ItemSchema, data):
    '''
    Gets sequence (value only) or dict as {'value':..} or {'value':..,'unit':..};
    passes that to _validated_quantity_2, which will do the proper data check and conversions;
    returns validated quantity as either np.array or astropy.units.Quantity
    '''
    if isinstance(data,Sequence): return _validated_quantity_2(item,data)
    elif isinstance(data,dict):
        if extras:=(data.keys()-{'value','unit'}):
            raise ValueError(f'Quantity has extra keywords: {", ".join(extras)} (only value, unit allowed).')
        return _validated_quantity_2(itemName, item,data['value'],data.get('unit',None))
    else: return _validated_quantity_2(itemName, item,value=data,unit=None)


class _PathEntry(pydantic.BaseModel):
    class Config:
        allow_mutation = False

    attr: str
    index: Optional[int]=None
    multiindex: Optional[List[int]]=None
    slice: Optional[Tuple[Optional[int],Optional[int],Optional[int]]]=None
    filter: Optional[str]=None

    def hasSubscript(self):
        'Has index or slice'
        return not (self.index is None and self.multiindex is None and self.slice is None)
    def isPlain(self):
        'No subscript or plain index (cannot exapand to multiple paths)'
        return (self.multiindex is None and self.slice is None)
    def subscript(self):
        if not self.hasSubscript(): return ''
        if self.index is not None: return f'[{self.index}]'
        elif self.multiindex is not None:
            # trailing comma to distinguish from plain index
            if len(self.multiindex)==1: return f'[{self.multiindex[0]},]'
            return f'{",".join([str(i) for i in self.multiindex])}'
        else:
            assert self.slice is not None
            s0,s1,s2=self.slice
            return f'[{"" if s0 is None else s0}:{"" if s1 is None else s1}{"" if s2 is None else ":"+str(s2)}]'
    def to_str(self): return self.attr+self.subscript()
    def apply_indexing(self,*,obj,klass,item):
        '''
        Returns
        * single-item list for scalar (no subscript)
        * single-item list for lists (index subscript)
        * list resulting from multiindex
        * list resulting from slicing (possibly empty, slice subscript)
        '''
        scalar=(len(item.shape)==0)
        assert item.link is not None
        val=obj[self.attr]
        if not self.hasSubscript():
            if not scalar: raise IndexError(f'{klass}.{self.attr} is a list, but was not subscripted (slice with [:] to select the entire list).')
            ret=[val]
        else:
            if scalar: raise IndexError(f'{klass}.{self.attr} is scalar but is indexed with {self.subscript()}')
            if self.index is not None: ret=[val[self.index]]
            elif self.multiindex is not None: ret=[val[i] for i in self.multiindex]
            else:
                assert self.slice is not None
                ret=val[slice(*self.slice)]
        return ret

def _parse_path(path: str) -> [_PathEntry]:
    '''
    Parses path *p* in dot notation, returning list of _PathEntry. For example:

    dot[1].not.ation[::-1] → [_PathEntry(attr='dot',index=1),_pathEntry(attr='not'),_PathEntry(attr='ation',slice=(None,None,-1))]
    '''
    import parsy as P
    if path=='' or path is None: return []

    none=P.string('').result(None)
    dec=P.regex('[+-]?[0-9]+').map(int).desc('decimal integer')
    dec_none=(dec|none).desc('possibly-empty-decimal')
    colon,comma,dot,pipe=P.string(':'),P.string(','),P.string('.'),P.string('|')
    lbrack,rbrack=P.string('['),P.string(']')

    balanced=P.forward_declaration()
    bracketed=P.seq(lbrack,balanced,rbrack).concat() # concat as string, don't need component access
    plain=P.regex(r'[^[\]]+') # any string without []
    balanced.become((plain|bracketed).many().concat().desc('balanced-expression'))

    # subscript
    index=dec.map(int).tag('index')
    multiindex=((dec<<comma).times(1,float('inf'))+dec.times(0,1)).tag('multiindex')
    slice=(dec_none).sep_by(colon,min=2,max=3).tag('slice')
    filter=((pipe>>balanced)|none).tag('filter')
    subscript=(lbrack>>P.seq(slice|multiindex|index,filter)<<rbrack).combine_dict(dict)
    # identifier
    attr=P.regex('[a-zA-Z_][a-zA-Z_0-9]*')

    def mk_dotpath(vv):
        def mk_entry(v):
            d={'attr':v['attr']}|dict(v['sub'] if v['sub'] is not None else {})
            # expand 2-tuple slice to 3-tuple
            if 'slice' in d and len(d['slice'])==2: d['slice']=d['slice']+[None]
            return _PathEntry(**d)
        return [mk_entry(v) for v in vv]
    # dot-separated identifier with optional subscript
    dotpath=P.seq(attr=attr,sub=subscript|none).sep_by(dot).map(mk_dotpath)

    return dotpath.parse(path)


def _unparse_path(path: [(str,Optional[int])]):
    return '.'.join([ent.to_str() for ent in path])

@pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
def _quantity_to_dict(q: Union[np.ndarray,au.Quantity]) -> dict: 
    if isinstance(q,au.Quantity): return {'value':q.value.tolist(),'unit':str(q.unit)}
    return {'value':q.tolist()}

@pydantic.dataclasses.dataclass
class _ResolvedPath(object):
    #class Config:
    #    arbitrary_types_allowed = True
    obj: typing.Any
    type: str
    id: str
    tail: List[_PathEntry]
    parent: Optional[str]

@pydantic.dataclasses.dataclass
class _ResolvedPaths(object):
    paths: List[_ResolvedPath]
    # path only contained no or plain indices (no expansion to multiple paths, such as slices)
    isPlain: bool
    # support iteration over the object
    def __len__(self): return len(self.paths)
    def __getitem__(self,ix): return self.paths[ix]

def _resolve_path_head(db: str, type: str, id: str, path: Optional[str]) -> _ResolvedPaths:
    '''
    Resolves path head, descending as far as it can get. Returns list of paths resolved
    '''
    def _descend(*,klass,dbId,path,level,parentId,resolved):
        klassSchema,rec=GG.db_get_schema_object(db,klass,dbId)
        obj=_db_rec_to_api_value__obj(klass,rec,parent=parentId)
        obj|=rec
        # pprint(obj)
        import attrdict
        obj=attrdict.AttrDict(obj)
        # terminate recursion here
        if len(path)==0:
            resolved+=[_ResolvedPath(obj=obj,type=klass,id=dbId,tail=[],parent=None if level==0 else parentId)]
            return
        assert len(path)>0
        ent=path[0]
        item=klassSchema[ent.attr]
        if item.link is not None:
            links=ent.apply_indexing(obj=obj,klass=klass,item=item)
            for link in links: _descend(klass=item.link,dbId=link,path=path[1:],level=level+1,parentId=dbId,resolved=resolved)
        else:
            resolved+=[_ResolvedPath(obj=obj,type=klass,id=dbId,tail=path,parent=parentId)]
        return
    resolved=[]
    parsed_path=_parse_path(path)
    _descend(klass=type,dbId=id,path=parsed_path,level=0,parentId=None,resolved=resolved)
    isPlain=all([ent.isPlain() for ent in parsed_path])
    assert len(resolved)==1 or not isPlain
    if any([ent.filter is not None for ent in parsed_path]):
        import asteval
        def filter_predicate(R):
            for ent in parsed_path:
                if ent.filter is None: continue
                proxy=DbAttrProxy(DB=db,klass=R.type,id=R.id)
                aeval=asteval.Interpreter()
                aeval.symtable|=proxy._self_dict()
                pred=aeval(ent.filter)
                # error during evaluation; raise the first exception and ignore the rest
                if len(aeval.error)>0:
                    raise RuntimeError(f'{db}:{R.type}:{R.id}: filter raise exception(s):\n'+'\n'.join(['\n'.join(e.get_error()) for e in aeval.error]))
                if not isinstance(pred,(bool,np.bool_,int)): raise ValueError(f'{db}:{R.type}:{R.id}: filters must return bool, np.bool_ or int, but "{ent.filter}" returned {pred.__class__.__module__}.{pred.__class__.__name__} (value {pred}).')
                # print(f'{db}:{R.type}:{R.id}: filter "{ent.filter}" → {pred}')
                if not bool(pred): return False
            return True
        resolved=[r for r in resolved if filter_predicate(r)]
    return _ResolvedPaths(paths=resolved,isPlain=isPlain)


class DbAttrProxy(object):
    '''Proxy attribute access for database entries, for use with expression evaluation.'''
    def __init__(self,DB,klass,id):
        self.DB,self.klass,self.id=DB,klass,id
        self.schema,self.vals=GG.db_get_schema_object(self.DB,self.klass,self.id)
    def __getattr__(self,attr):
        if attr not in self.schema: raise AttributeError(f'{self.klass}.{attr}: no such attribute.')
        item=self.schema[attr]
        if attr not in self.vals: raise ValueError(f'{self.klass}.{attr}: exists in schema but absent from database (id={self.id}).')
        val=self.vals[attr]
        if attr=='meta': return attrdict.AttrDict(val)
        if item.link is None:
            # TODO: preprocess values?
            # e.g. wrap dicts as attrdict.AttrDict, or convert to astropy.Quantity etc.
            return val
        else:
            # handle links
            scalar=(len(item.shape)==0)
            if scalar: return DbAttrProxy(DB=self.DB,klass=item.link,id=val)
            else: return [DbAttrProxy(DB=self.DB,klass=item.link,id=v) for v in val]
    def __dir__(self):
        return self.schema.keys()
    def __repr__(self):
        return f'<{self.klass} proxy: DB={self.DB} dbId={self.id}>'
    def _self_dict(self):
        'Returns self dictionary, for populating top-level namespace'
        return dict([(k,self.__getattr__(k)) for k in self.schema.keys() if k in self.vals])


@pydantic.dataclasses.dataclass
class _LinkTracker:
    nodes: Set[str]=pydantic.dataclasses.Field(default_factory=set)
    edges: Set[Tuple[str,str]]=pydantic.dataclasses.Field(default_factory=set)

@router.get('/{db}/{type}/{id}/graph')
def _make_link_digraph(db: str, type: str, id:str, debug:bool=False) -> Tuple[Set[str],Set[Tuple[str,str]]]:
    def _nd(k,i): return (f'{k}\n{i}' if debug else i)
    def _descend(klass,dbId,linkTracker):
        klassSchema,obj=GG.db_get_schema_object(db,klass,dbId)
        linkTracker.nodes.add(_nd(klass,dbId))
        for key,val in obj.items():
            if key=='_id' or key=='meta': continue
            item=klassSchema[key]
            if item.link is None: continue
            def _handle_link(*,obj,index,i=item,key=key):
                linkTracker.edges.add((_nd(klass,dbId),_nd(i.link,obj)))
                _descend(i.link,obj,linkTracker)
            _apply_link(item,val,_handle_link)
    tracker=_LinkTracker()
    _descend(klass=type,dbId=id,linkTracker=tracker)
    return (tracker.nodes,tracker.edges)

##
## schema POST, GET
## 
@router.post('/{db}/schema')
def dms_api_schema_post(db: str, schema: SchemaSchema, force:bool=False):
    'Writes schema to the DB.'
    coll=GG.db_get(db)['schema']
    if (s:=coll.find_one()) is not None and not force: raise ValueError('Schema already defined (use force=True if you are sure).')
    if s is not None: coll.delete_one(s)
    from rich.pretty import pprint
    # print(f'Schema is a {type(schema)=}')
    coll.insert_one(json.loads(schema.json()))
    GG.schema_invalidate_cache()

@router.get('/{db}/schema')
def dms_api_schema_get(db: str, include_id:bool=False):
    return GG.schema_get(db,include_id=include_id)

@pydantic.dataclasses.dataclass
class _ObjectTracker:
    path2id: dict=pydantic.dataclasses.Field(default_factory=dict)
    id2path: dict=pydantic.dataclasses.Field(default_factory=dict)
    def path2key(self,path):
        return tuple([e.to_str() for e in path])
    def add_tracked_object(self,path,id):
        self.path2id[self.path2key(path)]=id
        self.id2path[id]=tuple(path)
    def resolve_relpath_to_id(self,*,relpath,curr):
        tail=relpath
        where=curr
        while True:
            if tail.startswith('.'):
                tail=tail[1:]
                where=where[:-1]
                continue
            dot=tail.find('.')
            assert dot!=0
            if dot>0: head,tail=tail[:dot],tail[dot+1:]
            else: head,tail=tail,''
            if m:=re.match(r'^(.*)\[([0-9])+\]$',head): name,ix=m.groups(1),int(m.groups(2))
            else: name,ix=head,None
            where.append(_PathEntry(attr=name,index=ix))
            if tail=='': break
        key=self.path2key(where)
        if key not in self.path2id: raise RuntimeError(f'Unable to resolve "{relpath}" relative to "{curr}" (known objects: {" ".join([_unparse_path(p) for p in self.path2id.keys()])}')
        return self.path2id[key]
    def resolve_id_to_relpath(self,*,id,curr):
        abspath=self.id2path.get(id,None)
        if abspath is None: return None
        #print(f'{id=} {curr=} {abspath=}')
        for common in itertools.count(start=0):
            if curr[:common+1]!=abspath[:common+1]: break
        #print(f'{common=}')
        return (len(curr)-common)*'.'+_unparse_path(abspath[common:])


def _api_value_to_db_rec__attr(item,val,prefix):
    'Convert API value to the DB record (for attribute)'
    assert item.link is None
    if item.dtype=='str':
        if not isinstance(val,str): raise TypeError(f'{prefix} must be a str (not a {val.__class__.__name__})')
        return val
    elif item.dtype=='bytes':
        if not isinstance(val,str): raise TypeError('{prefix} must be a str (base64-encoded perhaps).')
        return val
    elif item.dtype=='object':
        return json.loads(json.dumps(val))
    elif item.is_a_quantity():
        q=_validated_quantity(prefix,item,val)
        return _quantity_to_dict(q)
    else: raise NotImplementedError(f'{prefix}: unable to convert API data to database record?? {item=}')

def _api_value_to_db_rec__obj(data):
    'Convert API value to DB record, without attribute (for objects)'
    ret={}
    # transfer selected metadata to the new object, if any
    # data contains metadata if it is a dump from an existing instance
    meta=ret['meta']=data.pop('meta',{})
    if 'id' in meta:
        meta['upstream']=meta.pop('id')
    return ret

def _db_rec_to_api_value__attr(item,dbrec,prefix):
    'Convert DB record to API value (for attribute)'
    assert item.link is None
    if item.dtype=='str': return dbrec
    elif item.dtype=='bytes': return dbrec
    elif item.dtype=='object': return dbrec
    elif item.is_a_quantity():
        return dbrec
    else: raise NotImplementedError(f'{prefix}: unable to convert record to API data?? {item=}')

def _db_rec_to_api_value__obj(klass,rec,parent):
    'Convert DB record to API value, without any attributes (for objects)'
    ret={}
    meta=ret['meta']=rec.pop('meta',{})
    meta|=dict(id=str(rec.pop('_id')),type=klass)
    if parent is not None: meta['parent']=parent
    return ret



class PatchData(pydantic.BaseModel):
    path: str
    data: Union[List[Dict[str,Any]],Dict[str,Any]]

@router.patch('/{db}/{type}/{id}')
def dms_api_object_patch(db:str,type:str,id:str,patchData:PatchData):
    path,data=patchData.path,patchData.data
    RR=_resolve_path_head(db=db,type=type,id=id,path=path)

    # validate inputs
    if RR.isPlain:
        if not isinstance(data,dict): raise ValueError(f'Patch data must be dict for plain (non-wildcard) paths (not a {data.__class__.__name__}).')
        data=[data]
    elif not isinstance(data,list): raise ValueError(f'Patch data must be a list for wildcard paths (not a {data.__class__.__name__}).')
    if len(RR)!=len(data):
        raise ValueError(f'Resolved patch paths and data length mismatch: {len(RR)} paths, {len(data)} data.')

    # write the data now
    for R,dat in zip(RR,data):
        if len(R.tail)==0: raise ValueError('Objects cannot be patched (only attributes can)')
        assert len(R.tail)==1
        ent=R.tail[0]
        if ent.index is not None: raise ValueError('Path {path} indexes an attribute (only whole attribute can be set, not its components).')
        item=GG.schema_get_type(db,R.type)[ent.attr]
        assert item.link is None
        rec=_api_value_to_db_rec__attr(item,dat,f'{R.type}:{path}')
        if ent.attr=='meta': rec=R.obj['meta']|dat
        r=GG.db_get(db)[R.type].find_one_and_update(
            {'_id':bson.objectid.ObjectId(R.id)}, # filter
            {'$set':{ent.attr:rec}}, # update
        )
        if r is None: raise RuntimeError(f'{db}/{R.type}/{R.id} not found for update?')


@router.post('/{db}/{type}')
def dms_api_object_post(db: str, type:str, data:dict) -> str:
    def _new_object(klass,dta,path,tracker):
        klassSchema=GG.schema_get_type(db,klass)
        rec=_api_value_to_db_rec__obj(dta)
        for key,val in dta.items():
            if not key in klassSchema: raise AttributeError(f'Invalid attribute {klass}.{key} (hint: {klass} defines: {", ".join(klassSchema.keys())}).')
            item=klassSchema[key]
            if item.link is not None:
                if len(item.shape)>0 and not isinstance(val,list): raise ValueError(f'{klass}.{key} should be list (not a {val.__class__.__name__}).')
                def _handle_link(*,obj,index,key=key,path=path):
                    if _is_object_id(obj): return obj
                    elif isinstance(obj,dict):
                        return _new_object(item.link,obj,path+[_PathEntry(attr=key,index=index)],tracker)
                    elif isinstance(obj,str):
                        # relative path to an object already created, resolve it...
                        return tracker.resolve_relpath_to_id(relpath=obj,curr=path)
                    else: raise ValueError('{klass}.{key}: must be dict, object ID or relative path (not a {obj.__class__.__name__})')
                rec[key]=_apply_link(item,val,_handle_link)
            else: rec[key]=_api_value_to_db_rec__attr(item,val,f'{klass}.{key}')
        ins=GG.db_get(db)[klass].insert_one(rec)
        idStr=str(ins.inserted_id)
        tracker.add_tracked_object(path,idStr)
        return idStr
    return _new_object(type,data,path=[],tracker=_ObjectTracker())

@router.post('/{db}/blob/upload')
def dms_api_blob_upload(db:str,blob: fastapi.UploadFile) -> str:
    'Streming blob upload'
    fs=gridfs.GridFS(GG.db_get(db))
    with fs.new_file() as f:
        shutil.copyfileobj(blob.file,f)
        return str(f._id)

@router.get('/{db}/blob/{id}')
def dms_api_blob_get(db:str,id:str):
    'Streaming blob download'
    fs=gridfs.GridFS(GG.db_get(db))
    def iterfile():
        yield from fs.get(id)
    return fastapi.responses.StreamingResponse(iterfile(),media_type="application/octet-stream")


#
# FIXME: paths is a space-delimited array
#
@router.get('/{db}/{type}/{id}/safe-links')
def dms_api_path_safe_links(db:str, type:str, id:str, paths:str='', debug:bool=False) -> List[str]:
    # collect leaf IDs of modification paths
    modIds=set()
    for p in paths.split():
        RR=_resolve_path_head(db,type,id,p)
        for R in RR:
            modIds.add(f'{R.type}\n{R.id}' if debug else R.id)
    #print(f'{modIds=}')
    # create directed graph of the current object
    nodes,edges=_make_link_digraph(db,type,id,debug=debug)
    import networkx as nx
    G=nx.DiGraph()
    G.add_nodes_from(nodes)
    G.add_edges_from(edges)
    assert nx.is_weakly_connected(G)
    # collect IDs of all objects leading to modified IDs
    viaIds=set()
    for modId in modIds:
        for p in nx.all_simple_paths(G,(f'{type}\n{id}' if debug else id),modId):
            #print(f'{p=}')
            viaIds.update(p)
    # return IDs which are not on path to modifications
    ret=nodes-viaIds
    return list(ret)


@router.get('/{db}/{type}/{id}/clone')
def dms_api_path_clone_get(db:str,type:str,id:str,shallow:str='') -> str:
    dump=dms_api_path_get(db=db,type=type,id=id,path=None,max_level=-1,tracking=True,meta=True,shallow=shallow)
    return dms_api_object_post(db=db,type=type,data=dump)

#
# FIXME: shallow is space-delimited list
#
@router.get('/{db}/{type}/{id}')
def dms_api_path_get(db:str,type:str,id:str,path: Optional[str]=None, max_level:int=-1, tracking:bool=False, meta:bool=True, shallow:str='') -> typing.Union[dict,List[dict]]:
    def _get_object(klass,dbId,parentId,path,tracker):
        if tracker and (p:=tracker.resolve_id_to_relpath(id=dbId,curr=path)): return p
        if max_level>=0 and len(path)>max_level: return {}
        klassSchema,obj=GG.db_get_schema_object(db,klass,dbId)
        # creates API value skeleton (meta etc), without attributes
        ret=_db_rec_to_api_value__obj(klass,obj,parentId)
        if not meta: ret.pop('meta')
        for key,val in obj.items():
            if key in ('_id','meta'): continue
            if not key in klassSchema: raise AttributeError(f'Invalid stored attribute {klass}.{key} (not in schema).')
            item=klassSchema[key]
            if item.link is not None:
                if len(path)==max_level: continue
                def _resolve(*,obj,index,i=item,key=key):
                    # print(f'{i.link} {obj=} {shallow_=}')
                    if obj in shallow_: return obj
                    return _get_object(i.link,obj,parentId=dbId,path=path+[_PathEntry(attr=key,index=index)],tracker=tracker)
                ret[key]=_apply_link(item,val,_resolve)
            else:
                ret[key]=_db_rec_to_api_value__attr(item,val,f'{klass}.{key}')
        if tracker: tracker.add_tracked_object(path,dbId)
        return ret
    shallow_=set(shallow.split())
    # print(f'{shallow_=}')
    RR=_resolve_path_head(db=db,type=type,id=id,path=path)
    ret=[]
    for R in RR:
        if len(R.tail)==0:
            obj=_get_object(R.type,R.id,parentId=R.parent,path=[],tracker=_ObjectTracker() if tracking else None)
            ret.append(obj)
            continue
        # the result is an attribute which is yet to be obtained from the object
        if len(R.tail)>1: raise ValueError(f'Path {path} has too long tail ({_unparse_path(R.tail)}).')
        ent=R.tail[0]
        if ent.index is not None: raise ValueError(f'Path {path} indexes an attribute (indexing is only allowed within link array)')
        item=GG.schema_get_type(db,R.type)[ent.attr]
        assert item.link is None
        ret.append(_db_rec_to_api_value__attr(item,R.obj[ent.attr],f'{R.type}.{ent.attr}'))
    # print(f'{path=} {RR.isPlain=} {len(ret)=}')
    return (ret[0] if RR.isPlain else ret)


@router.get('/{db}')
def dms_api_type_list(db: str):
    return list(GG.schema_get(db).dict().keys())

@router.get('/{db}/{type}')
def dms_api_object_list(db: str, type: str):
    res=GG.db_get(db)[type].find()
    return [str(r['_id']) for r in res]



class GG(object):
    '''Global (static) objects for the server, used throughout. Populated at startup here below'''
    _DB={}
    _SCH={}
    _cli=None

    @staticmethod
    def client_set(cli: pymongo.MongoClient):
        GG._cli=cli
    @staticmethod
    def db_get(db:str):
        if db not in GG._DB: GG._DB[db]=GG._cli[db]
        return GG._DB[db]
    @staticmethod
    def db_get_schema_object(db:str,klass:str,dbId:str):
        obj=GG.db_get(db)[klass].find_one({'_id':bson.objectid.ObjectId(dbId)})
        if obj is None: raise KeyError(f'No object {klass} with id={dbId} in the database {db}')
        assert str(obj['_id'])==dbId
        #if 'meta' not in obj: obj['meta']={}
        #obj['meta']['id']=dbId
        #del obj['_id']
        return GG.schema_get_type(db,klass),obj
    @staticmethod
    def schema_get(db:str,include_id:bool=False):
        if db not in GG._SCH:
            rawSchema=GG.db_get(db)['schema'].find_one()
            if rawSchema is not None:
                if not include_id and '_id' in rawSchema: del rawSchema['_id'] # this prevents breakage when reloading
            GG._SCH[db]=SchemaSchema.parse_obj(rawSchema)
        return GG._SCH[db]
    @staticmethod
    def schema_get_type(db:str,type:str):
        # why do we need to access through __root__ here? unclear
        return GG.schema_get(db).__root__[type]

    @staticmethod
    def schema_invalidate_cache():
        GG._SCH={}

    @staticmethod
    def schema_import(db:str, json_str:str, force=False):
        rawSchema=json.loads(json_str)
        dms_api_schema_post(db,rawSchema,force=force)
        GG.schema_invalidate_cache()

    @staticmethod
    def schema_import_maybe(db: str, json_str:str):
        try: s=GG.schema_get(db)
        except KeyError:
             GG.schema_import(db,json_str)



def initializeEdm(client):
    GG.client_set(client)
    GG.schema_import_maybe('dms0',open(os.path.dirname(__file__)+'/dms-schema.json').read())

# when run as main, but also upon reload from uvicorn — see at the beginning of this file
if __name__=='__main__' or sys.argv[0]=='dms3.py':
    initializeEdm(pymongo.MongoClient("localhost",27017))

if __name__=='__main__':
    import uvicorn
    uvicorn.run('dms3:router',host='0.0.0.0',port=8080,reload=True)

