import unittest
import requests
import json
from rich.pretty import pprint
from rich import print_json
import numpy as np

import pymongo


import sys,os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import dms3


DB='test'
DB1='test-1'
REST_URL='http://localhost:8080'
dms3.initializeEdm(pymongo.MongoClient("localhost",27017))


dta={ # BeamState 
    "beam":{ # Beam
        "length": { "value": 2500, "unit":"mm" },
        "height": { "value": 20, "unit":"cm" },
        "density": { "value": 3.5, "unit":"g/cm3" },
        "cs":{ # CrossSection
            "rvePositions": {"value":[[1,2,3],[4,5,6]],"unit":"mm"},
            "rve":{  # ConcreteRVE
                "origin":{"value":[5,5,5],"unit":"mm"},
                "size":{ "value":[150,161,244],"unit":"um" },
                "ct":{ # CTScan
                    "id":{'value':"scan-000"}
                },
                "materials":[
                    { # MaterialRecord
                        "name":{'value':"mat0"},
                        "props":{"origin":"CZ","year":2018,"quality":"good"},
                    },
                    { # MaterialRecord
                        "name":{'value':"mat1"},
                        "props":{"origin":"PL","year":2016,"project":"HTL-344PRP"},
                    }
                ]
            },
        }
    },
    "cs": ".beam.cs", # relative link to the ../beam/cs object
    "npointz": 2,
    "csState":[
        { # CrossSectionState
            "eps_axial": { "value":344, "unit":"um/m" },
            "bendingMoment": { "value":869, "unit":"kN*m" },
            "meta": { "tags": ["tagCommon","tag1"] },
            "rveStates":[ 
                { # ConcreteRVEState
                    "rve":"...beam.cs.rve", # rel 
                    "sigmaHom": { "value": 89.5, "unit":"MPa" }
                },
                { # ConcreteRVEState
                    "rve":"...beam.cs.rve", # rel 
                    "sigmaHom": { "value": 81.4, "unit":"MPa" }
                },
            ]
        },
        { # CrossSectionState
            "eps_axial": { "value":878, "unit":"um/m" },
            "bendingMoment": { "value":123, "unit":"kN*m" },
            "meta": { "tags": ["tagCommon","tag2"] },
            "rveStates":[ 
                { # ConcreteRVEState
                    "rve":"...beam.cs.rve", # rel 
                    "sigmaHom": { "value": 55.6, "unit":"MPa" }
                },
            ]
        },

    ],

}


class Test_REST(unittest.TestCase):
    # wrappers for the REST API
    @classmethod
    def get(C,p,show=False,**kw):
        r=requests.get(f'{REST_URL}/{p}',params=kw)
        if not r.ok:
            raise RuntimeError(r.text)
        data=json.loads(r.text)
        if show: pprint(data)
        return data
    @classmethod
    def post(C,p,body,query=None):
        r=requests.post(f'{REST_URL}/{p}',json=body,params=query)
        if not r.ok: raise RuntimeError(r.text)
        return json.loads(r.text)
    @classmethod
    def post_data(C,p,name,data):
        r=requests.post(f'{REST_URL}/{p}',files={name:data})
        if not r.ok: raise RuntimeError(r.text)
        return json.loads(r.text)
    @classmethod
    def get_data(C,p):
        with requests.get(f'{REST_URL}/{p}',stream=True) as res: return res.content
    @classmethod
    def patch(C,p,patchData):
        r=requests.patch(f'{REST_URL}/{p}',json=patchData)
        if not r.ok: raise RuntimeError(r.text)
        return json.loads(r.text)

    def test_00_set_schema(self):
        C=self.__class__
        schema=json.loads((f:=open(os.path.dirname(__file__)+'/dms-schema.json')).read())
        f.close()
        # print(schema)
        C.post(f'{DB}/schema',body=schema,query={'force':True})
    def test_01_post(self):
        C=self.__class__
        C.ID_01=C.post(f'{DB}/BeamState',body=dta)
        C.ID_02=C.post(f'{DB}/BeamState',body={'beam':{ "cs":{ "rve":{ "ct":{ "id":{'value':"scan-000"} } } } }})
    def test_02_get(self):
        C=self.__class__
        d=C.get(f'{DB}/BeamState/{C.ID_01}',meta=True,tracking=False)
        # check that relative link was correctly interpreted
        self.assertEqual(d['cs']['meta']['id'],d['beam']['cs']['meta']['id'])
        # check that units were converted
        self.assertEqual(d['beam']['length']['unit'],'m')
        self.assertEqual(d['beam']['length']['value'],2.5)
        # check type metadata
        self.assertEqual(d['meta']['type'],'BeamState')
        self.assertEqual(d['beam']['meta']['type'],'Beam')
    def test_03_tracking(self):
        C=self.__class__
        d=C.get(f'{DB}/BeamState/{C.ID_01}',meta=False,tracking=True)
        #pprint(d)
        # relative link is recovered via object tracking
        self.assertEqual(d['cs'],'.beam.cs')
        self.assertEqual(d['csState'][0]['rveStates'][0]['rve'],'...beam.cs.rve')
        # metadata not returned
        self.assertTrue('meta' not in d)
    def test_05_max_level(self):
        C=self.__class__
        d=C.get(f'{DB}/BeamState/{C.ID_01}',meta=False,tracking=True,max_level=0)
        self.assertTrue('cs' not in d)
        self.assertTrue('npointz' in d)
    def test_06_patch(self):
        C=self.__class__
        C.patch(f'{DB}/BeamState/{C.ID_01}',dict(path='beam.length',data={'value':0.005,'unit':'km'}))
        d=C.get(f'{DB}/BeamState/{C.ID_01}',meta=False)
        self.assertAlmostEqual(d['beam']['length']['value'],5,places=4)
        self.assertEqual(d['beam']['length']['unit'],'m')

    # @unittest.skip('Failing test (TODO)')
    def test_07_meta(self):
        C=self.__class__
        d=C.get(f'{DB}/BeamState/{C.ID_01}',meta=True,max_level=0)
        self.assertTrue('id' in d['meta'])
        d=C.get(f'{DB}/BeamState/{C.ID_01}',path='meta',meta=True)
        C.patch(f'{DB}/BeamState/{C.ID_01}',dict(path='meta',data={'tags':['asdf','ghjk']}))
        d=C.get(f'{DB}/BeamState/{C.ID_01}',path='meta',meta=True)
        #d2=C.get(f'{DB}/BeamState["asdf" in meta.tags]')
        #self.assertEqual(d,d2)
        # print(f'{d=}')



    def test_06_blob(self):
        C=self.__class__
        blob=os.urandom(1024*1024)
        id=C.post_data(f'{DB}/blob/upload','blob',blob)
        blob2=C.get_data(f'{DB}/blob/{id}')
        self.assertEqual(blob,blob2)

    # @unittest.skip('asas')
    def test_07_largedata(self):
        C=self.__class__
        schema={
            'TestLarge':{
                'arr': { 'dtype':'f', 'shape':[-1,100] }
            }
        }
        C.post(f'{DB1}/schema',body=schema,query={'force':True})
        # this is about 25MB worth of BSON, above the 16MB limit
        a0=np.random.rand(10000,100)
        ID=C.post(f'{DB1}/TestLarge',body={'arr':{'value':a0.tolist()}})
        #ID=C.post(f'{DB1}/TestLarge',body={'arr':{'value':[[1,2],[3,4]]}})
        a1=C.get(f'{DB1}/TestLarge/{ID}')
        # print(f'{list(a1.keys())=}')
        self.assertEqual(a0.shape,np.array(a1['arr']['value']).shape)



    def test_10_schema_post(self):
        C=self.__class__
        schema={
            'Test':{
                'prop': {
                    'unit':'m'
                    ,'implicit':{'DataID':'ID_Length','ClassName':'Property'}
                 },
                'astr': {'dtype':'str'},
                'anum': {'dtype':'i'},
                'sstr1': {'dtype':'str', 'shape':[-1]},
                'sstr22': {'dtype':'str', 'shape':[2,2]},
            }
        }
        C.post(f'{DB1}/schema',body=schema,query={'force':True})
        ID=C.post(f'{DB1}/Test',body={'prop':{'value':955,'unit':'mm'},'astr':{'value':'foo'},'anum':{'value':4},'sstr1':{'value':['foo','bar','baz']},'sstr22':{'value':[['foo','bar'],['baz','cha']]}})
        t=C.get(f'{DB1}/Test/{ID}')
        # pprint(t)
        self.assertEqual(t['prop']['DataID'],'ID_Length')
        self.assertEqual(t['prop']['ClassName'],'Property')
        self.assertEqual(t['sstr22']['value'],[['foo','bar'],['baz','cha']])
        self.assertRaises(RuntimeError,lambda: C.post(f'{DB1}/Test',body={'sstr22':[['foo','bar'],['baz']]}))
        self.assertRaises(RuntimeError,lambda: C.post(f'{DB1}/Test',body={'sstr22':[['foo'],['baz']]}))
        self.assertRaises(RuntimeError,lambda: C.post(f'{DB1}/Test',body={'sstr22':['foo','bar']}))

    def test_11_schema_graphviz(self):
        C=self.__class__
        # C.post(f'{DB1}/schema',body=dta,query={'force':True})
        gr=C.get(f'{DB1}/schema/graphviz')
        print(gr)
        self.assertTrue(gr.startswith('digraph '))




    def test_99_float_error(self):
        C=self.__class__
        beamDta={ # Beam 
            "length": { "value": 1, "unit":"km" },
            "height": { "value": 12.3456789, "unit":"cm" },
            "density": { "value": 3.456789, "unit":"g/cm3" },
        }
        ID=C.post(f'{DB}/Beam',body=beamDta)
        b=C.get(f'{DB}/Beam/{ID}',meta=False,tracking=False)
        #
        self.assertEqual(b['height']['unit'],'m')
        self.assertGreater(b['height']['value'],0.1234567)



class Test_Direct(unittest.TestCase):
    def test_01_list(self):
        C=self.__class__
        dms3.dms_api_object_post(db=DB,type='BeamState',data=dta)
        C.ID0=dms3.dms_api_object_list(db=DB,type='BeamState')[-1]
    def test_02_parse(self):
        C=self.__class__
        N=None
        for p,res in [
            ('a',        [('a',N,N,N)]),
            ('a.b',      [('a',N,N,N),('b',N,N,N)]),
            ('a[1].b',   [('a',1,N,N),('b',N,N,N)]),
            ('a[1].b[2]',[('a',1,N,N),('b',2,N,N)]),
            ('a[-1].b',  [('a',-1,N,N),('b',N,N,N)]),
            ('a[:]',     [('a',N,N,(N,N,N))]),
            ('a[::]',    [('a',N,N,(N,N,N))]),
            ('a[1:]',    [('a',N,N,(1,N,N))]),
            ('a[1::]',   [('a',N,N,(1,N,N))]),
            ('a[1:2]',   [('a',N,N,(1,2,N))]),
            ('a[1:2:]',  [('a',N,N,(1,2,N))]),
            ('a[1:2:3]', [('a',N,N,(1,2,3))]),
            ('a[:2:3]',  [('a',N,N,(N,2,3))]),
            ('a[1::3]',  [('a',N,N,(1,N,3))]),
            ('a[::3]',   [('a',N,N,(N,N,3))]),
            ('a[1,]',    [('a',N,[1],N)]),
            ('a[1,2]',   [('a',N,[1,2],N)]),
            ('a[1,2,3]', [('a',N,[1,2,3],N)]),
            ('a[1,].b[:3:-1].c[0].d', [('a',N,[1,],N),('b',N,N,(N,3,-1)),('c',0,N,N),('d',N,N,N)]),
        ]:
            pp=dms3._parse_path(p)
            self.assertEqual(len(res),len(pp))
            for r,p in zip(res,pp):
                self.assertEqual(r,(p.attr,p.index,p.multiindex,p.slice))
                self.assertTrue(p.filter is None)
        for f in ['a>3','a[45[45]]>3','np.sum(123)[233]+[34[1,2,3]]']:
            pp=dms3._parse_path(f'a[1,2,3].b[2].c[1:2|{f}]')
            self.assertEqual(pp[2].filter,f)
    def test_03_resolve(self):
        C=self.__class__
        # single expanded path (no wildcards)
        (R,)=dms3._resolve_path_head(DB,type='BeamState',id=C.ID0,path='cs.rve.origin')
        self.assertEqual(len(R.tail),1) # one tail element
        self.assertEqual(R.tail[0].attr,'origin')

        (R,)=dms3._resolve_path_head(DB,type='BeamState',id=C.ID0,path='cs.rve')
        self.assertEqual(len(R.tail),0)

        RR=dms3._resolve_path_head(DB,type='BeamState',id=C.ID0,path='csState[:]')
        self.assertEqual(len(RR),2)
        # test that step -1 counts backwards
        RR2=dms3._resolve_path_head(DB,type='BeamState',id=C.ID0,path='csState[::-1]')
        self.assertEqual(RR2[-1].id,RR[0].id)

        # each wildcard expands to two
        RR=dms3._resolve_path_head(DB,type='BeamState',id=C.ID0,path='csState[:].rveStates[:].rve.materials[:].name')
        self.assertEqual(len(RR),6)

    def test_04_get(self):
        C=self.__class__
        # path without wildcards: returns scalar value (a dict)
        M=dms3.dms_api_path_get(DB,type='BeamState',id=C.ID0,path='csState[0].bendingMoment')
        self.assertTrue(isinstance(M,dict))
        # path with wilcards: returns list (even if it is one-item only)
        M=dms3.dms_api_path_get(DB,type='BeamState',id=C.ID0,path='csState[0:1].bendingMoment')
        self.assertFalse(isinstance(M,dict))
        # combined wildcards: each [:] has 2 items, thus 6 is returned
        names=dms3.dms_api_path_get(DB,type='BeamState',id=C.ID0,path='csState[:].rveStates[:].rve.materials[:].name')
        self.assertEqual(names,3*[{'value':'mat0'},{'value':'mat1'}])
        # reverse slice and explicit reverse multiindex return the same
        a1=dms3.dms_api_path_get(DB,type='BeamState',id=C.ID0,path='csState[::-1].bendingMoment')
        a2=dms3.dms_api_path_get(DB,type='BeamState',id=C.ID0,path='csState[1,0].bendingMoment')
        self.assertEqual(a1,a2)
        # filtering
        a3=dms3.dms_api_path_get(DB,type='BeamState',id=C.ID0,path='csState[:|len(rveStates)<2]')
        self.assertEqual(len(a3),1)
        a4=dms3.dms_api_path_get(DB,type='BeamState',id=C.ID0,path='csState[:|all([rs.sigmaHom["value"]>80 for rs in rveStates])]')
        self.assertEqual(len(a4),1)
    def test_05_set(self):
        C=self.__class__
        ##
        ## setting plain value
        ##
        dms3.dms_api_object_patch(DB,type='BeamState',id=C.ID0,
            # plain (non-wildcard) path takes a single data dictionary
            patchData=dms3.PatchData(
                path='csState[0].bendingMoment',
                data={'value':44,'unit':'kN*m'}
            )
        )
        self.assertEqual(dms3.dms_api_path_get(DB,type='BeamState',id=C.ID0,path='csState[0].bendingMoment'),{'value':44.0,'unit':'kN m'})

        ##
        ## setting wildcard value
        ##
        # wildcard path expands to 2 csState's, thus data needs to provide two values
        dms3.dms_api_object_patch(DB,type='BeamState',id=C.ID0,
            patchData=dms3.PatchData(
                path='csState[0:2].bendingMoment',
                data=[
                    {'value':55,'unit':'kN*m'},
                    {'value':66,'unit':'kN*m'}
                ]
            )
        )
        self.assertEqual(dms3.dms_api_path_get(DB,type='BeamState',id=C.ID0,path='csState[0].bendingMoment'),{'value':55.,'unit':'kN m'})
        self.assertEqual(dms3.dms_api_path_get(DB,type='BeamState',id=C.ID0,path='csState[1].bendingMoment'),{'value':66.,'unit':'kN m'})
        # getting them with ::-1 should go backwards
        self.assertEqual(dms3.dms_api_path_get(DB,type='BeamState',id=C.ID0,path='csState[::-1].bendingMoment'),
            [{'value':66.,'unit':'kN m'},{'value':55.,'unit':'kN m'}]
        )

        ##
        ## input validation
        ##
        # too little arguments for expanded path
        self.assertRaises(ValueError,lambda:dms3.dms_api_object_patch(DB,type='BeamState',id=C.ID0,patchData=dms3.PatchData(
            path='csState[0:2].bendingMoment',
            data=[{'value':77,'unit':'kN*m'}]
        )))
        # dict instead of list for expanded path
        self.assertRaises(ValueError,lambda:dms3.dms_api_object_patch(DB,type='BeamState',id=C.ID0,patchData=dms3.PatchData(
            path='csState[0:1].bendingMoment',
            data={'value':88,'unit':'kN*m'}
        )))
        # error writing to list (csState) with non-wildcard path (must write csState[:]
        self.assertRaises(IndexError,lambda:dms3.dms_api_object_patch(DB,type='BeamState',id=C.ID0,patchData=dms3.PatchData(
            path='csState.bendingMoment',
            data=[]
        )))

    def test_06_filter(self):
        C=self.__class__
        # each wildcard expands to two (6 in total) but only material ending with 0 is filtered
        RR=dms3._resolve_path_head(DB,type='BeamState',id=C.ID0,path='csState[:].rveStates[:].rve.materials[:|name["value"].endswith("0")].name')
        self.assertEqual(len(RR),3)
        RR=dms3._resolve_path_head(DB,type='BeamState',id=C.ID0,path='csState[:|eps_axial["value"]<800]')
        self.assertEqual(len(RR),1)
        RR=dms3._resolve_path_head(DB,type='BeamState',id=C.ID0,path='csState[:].rveStates[:|sigmaHom["value"]<85]')
        self.assertEqual(len(RR),2)
        self.assertRaises(RuntimeError,lambda:dms3._resolve_path_head(DB,type='BeamState',id=C.ID0,path='csState[:|some_nonsense_filter]'))
    def test_06_meta(self):
        C=self.__class__
        r1=dms3._resolve_path_head(DB,type='BeamState',id=C.ID0,path='csState[:|"tag1" in meta.tags]')
        rBoth=dms3._resolve_path_head(DB,type='BeamState',id=C.ID0,path='csState[:|"tagCommon" in meta["tags"]]')
        self.assertEqual(len(r1),1)
        self.assertEqual(len(rBoth),2)



if __name__=='__main__':
    unittest.main()
