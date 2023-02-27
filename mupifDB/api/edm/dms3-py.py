import sys,os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import dms3
from rich.pretty import pprint as print
B='http://localhost:8080'
DB='dms0'
ID0=dms3.dms_api_object_list(db=DB,type='BeamState')[0]
print(dms3._resolve_path_head(DB,type='BeamState',id=ID0,path='cs.rve.origin'))
print(dms3._resolve_path_head(DB,type='BeamState',id=ID0,path='cs.rve'))
print(dms3._resolve_path_head(DB,type='BeamState',id=ID0,path='csState[:].rveStates[:].rve.materials[:].name'))

print(dms3.dms_api_path_get(DB,type='BeamState',id=ID0,path='csState[:].rveStates[:].rve.materials[:].name'))

#for p in ['foo','foo.bar','foo[1].bar','foo[1].bar[2]','foo[-1].bar','foo[1:]','foo[1:4]','foo[1::3]','foo[::]','foo[:]','foo[::2]','foo[:4]','foo[::2]','foo[2::-1]']: print(f'{p} → {dms3._parse_path(p)}')
