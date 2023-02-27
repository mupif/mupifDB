import requests
from rich.pretty import pprint
from rich import print_json
import json
B='http://localhost:8080'

def get(p,no_show=False,**kw):
    r=requests.get(f'{B}/{p}',params=kw)
    pprint(r)
    data=json.loads(r.text)
    if not no_show: pprint(data)
    return data

# get schema
get('schema')
# list all object types in the schema
get('ls')
# list all ConcreteRVE objects (use the last one)
IDs=get('ConcreteRVE/ls',no_show=True)
if IDs:
    ID=IDs[0]
    # get the last inserted object (as dictionary)
    get(f'ConcreteRVE/{ID}/object')

    ## dot notation

    # get an object within the last inserted one (as dictionary)
    get(f'ConcreteRVE/{ID}/object',path='materials[0]')
    # get attribute (value) within the last inserted object
    get(f'ConcreteRVE/{ID}/attr',path='materials[0].name')

## create a new object (returns the ID)
r=requests.post(
    f'{B}/ConcreteRVE',
    json={
        "origin":{"value":[1,2,3],"unit":"mm"},
        "size":{"value":[1,2,3],"unit":"km"},
        "materials":[
            {"name":"mat1","props":{"origin":"CZ"}},
            {"name":"mat2","props":{"origin":"DE"}}
        ],
         "ct":{"id":"bar"} ### bytes not JSON-serializable: "image":bytes(range(70,80))
    }
)
pprint(r)
ID2=json.loads(r.text)
pprint(ID2)
get(f'ConcreteRVE/{ID2}/object')
