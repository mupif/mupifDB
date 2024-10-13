import pytest
import sys
import time
import socket
from rich.pretty import pprint
from rich import print_json

import logging
logging.basicConfig()
log=logging.getLogger()


from pathlib import Path
#sys.path.append(Path(__file__).absolute()/'..')

from . import models

from bson.objectid import ObjectId

sampleId=ObjectId('6709724d88e11a5a42f7caea')

class TestModels:
    def test_01_strictbase(self):
        with pytest.raises(TypeError): models.StrictBase.model_validate(foo='bar')
    def test_02_mongoobj(self):
        # test DatabasID annotated type (converts bson.objectid.ObjectId to str, when loading from database)
        m=models.MongoObj_Model.model_validate(dict(_id=sampleId))
        # passing _id from python side (not as dict, e.g. from db) is an error, since python uses dbID
        with pytest.raises(TypeError): models.MongoObj_Model.model_validate(_id=sampleId)
        assert m.dbID==str(sampleId)
        assert type(m.dbID)==str
        # dbID attribute is dumped as _id, automatically (StrictBase defined model_dump with by_alias=True by default)
        assert '_id' in m.model_dump() and 'dbID' not in m.model_dump()
        # model_dump_db: don't remove _id when not None
        assert '_id' in m.model_dump_db()
        m2=models.MongoObj_Model()
        # but here _id is None (default), so will not be in the dump at all
        assert '_id' not in m2.model_dump_db()

