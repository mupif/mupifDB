import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")
import mupif


def getEDMPropertyInstance(edm_data, data_id, value_type) -> mupif.ConstantProperty:
    obj = None
    if value_type in ['Scalar', 'Vector', 'Tensor']:
        value = edm_data.get('value', None)
        unit = edm_data.get('unit', '')
        obj = mupif.ConstantProperty.from_db_dict({
            "Value": value,
            "DataID": data_id,
            "ValueType": value_type,
            "Unit": unit,
            "Time": None
        })
    elif  value_type in ['ScalarArray', 'VectorArray', 'TensorArray']:
        raise ValueError('Handling of Onto input Property of ValueType %s is not implemented' % value_type)
    return obj


def getEDMStringInstance(edm_data, data_id, value_type) -> mupif.String:
    value = edm_data.get('value', None)
    unit = edm_data.get('unit', '')
    obj = mupif.String.from_db_dict({
        "Value": value,
        "DataID": data_id,
        "ValueType": value_type
    })
    return obj


def getEDMTemporalPropertyInstance(edm_data, data_id, value_type) -> mupif.TemporalProperty:
    obj = mupif.DbDictable.from_db_dict(edm_data, dialect='edm')
    return obj
