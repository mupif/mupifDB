from ast import literal_eval


if __name__ == "__main__":

    # mongodb conversion
    import bson
    from pymongo import MongoClient
    client = MongoClient()
    db = client.MuPIF

    table = db.IOData
    for s in table.find():
        for i in s['DataSet']:
            temp = i.get('Object', {})
            fileid = i.get('FileID', None)
            if fileid is not None:
                temp['FileID'] = fileid
            else:
                link = i.get('Link', None)
                doit = False
                if link is None:
                    doit = True
                elif link['ExecID'] == '':
                    doit = True
                if doit:
                    temp['ClassName'] = 'ConstantProperty'
                    temp['ValueType'] = i.get('ValueType', '')
                    temp['DataID'] = i.get('TypeID', '').replace('mupif.DataID.', '')
                    temp['Unit'] = i.get('Units', '')
                    val = i.get('Value', None)
                    if val is not None:
                        try:
                            val = literal_eval(val)
                        except:
                            print('A problem occured in ' + str(i))
                    temp['Value'] = val
                    temp['Time'] = i.get('Time', None)

            table.update_one({'_id': bson.objectid.ObjectId(s['_id'])}, {'$set': {"DataSet.$[r].Object": temp}}, array_filters=[{"r.Name": i['Name'], "r.ObjID": i['ObjID']}])
