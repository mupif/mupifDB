import fastapi
import pymongo
import Pyro5.api
import mupif as mp
import os

app = fastapi.FastAPI(openapi_tags=[{'name':'Stats'}])

mongoClient = pymongo.MongoClient("mongodb://localhost:27017")



@app.get("/status2/", tags=["Stats"])
def get_status2():
    ns = None
    try:
        ns = mp.pyroutil.connectNameserver();
        nameserverStatus = 'OK'
    except:
        nameserverStatus = 'Failed'

    # get Scheduler status
    schedulerStatus = 'Failed'
    query = ns.yplookup(meta_any={"type:scheduler"})
    try:
        for name, (uri, metadata) in query.items():
            s = Pyro5.api.Proxy(uri)
            st = s.getStatistics()
            schedulerStatus = 'OK'
    except Exception as e:
        print(str(e))

    # get DMS status
    if mongoClient: DMSStatus = 'OK'
    else: DMSStatus = 'Failed'

    return {'nameserver': nameserverStatus, 'dms': DMSStatus, 'scheduler': schedulerStatus, 'name':os.environ["MUPIF_VPN_NAME"]}


@app.get("/scheduler-status2/", tags=["Stats"])
def get_scheduler_status2():
    ns = mp.pyroutil.connectNameserver();
    return mp.monitor.schedulerInfo(ns)

@app.get("/ns-status2/", tags=["Stats"])
def get_ns_status2():
    ns = mp.pyroutil.connectNameserver();
    return mp.monitor.nsInfo(ns)

@app.get("/vpn-status2/", tags=["Stats"])
def get_vpn_status2():
    return mp.monitor.vpnInfo(hidePriv=False)

@app.get("/jobmans-status2/", tags=["Stats"])
def get_jobmans_status2():
    ns = mp.pyroutil.connectNameserver();
    return mp.monitor.jobmanInfo(ns)


if __name__ == '__main__':
    import uvicorn
    uvicorn.run('safeapi:app', host='0.0.0.0', port=8081, reload=True)

