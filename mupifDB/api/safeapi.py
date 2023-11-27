import fastapi
import mupif as mp

app = fastapi.FastAPI(openapi_tags=[{'name':'Stats'}])

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

