# MuPIFDB 

MuPIF is modular, object-oriented integration platform allowing to create complex, distributed, multiphysics simulation workflows across the scales and processing chains by combining existing simulation tools. MuPIFDB is database layer (based on MongoDB) and workflow manager/scheduler for MuPIF with REST API.

## Project website
http://www.mupif.org

## Prerequisites
* mongoDB server
* Python modules: pymongo, flask, bson, flask_pymongo, isodate, dateutil, python-pidfile, pygal
* for REST API debugging and development install chrome extension (ARC - Advanced REST client or Postman)

## Installation
*git clone https://github.com/mupif/mupifDB.git mupifDB.git*

## Running

### DB API

*```cd mupifDB/mupifDB/api```*

*```MUPIFDB_REST_SERVER_TYPE=mupif uvicorn main:app --reload --host 127.0.0.1 --port 8005```*

### WEB interface

*```cd mupifDB/webapi```*

*```MUPIF_NS=127.0.0.1:10000 MUPIFDB_REST_SERVER=http://127.0.0.1:8005 FLASK_APP=index.py MUPIFDB_REST_SERVER_TYPE=mupif python3 -m flask run --host 127.0.0.1 --port 5555```*

### Workflow execution scheduler

*```cd mupifDB/mupifDB```*

*```MUPIFDB_REST_SERVER=http://127.0.0.1:8005 MUPIFDB_REST_SERVER_TYPE=mupif MUPIF_NS=127.0.0.1:10000 python3 workflowscheduler.py```*

### Other

To support statistics graphs (using schedulerstat.py), the schedulerstat.py should be run periodically (using cron) to regenerate the charts. Dynamic generation is costly. Suggested crontab entry to update charts every 5 minutes:

**/5 * * * * python3 mupifDB/schedulerstat.py -w -h*






## License
MuPIF has been developed at Czech Technical University by Borek Patzak and coworkers and is available under GNU Library or Lesser General Public License version 3.0 (LGPLv3).

## Further information
Please consult MuPIF home page (http://www.mupif.org) for additional information.
