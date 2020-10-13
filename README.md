# MuPIFDB 

MuPIF is modular, object-oriented integration platform allowing to create complex, distributed, multiphysics simulation workflows across the scales and processing chains by combining existing simulation tools. MuPIFDB is database layer (based on MongoDB) and workflow manager for MuPIF with REST API.

## Project website
http://www.mupif.org

## Prerequisites
* mongoDB server
* Python modules: pymongo, flask, bson, flask_pymongo, isodate, dateutil, python-pidfile, pygal
* for REST API debugging and development install chrome extension (ARC - Advanced REST client or Postman)

## Instalation
*git clone https://github.com/mupif/mupifDB.git mupifDB.git*

## Runnig
*export FLASK_APP=mupifdbRestApi.py*
*nohup python3 -m flask run --host 172.30.0.1 &*
To support statistics graphs (using schedulerstat.py), the schedulerstat.py should be run periodically (using cron) to regenerate the charts. Dynamic generation is costly. Suggested crontab entry to update charts every 5 minutes:
**/5 * * * * python3 /home/bp/mupifDB/schedulerstat.py -w -h*






## License
MuPIF has been developped at Czech Technical University by Borek Patzak and coworkers and is available under GNU Library or Lesser General Public License version 3.0 (LGPLv3).

## Further information
Please consult MuPIF home page (http://www.mupif.org) for additional information.
