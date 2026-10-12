MAKEFLAGS+=-j8
TASKS=ns mongo mongo-setup rest safeapi web browse scheduler

# Local development stack. Override any of these on the command line, e.g.
#   make run MONGOD=$HOME/.local/bin/mongod API_CREDENTIALS_FILE=$PWD/creds.json
# MONGOD: mongod binary; the replica set is initiated with pymongo, so mongosh is not needed
MONGOD ?= /usr/bin/mongod
PYTHON ?= python3
# JSON file with {"username":..,"password":..} of a REST API account, used by the scheduler
API_CREDENTIALS_FILE ?=

.PHONY: $(TASKS)
run: $(TASKS)


# if any task fails, use kill -INT $(MAKPID) to teminate everything immediately
DIE := kill -INT $(shell echo $$PPID)


ns:
	python3 -m Pyro5.nameserver --port 11001 || $(DIE)
mongo:
	mkdir -p mongodb-tmp~ && $(MONGOD) --port 11002 --noauth --replSet=rs0  --enableMajorityReadConcern --dbpath=./mongodb-tmp~ --logpath=/dev/null --logappend || $(DIE)
mongo-setup:
	sleep 2; $(PYTHON) tools/mongo_replset_init.py 127.0.0.1:11002 rs0
rest:
	# MUPIFDB_DRY_RUN=1
	sleep 2 && cd mupifDB/api && MUPIFDB_MONGODB_PORT=11002 MUPIFDB_REST_SERVER=http://127.0.0.1:11003 MUPIFDB_LOG_LEVEL=DEBUG MUPIFDB_RESTAPI_HOST=localhost MUPIFDB_RESTAPI_PORT=11003 PYTHONPATH=../.. python3 main.py || $(DIE)
safeapi:
	sleep 2 && cd mupifDB/api && MUPIF_VPN_NAME=local-test MUPIF_NS=localhost:11001 MUPIF_LOG_LEVEL=DEBUG PYTHONPATH=../.. python3 -m uvicorn safeapi:app --host localhost --port 11005
web:
	sleep 7 && cd webapi &&  MUPIFDB_MONGODB_PORT=11002 MUPIFDB_REST_SERVER=http://127.0.0.1:11003 MUPIFDB_WEB_FAKE_AUTH=1 FLASK_APP=index.py PYTHONPATH=.. python3 -m flask run --debug --no-reload --host 127.0.0.1 --port 11004 || $(DIE)
browse:
	sleep 9 # && xdg-open http://127.0.0.1:11004
scheduler:
	sleep 7 && API_CREDENTIALS_FILE=$(API_CREDENTIALS_FILE) MUPIF_LOG_LEVEL=DEBUG MUPIFDB_REST_SERVER=http://127.0.0.1:11003 MUPIF_NS=localhost:11001 PYTHONPATH=.. python3 -c 'from mupifDB import workflowscheduler as ws; ws.LOOP_SLEEP_SEC=5; ws.schedulerStatFile="./sched-stat.json"; ws.main()' || $(DIE)

kill:
	# fuser -k 11001/tcp; fuser -k 11002/tcp; fuser -k 11003/tcp; fuser -k 11004/tcp || true
	fuser --verbose -k 11001/tcp 11002/tcp 11003/tcp 11004/tcp 11005/tcp
