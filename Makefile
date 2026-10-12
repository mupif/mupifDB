MAKEFLAGS+=-j8
TASKS=ns mongo mongo-setup rest safeapi web browse scheduler

# Local development stack. Override any of these on the command line, e.g.
#   make run MONGOD=$HOME/.local/bin/mongod API_CREDENTIALS_FILE=$PWD/creds.json
# MONGOD: mongod binary; the replica set is initiated with pymongo, so mongosh is not needed
MONGOD ?= /usr/bin/mongod
PYTHON ?= python3
# JSON file with {"username":..,"password":..} of a REST API account, used by the scheduler and the smoke test
API_CREDENTIALS_FILE ?=
# checkout of the mupif repository, needed for the demo model server (examples are not installed with the package)
MUPIF_DIR ?= ../mupif
# extra arguments for tools/scheduler_smoketest.py, e.g. SMOKETEST_ARGS="--count 12 --delay 15 --max-concurrent 4"
SMOKETEST_ARGS ?= --register --count 3 --delay 2
# files which must have complete docstrings (checked by "make lint-docs"); extended as code gets documented
DOCLINT_PATHS = tools/scheduler_smoketest.py tools/mongo_replset_init.py mupifDB/test_scheduler_unit.py

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

# Not part of "run": model server from mupif example 13, registered as CVUT.demo01 (maxJobs=4);
# it serves the model used by the demo workflow Workflow01 (mupifDB/api/demo/workflowdemo01.py)
modelserver13:
	mkdir -p modelserver13-tmp~ && cd modelserver13-tmp~ && MUPIF_NS=localhost:11001 $(PYTHON) $(abspath $(MUPIF_DIR))/examples/13/server.py

# End-to-end scheduler test against the running stack (needs "run" and "modelserver13")
smoketest:
	API_CREDENTIALS_FILE=$(API_CREDENTIALS_FILE) MUPIFDB_REST_SERVER=http://127.0.0.1:11003 MUPIF_LOG_LEVEL=WARNING $(PYTHON) tools/scheduler_smoketest.py $(SMOKETEST_ARGS)

# Docstring check of the files listed in DOCLINT_PATHS (needs ruff)
lint-docs:
	ruff check --config tools/ruff-docstrings.toml $(DOCLINT_PATHS)

.PHONY: modelserver13 smoketest lint-docs

kill:
	# fuser -k 11001/tcp; fuser -k 11002/tcp; fuser -k 11003/tcp; fuser -k 11004/tcp || true
	fuser --verbose -k 11001/tcp 11002/tcp 11003/tcp 11004/tcp 11005/tcp
