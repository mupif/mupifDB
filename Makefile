MAKEFLAGS+=-j6
TASKS=ns mongo rest web browse scheduler

.PHONY: $(TASKS)
run: $(TASKS)

ns:
	python3 -m Pyro5.nameserver --port 11001
mongo:
	mkdir -p mongodb-tmp~ && /usr/bin/mongod --port 11002 --noauth --dbpath=./mongodb-tmp~ --logpath=/dev/null --logappend
rest:
	# MUPIFDB_DRY_RUN=1
	sleep 2 && cd mupifDB/api && MUPIFDB_MONGODB_PORT=11002 MUPIFDB_REST_SERVER=http://127.0.0.1:11003 MUPIFDB_LOG_LEVEL=DEBUG MUPIFDB_RESTAPI_HOST=localhost MUPIFDB_RESTAPI_PORT=11003 PYTHONPATH=../.. python3 main.py
web:
	sleep 7 && cd webapi &&  MUPIFDB_MONGODB_PORT=11002 MUPIFDB_REST_SERVER=http://127.0.0.1:11003 MUPIFDB_WEB_FAKE_AUTH=1 FLASK_APP=index.py PYTHONPATH=.. python3 -m flask run --debug --no-reload --host 127.0.0.1 --port 11004
browse:
	sleep 9 # && xdg-open http://127.0.0.1:11004
scheduler:
	sleep 7 && MUPIF_LOG_LEVEL=DEBUG MUPIFDB_REST_SERVER=http://127.0.0.1:11003 MUPIF_NS=localhost:11001 PYTHONPATH=.. python3 -c 'from mupifDB import workflowscheduler as ws; ws.LOOP_SLEEP_SEC=5; ws.schedulerStatFile="./sched-stat.json"; ws.main()'
