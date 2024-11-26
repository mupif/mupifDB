MAKEFLAGS+=-j7
TASKS=ns mongo mongo-setup rest web browse scheduler

.PHONY: $(TASKS)
run: $(TASKS)


# if any task fails, use kill -INT $(MAKPID) to teminate everything immediately
DIE := kill -INT $(shell echo $$PPID)


ns:
	python3 -m Pyro5.nameserver --port 11001 || $(DIE)
mongo:
	mkdir -p mongodb-tmp~ && /usr/bin/mongod --port 11002 --noauth --replSet=rs0  --enableMajorityReadConcern --dbpath=./mongodb-tmp~ --logpath=/dev/null --logappend || $(DIE)
mongo-setup:
	sleep 2; mongosh --port 11002 --quiet --eval "try { rs.status() } catch (err) { rs.initiate({_id:'rs0',members:[{_id:0,host:'127.0.0.1:11002'}]}) }"
rest:
	# MUPIFDB_DRY_RUN=1
	sleep 2 && cd mupifDB/api && MUPIFDB_MONGODB_PORT=11002 MUPIFDB_REST_SERVER=http://127.0.0.1:11003 MUPIFDB_LOG_LEVEL=DEBUG MUPIFDB_RESTAPI_HOST=localhost MUPIFDB_RESTAPI_PORT=11003 PYTHONPATH=../.. python3 main.py || $(DIE)
web:
	sleep 7 && cd webapi &&  MUPIFDB_MONGODB_PORT=11002 MUPIFDB_REST_SERVER=http://127.0.0.1:11003 MUPIFDB_WEB_FAKE_AUTH=1 FLASK_APP=index.py PYTHONPATH=.. python3 -m flask run --debug --no-reload --host 127.0.0.1 --port 11004 || $(DIE)
browse:
	sleep 9 # && xdg-open http://127.0.0.1:11004
scheduler:
	sleep 7 && MUPIF_LOG_LEVEL=DEBUG MUPIFDB_REST_SERVER=http://127.0.0.1:11003 MUPIF_NS=localhost:11001 PYTHONPATH=.. python3 -c 'from mupifDB import workflowscheduler as ws; ws.LOOP_SLEEP_SEC=5; ws.schedulerStatFile="./sched-stat.json"; ws.main()' || $(DIE)

kill:
	# fuser -k 11001/tcp; fuser -k 11002/tcp; fuser -k 11003/tcp; fuser -k 11004/tcp || true
	fuser --verbose -k 11001/tcp 11002/tcp 11003/tcp 11004/tcp
