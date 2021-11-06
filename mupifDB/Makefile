MAKEFLAGS+=-j2
TASKS=rest web

.PHONY: $(TASKS)
run: $(TASKS)


rest:
	FLASK_APP=mupifdbRestApi.py PYTHONPATH=/home/eudoxos:/home/eudoxos/mp python3 -m flask run --host 127.0.0.1 --port 5000
web:
	cd webapi && MUPIFDB_REST_SERVER=http://127.0.0.1:5000 FLASK_APP=index.py PYTHONPATH=/home/eudoxos:/home/eudoxos/mp python3 -m flask run --host 127.0.0.1 --port 5555

