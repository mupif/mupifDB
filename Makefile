MAKEFLAGS+=-j3
TASKS=rest web browse

.PHONY: $(TASKS)
run: $(TASKS)


rest:
	cd mupifDB && FLASK_APP=mupifdbRestApi.py PYTHONPATH=.. python3 -m flask run --host 127.0.0.1 --port 5000
web:
	cd webapi && MUPIFDB_REST_SERVER=http://127.0.0.1:5000 FLASK_APP=index.py PYTHONPATH=.. python3 -m flask run --host 127.0.0.1 --port 5555
browse:
	sleep 2 && xdg-open http://127.0.0.1:5555
