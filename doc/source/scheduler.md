# Workflow scheduler

The scheduler (`mupifDB/workflowscheduler.py`) runs workflow executions submitted to the database. It is a background service of the central node (started by supervisor in the deployment container); users never talk to it directly. They create an execution, set its inputs and schedule it through the web interface or the REST API; the scheduler picks it up, runs the workflow and stores the result.

There is one scheduler per deployment.

## Life of an execution

| Status | Meaning | Set by |
|---|---|---|
| `Created` | being prepared by the user (inputs not complete) | user |
| `Pending` | submitted, waiting for the scheduler | user (`schedule`), scheduler (requeue) |
| `Scheduled` | taken by the scheduler, queued in its worker pool | scheduler |
| `Running` | the workflow process runs | scheduler, execution script |
| `Finished` / `Failed` | done | scheduler, execution script |

Transitions made by the scheduler:

- `Pending → Scheduled`: the execution is taken and handed to the worker pool (only if the status change succeeded).
- `Scheduled → Running`: a worker starts the workflow process.
- `Running → Finished | Failed | Pending`: according to the exit code of the workflow process (see below).
- `Scheduled → Pending`: a worker failed before the workflow process was started (e.g. REST API unreachable); the execution is tried again.
- `Pending → Created`: the execution could not be started after 12 attempts because the model servers it needs were unreachable; the user is notified by e-mail (if notifications are configured).
- at startup, `Scheduled`/`Running → Pending`: executions interrupted by a previous scheduler run (crash, kill) are run again, unless their workflow process is still alive.

## Main loop

Every `LOOP_SLEEP_SEC` seconds (20 s) the scheduler:

1. reads up to 300 `Pending` executions, **oldest submitted first** (`GET /executions?status=Pending&sort=SubmittedDate`);
2. for each, reserves one slot on the model server (jobmanager) of every model of its workflow. The free slots of each model server are read once per iteration and consumed by the executions taken in that iteration, so no more executions are started than the model servers can serve:
   - all slots available: the execution is set `Scheduled` and handed to the worker pool;
   - a model server is busy (too few free slots): the execution waits for the next iteration; waiting does not count as an attempt;
   - a model server (or the workflow record) is unreachable: the attempt count is increased; after 12 attempts the execution returns to `Created`;
3. stores its statistics.

An error in one iteration (e.g. the REST API temporarily unreachable) is logged and the loop continues with the next one.

## Running a workflow

A worker of the pool (30 processes) creates a temporary directory, downloads the workflow package (a `.py` file or a `.zip` archive) into it and runs `workflow_execution_script.py -eid <execution ID>` in it with the scheduler's Python interpreter. The script imports the workflow class, maps the inputs, solves the workflow and maps the outputs. Its exit code tells the scheduler how it ended:

| Exit code | Meaning | Final status |
|---|---|---|
| 0 | workflow finished | `Finished` |
| 2 | a model server had no free slot (another client took it meanwhile) | `Pending` (tried again, attempt count kept) |
| 1, other, or killed by a signal | workflow failed | `Failed` |

The workflow output is stored in the database as the execution log (`ExecutionLog`), then the final status is set and the owner of the execution is notified by e-mail (if notifications are configured: `webUrl` setting and SMTP credentials in `/var/lib/mupif/persistent/mupif-smtp-credentials.json`).

## Statistics

The scheduler registers its monitor in the Pyro nameserver as `mupif.scheduler` (metadata `type:scheduler`); `getStatistics()` returns the counters shown by the monitoring pages:

- `scheduledTasks` – executions in the worker pool, not started yet; `runningTasks` – running workflow processes;
- `processedTasks` = `finishedTasks` + `failedTasks` – executions that reached a final status (an execution returned to `Pending` is not counted);
- hourly history of the last 48 hours (`pooledTasks48`: executions handed to the pool, `processedTasks48`, `finishedTasks48`, `failedTasks48`, `load48`).

The totals and the history are kept across restarts in `/var/lib/mupif/persistent/scheduler-stat.json` and copied to the database (`GET /scheduler_statistics`).

## Configuration

| Setting | Default | Meaning |
|---|---|---|
| `MUPIF_NS` | – | Pyro nameserver (`host:port`) |
| `MUPIFDB_REST_SERVER` | `http://127.0.0.1:8005` | REST API |
| `API_CREDENTIALS_FILE` | – | JSON file with `username`/`password` of the account the scheduler uses for the REST API; the client logs in again automatically when the API rejects its token |
| `LOOP_SLEEP_SEC` | 20 | seconds between iterations of the main loop |
| `STARTUP_DELAY_SEC` | 5 | delay before the scheduler starts working, giving the other services time to come up |
| `poolsize` | 30 | number of worker processes, i.e. executions running at the same time |
| `schedulerStatFile` | `/var/lib/mupif/persistent/scheduler-stat.json` | persistent statistics |

The last four are module-level settings of `workflowscheduler.py`.

## Testing locally

The `Makefile` of the repository starts a complete local stack:

```shell
make run MONGOD=/path/to/mongod API_CREDENTIALS_FILE=$PWD/creds.json   # nameserver, MongoDB, REST API, web, scheduler
make modelserver13 MUPIF_DIR=../mupif                                   # model server CVUT.demo01 (4 slots) from mupif example 13
make smoketest SMOKETEST_ARGS="--register --count 12 --delay 15 --max-concurrent 4 --check-order"
```

`tools/scheduler_smoketest.py` submits executions of the demo workflow `Workflow01`, waits until all of them are final and checks their results, logs, concurrency and start order; see its `--help` for the scenarios. Unit tests: `pytest mupifDB/test_scheduler_unit.py mupifDB/test_client_auth.py`; integration tests (start MongoDB, the REST API, a nameserver and the scheduler): `pytest mupifDB/test_scheduler.py`. Results of the scenarios before and after the refactoring are in [](scheduler-smoketest-results.md).

## Known limitations

- Only one scheduler may run per deployment: executions are taken with a plain status change, not an atomic claim, so two schedulers could run the same execution twice.
- When a workflow process dies abnormally, the model-server jobs it allocated are not terminated and keep their slots.
- Several instances of the same model server cannot share the load: they would register under the same name in the nameserver, and the last one registered replaces the others.
- Shutdown (SIGTERM) does not stop running workflow processes and may hang while waiting for the worker pool.

These are addressed by the ongoing refactoring (see the changelog).
