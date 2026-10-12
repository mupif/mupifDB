# Workflow scheduler

The scheduler runs workflow executions submitted to the database. It is a background service of the central node (started by supervisor in the deployment container as `mupifDB/workflowscheduler.py`, or with `python -m mupifDB.scheduler`); users never talk to it directly. They create an execution, set its inputs and schedule it through the web interface or the REST API; the scheduler picks it up, runs the workflow and stores the result.

There is one scheduler per deployment. It is a single process: executions are run by job threads, each starting the workflow as a subprocess.

## Life of an execution

| Status | Meaning | Set by |
|---|---|---|
| `Created` | being prepared by the user (inputs not complete) | user |
| `Pending` | submitted, waiting for the scheduler | user (`schedule`), scheduler (requeue) |
| `Scheduled` | taken by the scheduler, its workflow about to start | scheduler |
| `Running` | the workflow process runs | scheduler |
| `Finished` / `Failed` | done; the log is stored before the status is set | scheduler |

Transitions made by the scheduler:

- `Pending → Scheduled`: the execution is taken (only if the status change succeeded).
- `Scheduled → Running`: its workflow process starts; the attempt count is increased.
- `Running → Finished | Failed`: according to the exit code of the workflow process (see below), after its log was stored.
- `Running → Pending`: the workflow found no free model-server slot when it started, or the scheduler is shutting down; the execution runs again later (attempt count kept).
- `Scheduled → Failed`: compulsory inputs are not set, or the workflow package cannot be used (wrong module name, unsupported file type, unsafe archive). The execution is not started; for missing inputs a short log says why.
- `Scheduled → Pending`: a transient error before the workflow started (e.g. REST API unreachable).
- `Pending → Created`: the model servers the execution needs stayed unreachable for 12 attempts; the owner is notified by e-mail (if notifications are configured).
- at startup, `Scheduled`/`Running → Pending`: executions interrupted by a previous scheduler run (crash, kill) are run again; model-server jobs left by their workflows are terminated first.

The owner of an execution is notified by e-mail of its final status (if notifications are configured: `webUrl` setting and SMTP credentials in `/var/lib/mupif/persistent/mupif-smtp-credentials.json`).

## Main loop

Every `poll_interval` seconds (20 s) the scheduler takes pending executions, **oldest submitted first**, at most as many as it has free job slots (`max_jobs`, 30). For each candidate it reserves one slot on the model server (jobmanager) of every model of the workflow. The free slots of each model server are read once per pass and consumed by the executions taken in that pass, so no more executions are started than the model servers can serve:

- all slots available: the execution is taken (`Scheduled`) and handed to a job thread;
- a model server is busy (too few free slots): the execution waits for a later pass; this is normal queueing and does not count as an attempt;
- a model server (or the workflow record) is unreachable: the attempt count is increased and the execution is retried after a delay.

**Retries.** After a transient failure (model server unreachable, no free slot when the workflow started, REST API error before the start) an execution is not tried again before a delay of `retry_backoff` (10 s), doubled after every further transient failure up to `retry_backoff_max` (10 min), with ±20 % jitter. With the defaults an unreachable model server returns its executions to `Created` after about an hour. Permanent failures (workflow error, timeout, missing inputs, unusable package) are not retried.

An error in one pass (e.g. the REST API temporarily unreachable) is logged and the loop continues with the next one.

## Running a workflow

A job thread creates a temporary directory under `work_dir`, stages the workflow package (a `.py` file or a `.zip` archive with the module at its top level; packages are cached, see `cache_max_bytes`) and the execution script into it, and runs `workflow_execution_script.py -eid <execution ID>` there with the scheduler's Python interpreter, in a process group of its own. The script imports the workflow class, maps the inputs, solves the workflow and maps the outputs. Its exit code tells the scheduler how it ended:

| Exit code | Meaning | Final status |
|---|---|---|
| 0 | workflow finished | `Finished` |
| 2 | a model server had no free slot (another client took it meanwhile) | `Pending` (retried after a delay) |
| 143 | stopped by the scheduler (timeout, shutdown); the workflow and its models were terminated | `Failed`, or `Pending` at shutdown |
| 1, other, or killed by a signal | workflow failed | `Failed` |

The workflow output is stored with the execution as its log (`ExecutionLog`).

**Timeout.** An execution may run for the `Timeout` declared in its workflow metadata (seconds), or for `default_job_timeout` if the workflow declares none (default: no limit). When exceeded, the workflow gets SIGTERM, which makes the execution script terminate the workflow and its models, then SIGKILL after `kill_grace` seconds; the execution fails. Workflows registered before the scheduler refactoring have no stored timeout until they are uploaded again.

**Model-server jobs left behind.** If a workflow process dies without terminating its models (killed, crashed), the scheduler terminates the model-server jobs belonging to the execution after its run, so they do not keep their slots.

**Debugging failures.** With `keep_failed_workdirs` set, the working directories of the most recent failed executions are kept in `<work_dir>/failed/<execution ID>`.

## Stopping, restarting and maintenance

- **SIGTERM / SIGINT** (e.g. `supervisorctl stop`): the scheduler takes no new executions, waits up to `drain_timeout` (default 0 s) for running workflows, then stops them (SIGTERM, SIGKILL after `kill_grace` = 5 s). Their executions return to `Pending` with their attempt count and run again after the restart. The shutdown takes `drain_timeout + kill_grace` plus a few seconds; keep this below the stop timeout of the service manager (supervisor `stopwaitsecs`, default 10 s), or increase `stopwaitsecs` together with `drain_timeout`.
- **SIGKILL** or a crash: the workflow processes end with the scheduler (they ask the kernel for a signal when it dies). After the restart their executions are found `Running`, their model-server jobs are terminated and they run again.
- **Drain mode**, for maintenance without interrupting workflows: `kill -USR1 <scheduler PID>` (toggle), or `setDrain(True)` on the scheduler's Pyro object from the scheduler's host. While draining no new executions are taken; running ones finish. `getStatistics()` reports `draining`. Stop the scheduler once `runningTasks` is 0.

## Statistics

The scheduler registers its monitor in the Pyro nameserver as `mupif.scheduler` (metadata `type:scheduler`); `getStatistics()` returns the counters shown by the monitoring pages:

- `scheduledTasks` – executions taken and not started yet; `runningTasks` – running workflow processes;
- `processedTasks` = `finishedTasks` + `failedTasks` – executions that reached a final status (an execution returned to `Pending` is not counted);
- `currentLoad` – percentage of `max_jobs` in use;
- hourly history of the last 48 hours (`pooledTasks48`: executions taken, `processedTasks48`, `finishedTasks48`, `failedTasks48`, `load48`);
- `draining` – drain mode.

The totals and the history are kept across restarts in the statistics file and copied to the database (`GET /scheduler_statistics`). Over Pyro, `stop()` and `setDrain()` are accepted only from the scheduler's host.

Log records of the scheduler carry the ID of the execution they concern (`weid` attribute; job threads are named `job:<execution ID>`).

## Configuration

All settings can be set by environment variables; `python -m mupifDB.scheduler --print-config` prints them with their current values.

| Variable | Default | Meaning |
|---|---|---|
| `MUPIF_NS` | – | Pyro nameserver (`host:port`) |
| `MUPIFDB_REST_SERVER` | `http://127.0.0.1:8005` | REST API |
| `API_CREDENTIALS_FILE` | – | JSON file with `username`/`password` of the account the scheduler uses for the REST API; the client logs in again automatically when the API rejects its token |
| `MUPIFDB_SCHEDULER_SCHEDULER_ID` | host name and process ID | Identifier of this scheduler process. |
| `MUPIFDB_SCHEDULER_MAX_JOBS` | 30 | Maximum number of executions run at the same time. |
| `MUPIFDB_SCHEDULER_POLL_INTERVAL` | 20 | Seconds between two passes over the pending executions. |
| `MUPIFDB_SCHEDULER_STARTUP_DELAY` | 5 | Seconds to wait at startup, giving the other services of a freshly started deployment time to come up. |
| `MUPIFDB_SCHEDULER_MAX_ATTEMPTS` | 12 | Attempts after which an execution whose resources stay unavailable returns to `Created`. |
| `MUPIFDB_SCHEDULER_FETCH_LIMIT` | 300 | Maximum number of pending executions read from the database per pass. |
| `MUPIFDB_SCHEDULER_WORK_DIR` | system temporary directory | Directory in which a temporary working directory is created for each execution. |
| `MUPIFDB_SCHEDULER_RETRY_BACKOFF` | 10 | Seconds before an execution is tried again after a transient failure; doubled after every further one. |
| `MUPIFDB_SCHEDULER_RETRY_BACKOFF_MAX` | 600 | Upper limit of the retry delay in seconds. |
| `MUPIFDB_SCHEDULER_RETRY_JITTER` | 0.2 | Random variation of the retry delay (fraction). |
| `MUPIFDB_SCHEDULER_DEFAULT_JOB_TIMEOUT` | 0 | Maximum run time of an execution in seconds when its workflow declares none; 0: no limit. |
| `MUPIFDB_SCHEDULER_KILL_GRACE` | 5 | Seconds a stopped workflow gets between SIGTERM and SIGKILL (to terminate its models). |
| `MUPIFDB_SCHEDULER_DRAIN_TIMEOUT` | 0 | On shutdown, seconds to let running workflows finish before they are stopped. |
| `MUPIFDB_SCHEDULER_KEEP_FAILED_WORKDIRS` | 0 | Number of working directories of failed executions kept in `<work_dir>/failed` (0: none). |
| `MUPIFDB_SCHEDULER_CACHE_DIR` | `<work_dir>/mupifDB-package-cache` | Directory caching downloaded workflow packages. |
| `MUPIFDB_SCHEDULER_CACHE_MAX_BYTES` | 1 GiB | Size of the package cache above which the least recently used packages are removed (0: no cache). |
| `MUPIFDB_SCHEDULER_STAT_FILE` | `/var/lib/mupif/persistent/scheduler-stat.json` | File keeping the statistics across restarts. |
| `MUPIFDB_SCHEDULER_PIDFILE` | `mupifDB_scheduler_pidfile` | PID file preventing a second scheduler; the REST API reads it (relative to its working directory) to report whether the scheduler runs. |

The structure of the code is described in the [API reference](scheduler-api.rst).

## Testing locally

The `Makefile` of the repository starts a complete local stack:

```shell
make run MONGOD=/path/to/mongod API_CREDENTIALS_FILE=$PWD/creds.json   # nameserver, MongoDB, REST API, web, scheduler
make modelserver13 MUPIF_DIR=../mupif                                   # model server CVUT.demo01 (4 slots) from mupif example 13
make smoketest SMOKETEST_ARGS="--register --count 12 --delay 15 --max-concurrent 4 --check-order"
```

`tools/scheduler_smoketest.py` submits executions of the demo workflow `Workflow01`, waits until all of them are final and checks their results, logs, concurrency and start order; see its `--help` for the scenarios. Unit tests (no services needed): `pytest mupifDB/scheduler/tests mupifDB/test_client_auth.py mupifDB/test_client_files.py`; integration tests (start MongoDB, the REST API, a nameserver and the scheduler): `pytest mupifDB/test_scheduler.py`. Results of the scenarios before and during the refactoring are in [](scheduler-smoketest-results.md).

## Known limitations

- Only one scheduler may run per deployment: executions are taken with a plain status change, not an atomic claim, so two schedulers could run the same execution twice.
- Several instances of the same model server cannot share the load: they would register under the same name in the nameserver, and the last one registered replaces the others.
- Retry delays are kept in memory: after a restart, executions are retried at once.

These are addressed by the next phases of the refactoring (see the changelog).
