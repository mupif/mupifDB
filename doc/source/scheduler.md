# Workflow scheduler

The scheduler runs workflow executions submitted to the database. It is a background service of the central node (started by supervisor in the deployment container as `mupifDB/workflowscheduler.py`, or with `python -m mupifDB.scheduler`); users never talk to it directly. They create an execution, set its inputs and schedule it through the web interface or the REST API; the scheduler picks it up, runs the workflow and stores the result.

Several schedulers may run at once, normally on several hosts, for redundancy and capacity (see [Several schedulers](#several-schedulers)); each execution is run by exactly one of them. A scheduler is a single process: executions are run by job threads, each starting the workflow as a subprocess.

## Life of an execution

| Status | Meaning | Set by |
|---|---|---|
| `Created` | being prepared by the user (inputs not complete) | user |
| `Pending` | submitted, waiting for the scheduler | user (`schedule`), scheduler (requeue) |
| `Scheduled` | taken by the scheduler, its workflow about to start | scheduler |
| `Running` | the workflow process runs | scheduler |
| `Finished` / `Failed` | done; the log is stored before the status is set | scheduler |

Transitions made by the scheduler:

- `Pending → Scheduled`: the execution is claimed under a lease (fails if another scheduler claimed it first).
- `Scheduled → Running`: its workflow process starts; the attempt count is increased.
- `Running → Finished | Failed`: according to the exit code of the workflow process (see below), after its log was stored.
- `Running → Pending`: the workflow found no free model-server slot when it started (the attempt is not counted; retried after a delay), or the scheduler is shutting down; the execution runs again later.
- `Scheduled → Failed`: compulsory inputs are not set, or the workflow package cannot be used (wrong module name, unsupported file type, unsafe archive). The execution is not started; for missing inputs a short log says why.
- `Scheduled → Pending`: a transient error before the workflow started (e.g. REST API unreachable).
- `Pending → Created`: the model servers the execution needs stayed unreachable for 12 attempts; the owner is notified by e-mail (if notifications are configured).
- `Pending → Failed`: the execution asks for an execution profile its workflow does not have; a short log says why.
- `Scheduled`/`Running → Pending` after a takeover: the execution's scheduler died (crash, kill, host lost); once its lease expired, another scheduler (or the same one, restarted) takes the execution over, terminates the model-server jobs left by its workflow and requeues it (attempt count kept).

`Pending → Created` and `Pending → Failed` claim the execution first, so that only one scheduler does it and the owner gets one notification.

The owner of an execution is notified by e-mail of its final status (if notifications are configured: `webUrl` setting and SMTP credentials in `/var/lib/mupif/persistent/mupif-smtp-credentials.json`).

## Main loop

Every `poll_interval` seconds (20 s) the scheduler takes pending executions, **oldest submitted first**, at most as many as it has free job slots (`max_jobs`, 30). For each candidate it counts one slot for every remote model of the workflow on a compatible model server (see [Model servers](#model-servers-instances-and-execution-profiles)). Models marked `Instantiate: false` are templates which the workflow allocates itself later, maybe several times (e.g. one per loop iteration): for them no slot is counted, a compatible server only has to be registered and reachable. The compatible servers and their free slots are read once per pass and consumed by the executions taken in that pass, so no more executions are started than the model servers can serve:

- a slot for every model: the execution is taken (`Scheduled`) and handed to a job thread;
- the compatible model servers are busy (too few free slots): the execution waits for a later pass; this is normal queueing and does not count as an attempt;
- no compatible model server of some model is registered and reachable (or the workflow record cannot be read): the attempt count is increased and the execution is retried after a delay.

**Retries.** After a transient failure (model server unreachable, no free slot when the workflow started, REST API error before the start) an execution is not tried again before a delay of `retry_backoff` (10 s), doubled after every further transient failure up to `retry_backoff_max` (10 min), with ±20 % jitter. With the defaults an unreachable model server returns its executions to `Created` after about an hour. Permanent failures (workflow error, timeout, missing inputs, unusable package) are not retried.

An error in one pass (e.g. the REST API temporarily unreachable) is logged and the loop continues with the next one.

## Running a workflow

A job thread creates a temporary directory under `work_dir`, stages the workflow package (a `.py` file or a `.zip` archive with the module at its top level; packages are cached, see `cache_max_bytes`) and the execution script into it, and runs `workflow_execution_script.py -eid <execution ID>` there with the scheduler's Python interpreter, in a process group of its own. The script imports the workflow class, maps the inputs, solves the workflow and maps the outputs. Its exit code tells the scheduler how it ended:

| Exit code | Meaning | Final status |
|---|---|---|
| 0 | workflow finished | `Finished` |
| 2 | no compatible model server had a free slot (other clients took them meanwhile) | `Pending` (retried after a delay) |
| 143 | stopped by the scheduler (timeout, shutdown); the workflow and its models were terminated | `Failed`, or `Pending` at shutdown |
| 1, other, or killed by a signal | workflow failed | `Failed` |

The workflow output is stored with the execution as its log (`ExecutionLog`).

**Timeout.** An execution may run for the `Timeout` declared in its workflow metadata (seconds), or for `default_job_timeout` if the workflow declares none (default: no limit). When exceeded, the workflow gets SIGTERM, which makes the execution script terminate the workflow and its models, then SIGKILL after `kill_grace` seconds; the execution fails. Workflows registered before the scheduler refactoring have no stored timeout until they are uploaded again.

**Model-server jobs left behind.** If a workflow process dies without terminating its models (killed, crashed), the scheduler terminates the model-server jobs belonging to the execution after its run, so they do not keep their slots. It looks for them on the model servers the execution used (`ModelServers` of the execution, stored once the models are allocated at initialization), and on all servers compatible with the other models: the templates (`Instantiate: false`), whose copies the workflow allocates later, or all models if the script died before storing them.

## Model servers, instances and execution profiles

A remote model of a workflow runs on a model server (jobmanager). Several model servers may be able to run it:

- **instances** of one model server, e.g. on several hosts, to share the load. All register with the same base name; the first one keeps it in the nameserver (`CVUT.demo01`), further ones get an instance name: `CVUT.demo01~<instance>` if started with an instance id (`instance=` of the model server, or `MUPIF_MODELSERVER_INSTANCE`), else `CVUT.demo01~<host>-<pid>`. Clients which look a server up by its exact name (older mupif) still reach the first instance;
- **model servers of the same model** registered under other names: a workflow model giving `ModelID` (the ID of the model's metadata) besides `Jobmanager` accepts every model server of that model.

Which of them may run a model, and which are preferred, is chosen per execution by its **execution profile** (`ExecutionProfileIndex` of the execution, set when it is created; `ExecutionProfiles` of the workflow metadata). A profile lists, per model, nameserver tags a model server must carry (`RequiredModelMetadata`, e.g. `Runtime_seconds`, `Accuracy_High`) and tags preferring it (`OptionalModelMetadata`). Without a profile (the **default profile**, `-1`), every compatible server may run the model.

The **workflow chooses** the server when it allocates a model (mupif `mupif.modelplacement`): servers with a free slot first, then those with more preferred tags, then the least loaded (used share of their job slots), at random among equal ones; if the chosen server refuses (full, unreachable), it tries the next one. The scheduler only checks that the models can find free slots before it starts the workflow (above), using the same ranking for its own count; it does not tell the workflow where to run, so the choice is also right for workflows run without the scheduler.

**Model servers which crash** (killed, host down) leave their nameserver entry behind. A model server of the same base name removes such entries when it starts, and the scheduler removes an entry whose server stayed unreachable for `stale_server_timeout` (300 s; it asks only while executions wait for those servers). Until then an unreachable server is skipped, and reported once. A model server checks its own entry every 30 s and registers again if it is missing (also after a nameserver restart), so a live server removed by mistake comes back by itself. Running model jobs of a stopped or crashed model server keep running until their workflow ends.

Where the models ran is shown in

- the **execution log**: for each model, the profile, the compatible servers with their load, the server chosen and every server skipped with the reason; then `Model servers used: …`;
- the **execution record**: `ModelServers` (model → server), stored by the execution script once the models are allocated;
- the **scheduler log** (records tagged with the execution ID): the profile and number of compatible servers when the execution is taken, the servers once known, and at the end of the run;
- the **statistics**: the model servers of each of the last executions, and the number of running executions per model server (below).

Requires mupif with `mupif.modelplacement` (branch `feature/execution-profiles`) for the scheduler, the execution script and the model servers that should register as instances; model servers of older versions keep working (one instance per name).

**Debugging failures.** With `keep_failed_workdirs` set, the working directories of the most recent failed executions are kept in `<work_dir>/failed/<execution ID>`.

## Stopping, restarting and maintenance

- **SIGTERM / SIGINT** (e.g. `supervisorctl stop`): the scheduler takes no new executions, waits up to `drain_timeout` (default 0 s) for running workflows, then stops them (SIGTERM, SIGKILL after `kill_grace` = 5 s). Their executions return to `Pending` with their attempt count and run again after the restart. The shutdown takes `drain_timeout + kill_grace` plus a few seconds; keep this below the stop timeout of the service manager (supervisor `stopwaitsecs`, default 10 s), or increase `stopwaitsecs` together with `drain_timeout`.
- **SIGKILL** or a crash: the workflow processes end with the scheduler (they ask the kernel for a signal when it dies). The restarted scheduler (same `scheduler_id`) takes its executions back at once, terminates their model-server jobs and runs them again; if it does not come back, another scheduler takes them over once their leases expired (`lease_seconds`).
- A clean stop releases the scheduler's identity, so it can be restarted at once (also on another host with the same `scheduler_id`).
- **Drain mode**, for maintenance without interrupting workflows: `kill -USR1 <scheduler PID>` (toggle), or `setDrain(True)` on the scheduler's Pyro object from the scheduler's host. While draining no new executions are taken; running ones finish. `getStatistics()` reports `draining`. Stop the scheduler once `runningTasks` is 0.

## Several schedulers

Several schedulers may run at once against the same REST API, database and nameserver, normally one per host. They share the queue of pending executions; each execution is run by exactly one of them:

- **Leases.** A scheduler takes an execution by *claiming* it: one atomic database update which sets it `Scheduled` and records the scheduler (`SchedulerID`), a lease number (`LeaseEpoch`, increased by every claim and takeover) and the lease's expiry (`LeaseExpires`, database time). Of several schedulers claiming the same execution, one succeeds. The scheduler renews the leases of its executions every `renew_interval` (60 s), together with its heartbeat, in a thread of its own.
- **Fencing.** Every write made for a leased execution names the lease (request header `X-MupifDB-Lease: <execution>:<scheduler>:<epoch>`): the scheduler's status changes and log, and the workflow's parameters, outputs and EDM writes (the execution script gets its lease in `MUPIFDB_LEASE`). The REST API refuses writes naming a lease which is not current (409). A scheduler, or a workflow, whose execution was taken over cannot overwrite the work of the new holder; when it notices (its renewal no longer lists the execution), it stops the workflow without writing anything.
- **Takeover.** When a scheduler dies, its leases are no longer renewed. Once a lease has expired *and* the scheduler's heartbeat has expired, another scheduler takes the execution over (new epoch), terminates the model-server jobs its workflow left behind and requeues it. A scheduler takes over only after it has itself reached the database without interruption for a whole lease period: after an outage of the REST API or the database every lease has expired, but the owners renew within one `renew_interval` once it is back, so an outage never makes the deployment run its executions again. Schedulers never stop their workflows because they lost contact: workflows keep running through an outage (the execution script retries its requests for up to `lease_seconds`), and results are stored if the API is back before they finish.
- **Identity.** Each scheduler has a `scheduler_id` (default: the host name), registered in the database (collection `Schedulers`) with its heartbeat and statistics. A scheduler refuses to start if another live process holds its ID. A scheduler restarted with the same ID takes its interrupted executions back at once.
- **Retry delays** (`NotBefore`, growing with consecutive delays) are stored with the execution, so all schedulers respect them and restarts keep them. When the resources of an execution are unavailable, one attempt is counted per delay, whichever schedulers see it.
- **Admission.** Each scheduler counts, before taking an execution, the model-server slots of the executions all schedulers started but whose models are not allocated yet; several schedulers admitting at the same moment may still overshoot a model server, which ends in a requeue that costs no attempt.
- **EDM outputs.** Each attempt of an execution creates its own EDM output objects (stamped with the execution and the lease epoch); objects of an earlier attempt are marked orphaned (`meta.orphaned`), not deleted.

**Settings:** `lease_seconds` (600): how long a lease and a heartbeat stay valid without renewal: the delay before another scheduler takes over the executions of a scheduler which died; `renew_interval` (60, at most half the lease), `takeover_interval` (60), `takeover_limit` (50).

**REST API:** the lease operations are allowed to admin accounts and to the accounts listed in `MUPIFDB_SCHEDULER_ACCOUNTS` (comma-separated e-mails; the accounts of the schedulers' `API_CREDENTIALS_FILE`).

**Upgrading** from a version without leases: stop the old scheduler, deploy the REST API, the web interface and the schedulers' code together, then start the schedulers. Old and new schedulers must not run together; the new REST API refuses the old scheduler's way of taking executions (`Status=Scheduled`), so an old scheduler takes nothing. Executions left `Scheduled`/`Running` by the old scheduler are taken over one lease period after the first new scheduler started.

**Two schedulers on one host** need different `scheduler_id`, `pidfile`, `stat_file`, `work_dir` (and `cache_dir`), see the Makefile target `scheduler2`.

**Remaining risks:** during a network partition which cuts one scheduler off the database (but not its workflows' model servers), its executions may compute twice for a while (only the new holder's results are stored); EDM output written before a takeover into objects given by the user (not created by the run) may stay mixed if the new attempt fails; an execution which makes its scheduler crash is limited only by `max_attempts`.

## Statistics

Each scheduler registers its monitor in the Pyro nameserver as `mupif.scheduler.<scheduler_id>` (metadata `type:scheduler`, by which the monitoring pages find all schedulers); `getStatistics()` returns the counters shown by the monitoring pages (and `schedulerId`). The REST API (`/status`, `/scheduler_statistics`) adds up the counters of the live schedulers, sent with their heartbeat:

- `scheduledTasks` – executions taken and not started yet; `runningTasks` – running workflow processes;
- `processedTasks` = `finishedTasks` + `failedTasks` – executions that reached a final status (an execution returned to `Pending` is not counted);
- `currentLoad` – percentage of `max_jobs` in use; `capacity` – `max_jobs`;
- `pendingTasks` – executions in the database queue (`Pending`), sampled every iteration; the queue is shared by the deployment;
- `waitingTasks` = `pendingTasks` + `scheduledTasks` – executions waiting to run;
- `dbRunningTasks` – executions `Running` according to the database (`null` until the first sample). It is normally equal to `runningTasks`; it is higher when some `Running` executions are not processes of this scheduler, e.g. a workflow that outlived a previous scheduler run (left alone at startup) or a record left behind;
- hourly history of the last 48 hours (`pooledTasks48`: executions taken, `processedTasks48`, `finishedTasks48`, `failedTasks48`: events in each hour; `load48`, `running48`, `waiting48`: peak level in each hour);
- history of the last 2 hours by minutes, peak levels: `running120m`, `waiting120m`;
- time axis of the histories: `periodStart48`/`periodStart120m` – start of the last (current) period in seconds since the epoch, `periodLength48`/`periodLength120m` – length of a period in seconds; element *i* of a list of *n* starts at `periodStart - (n-1-i)*periodLength`;
- `draining` – drain mode;
- `modelServerJobs` – running executions of this scheduler per model server (registered name), counted once the execution has stored its model servers;
- `lastJobs` – the last executions started: ID, workflow, status, start, end, and the model servers of its models (empty until known; older readers use the first five fields).

Peak levels: a period starts with the level of the time (a quiet hour with long workflows still shows them running) and keeps the highest level reached in it. If the clock goes back (NTP step, VM resync), the history stays at its last period until real time catches up; the counters are not affected.

The totals and the history are kept across restarts in the statistics file and copied to the database (`GET /scheduler_statistics`). Over Pyro, `stop()` and `setDrain()` are accepted only from the scheduler's host.

Log records of the scheduler carry the ID of the execution they concern (`weid` attribute; job threads are named `job:<execution ID>`).

## Configuration

All settings can be set by environment variables; `python -m mupifDB.scheduler --print-config` prints them with their current values.

| Variable | Default | Meaning |
|---|---|---|
| `MUPIF_NS` | – | Pyro nameserver (`host:port`) |
| `MUPIFDB_REST_SERVER` | `http://127.0.0.1:8005` | REST API |
| `API_CREDENTIALS_FILE` | – | JSON file with `username`/`password` of the account the scheduler uses for the REST API; the client logs in again automatically when the API rejects its token |
| `MUPIFDB_SCHEDULER_SCHEDULER_ID` | host name | Identity of this scheduler; each running scheduler needs its own. |
| `MUPIFDB_SCHEDULER_LEASE_SECONDS` | 600 | Validity of leases and of the heartbeat without renewal: delay before the executions of a dead scheduler are taken over. |
| `MUPIFDB_SCHEDULER_RENEW_INTERVAL` | 60 | Seconds between two renewals of the heartbeat and the leases (at most half of `lease_seconds`). |
| `MUPIFDB_SCHEDULER_TAKEOVER_INTERVAL` | 60 | Seconds between two takeover passes. |
| `MUPIFDB_SCHEDULER_TAKEOVER_LIMIT` | 50 | Maximum number of executions taken over in one pass. |
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
| `MUPIFDB_SCHEDULER_STALE_SERVER_TIMEOUT` | 300 | Seconds a model server registered in the nameserver may stay unreachable before the scheduler removes its entry; 0: never. |
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

- See the remaining risks of [several schedulers](#several-schedulers).
