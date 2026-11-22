# Changelog

## Unreleased: workflow scheduler refactoring

### Phase 2: restructured scheduler

The scheduler is now the package `mupifDB/scheduler/` (configuration, core, database backend, runner, staging, resources, statistics); `mupifDB/workflowscheduler.py` remains the entry point. Behaviour changes for users and operators:

- **One process, job threads.** The scheduler is a single process (31 before): executions run in threads, each starting its workflow as a subprocess in its own process group. It takes at most `max_jobs` executions at a time, instead of queueing up to 300 behind its workers.
- **Configuration by environment variables** `MUPIFDB_SCHEDULER_*` (`python -m mupifDB.scheduler --print-config` lists them). Defaults keep the former behaviour.
- **Orderly shutdown.** SIGTERM stops the running workflows (they terminate their models) and returns their executions to `Pending`; they run again after the restart. The default shutdown fits supervisor's 10 s stop timeout (`drain_timeout`, `kill_grace`). If the scheduler is killed, its workflows end too and are run again after the restart.
- **Drain mode** for maintenance (`kill -USR1`, or `setDrain()` over Pyro from the scheduler's host): no new executions, running ones finish.
- **Workflow timeout.** The `Timeout` of the workflow metadata is stored with the workflow (new field of the workflow record) and enforced; `default_job_timeout` applies to workflows without one.
- **Missing compulsory inputs** fail an execution before it starts, with a log saying why.
- **Retries with a growing delay** after transient failures (10 s doubling up to 10 min): an unreachable model server no longer exhausts the attempts of its executions within minutes.
- **Leftover model-server jobs** of a workflow that died (killed, timed out) are terminated by the scheduler, so model-server slots are not lost.
- The **execution log is stored before the final status** is set: the execution script no longer sets statuses itself under the scheduler (`--standalone` keeps that for manual runs).
- **Workflow packages are cached** (`cache_max_bytes`, 1 GiB); a package is downloaded once however many executions use it. Packages are checked when staged (module present, no unsafe archive paths).
- Optional: working directories of failed executions kept for debugging (`keep_failed_workdirs`); log records tagged with the execution ID; owner notifications sent in the background (SMTP connection with a timeout).
- Pyro: only `getStatistics()`, `stop()` and `setDrain()` are exposed, and the last two only to callers on the scheduler's host (`stop()` was callable by anyone). `getStatistics()` additionally reports `draining`.
- Fixed: the REST client misread the file name of every download from `Content-Disposition`; the statistics file is written atomically.

Recommended together with the mupif branch `feature/scheduler-refactor` (commit "meta: merge metadata recursively"). Without it, workflows lose their `Execution` metadata (ID, log URI, ...) in `initialize()`, so model output does not reach the execution's live log. The scheduler itself works with either mupif version.

Development: unit tests moved to `mupifDB/scheduler/tests/` (106 tests, no services needed); API reference `doc/source/scheduler-api.rst` (sphinx autodoc enabled).

### Phase 1: scheduler bug fixes

Behaviour changes visible to users and operators:

- Executions are started **oldest submitted first** (they used to start newest first). `GET /executions` has a new optional `sort` parameter (`CreatedDate`, `SubmittedDate`, descending with a leading `-`; default `-CreatedDate` as before).
- An execution whose model server has **no free slot** when the workflow starts goes back to `Pending` and is retried. It used to be marked `Failed`: the model server's exception could not be transferred by Pyro, so the check never matched.
- A workflow process **killed by a signal** (OOM killer, manual kill) or crashing ends as `Failed`. It used to stay `Running` forever.
- Executions left `Scheduled` or `Running` by a **crashed or killed scheduler** are run again after it restarts, unless their workflow process is still running. `Scheduled` executions used to stay there forever when resources were busy at startup, and `Running` ones were never recovered.
- A worker error before the workflow process starts (e.g. REST API unreachable) returns the execution to `Pending` instead of leaving it `Scheduled`.
- The scheduler no longer sleeps 2 s per pending execution. It reads the free slots of each model server once per iteration and starts no more executions than they can serve. **Waiting for a busy model server no longer uses up attempts**; only unreachable model servers count towards the limit of 12 attempts (after which the execution returns to `Created`).
- One failed REST call no longer stops the scheduler: the main loop logs it and continues. The REST client logs in again when the API rejects its access token (e.g. after the API restarted with a new key) instead of failing until the token expires.
- Statistics: `scheduledTasks` counts executions queued in the scheduler (not the database backlog), the hourly "pooled" history no longer re-counts the backlog every iteration, requeued executions are not counted as failed, and the totals are kept across restarts.
- The workflow log is no longer repeated line by line in the scheduler's own log (it is stored with the execution as before).
- `SchedulerMonitor.stop()` stops the scheduler (it did nothing).
- Importing `mupifDB.workflowscheduler` has no side effects (it used to sleep 5 s and connect to the nameserver).

Development and testing:

- `tools/scheduler_smoketest.py` and the Makefile targets `modelserver13`, `smoketest`, `lint-docs`: end-to-end test against a local deployment with the model server of mupif example 13. `make run` works without `mongosh` (`tools/mongo_replset_init.py`), and `MONGOD`, `PYTHON` and `API_CREDENTIALS_FILE` can be overridden.
- Unit tests `mupifDB/test_scheduler_unit.py` and `mupifDB/test_client_auth.py`. The integration test `mupifDB/test_scheduler.py` runs again (authenticated fixtures, correct start pattern).
- REST client: `setAPICredentials()` sets the account after import.
- Documentation: new page `doc/source/scheduler.md`; smoke-test results before and after in `doc/source/scheduler-smoketest-results.md`.
