# Changelog

## Unreleased: workflow scheduler refactoring

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
