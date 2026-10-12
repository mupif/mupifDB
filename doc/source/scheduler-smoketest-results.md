# Scheduler smoke-test results

Results of `tools/scheduler_smoketest.py` against a local deployment, recorded during the scheduler refactoring. Setup: `make run` stack (nameserver, MongoDB 8.0 replica set, REST API, scheduler with `LOOP_SLEEP_SEC=5`) and the model server of mupif example 13 (`CVUT.demo01`, `maxJobs=4`), demo workflow `Workflow01` (`Value_1*Value_2` with a `Delay` input). "Before" is the scheduler of mupifDB master (`1d70b52`), "Phase 1" the end of phase 1 of the refactoring.

| Scenario | Command / action | Before | Phase 1 |
|---|---|---|---|
| **A** baseline | `--count 3 --delay 2 --check-order` | all Finished, results correct; **started newest first** (2,1,0), 2 s apart; 13.3 s | all Finished, oldest first, started together; 9.3 s |
| **B** overload | `--count 12 --delay 15 --max-concurrent 4 --check-order` | all Finished, never more than 4 running; 69.7 s | all Finished with **1 attempt** each, batches of 4 started together, never more than 4 running, oldest first; 65.1 s |
| **C** model server down | model server stopped for 25 s, `--count 2 --delay 2` | attempts counted, Finished after restart | 5 attempts counted while the server was down, Finished after its restart |
| **D** workflow killed | `--count 1 --delay 30`, workflow subprocess killed with SIGKILL | scheduler logged return code -9 and left the execution **Running forever** (no end date) | **Failed** after 1 attempt, log stored |
| **E2** scheduler killed | `--count 3 --delay 40`, scheduler main process SIGKILLed while they run, restarted at once | surviving pool workers finished the executions; statistics lost | restarted scheduler "left alone" all 3 (their processes were alive), started none twice; all Finished |
| No resources at start | last free slot taken right after the workflow started (test helper) | execution **Failed** (exit-2 path dead) | Running → Pending → Running → Finished |
| REST API key change | REST API restarted with a new JWT key under a running scheduler | **scheduler stopped** (401 propagated out of the main loop) | client logged in again, scheduler kept running, next executions Finished |
| Two instances of `CVUT.demo01` | second instance of the same model server started; `--count 12 --delay 15 --max-concurrent 8` | – | not supported yet (phase 4): the second instance replaces the first in the nameserver, at most 4 run; stopping it makes the other unreachable |

Remaining issues seen in these runs (addressed in later phases):

- the execution script sets `Finished` before the scheduler stores the log, so for a moment an execution is `Finished` without a log (seen for 1–4 executions per run);
- a killed workflow leaves its model-server job allocated (scenario D: `3@CVUT.demo01` had to be terminated by hand);
- on SIGTERM the scheduler can hang in shutdown, and forked pool workers can outlive it.
