# Scheduler smoke-test results

Results of `tools/scheduler_smoketest.py` against a local deployment, recorded during the scheduler refactoring. Setup: `make run` stack (nameserver, MongoDB 8.0 replica set, REST API, scheduler with poll interval 5 s) and the model server of mupif example 13 (`CVUT.demo01`, `maxJobs=4`), demo workflow `Workflow01` (`Value_1*Value_2` with a `Delay` input; metadata `Timeout` 30 s). "Before" is the scheduler of mupifDB master (`1d70b52`); "Phase 1" and "Phase 2" are the ends of these phases of the refactoring.

## Core scenarios

| Scenario | Command / action | Before | Phase 1 | Phase 2 |
|---|---|---|---|---|
| **A** baseline | `--count 3 --delay 2 --check-order` | all Finished; **started newest first** (2,1,0), 2 s apart; 13.3 s | all Finished, oldest first, started together; 9.3 s | as phase 1 |
| **B** overload | `--count 12 --delay 15 --max-concurrent 4 --check-order` | all Finished, max. 4 running; 69.7 s | all Finished with 1 attempt each, batches of 4, oldest first; 65.1 s | as phase 1 (65 s); one scheduler process instead of 31 |
| **C** model server down | model server stopped for 25 s, `--count 2 --delay 2` | attempts counted, Finished after restart | 5 attempts used while down | **2 attempts** used (retry delays 8 s, then ~22 s); Finished after the restart |
| **D** workflow killed | `--count 1 --delay 30`, workflow subprocess killed with SIGKILL | scheduler logged -9 and left the execution **Running forever**; its model job leaked | Failed after 1 attempt, log stored; model job leaked | Failed, log stored; **leaked model job terminated** by the scheduler, all slots free |
| **E1** scheduler stopped | `--count 3 --delay 25`, SIGTERM to the scheduler while they run, restart | (killed immediately; pool workers left behind) | – | scheduler exited after 1.3 s, workflows stopped and their models terminated (all slots free), executions Pending; after the restart all Finished (attempt 2) |
| **E2** scheduler killed | as E1 with SIGKILL | surviving pool workers finished the executions; statistics lost | restarted scheduler "left alone" the surviving workflows, which finished | workflows ended with the scheduler; the restart returned the 3 Running executions to Pending, terminated a leftover model job and ran them again: all Finished |

## Phase 2 features

| Scenario | Command / action | Phase 2 |
|---|---|---|
| **J** timeout | `--count 1 --delay 60` (workflow Timeout 30 s) | stopped after 30.4 s ("run time exceeded 30.0 s"), Failed with log, all model slots free |
| **K** missing input | `--count 1 --skip-input Value_2` | Failed without being started (0 attempts); log "The execution was not started: some compulsory inputs are not set" |
| **L** drain | `--count 8 --delay 20`, `kill -USR1` while the first 4 run, again 40 s later | first 4 finished; nothing started while draining; the other 4 started at the first pass after drain was switched off |
| **M** package cache | `--count 10 --delay 2` | package downloaded once (first run after a restart of the cache), all Finished |

## Other checks

| Check | Before | Phase 1 | Phase 2 |
|---|---|---|---|
| No resources at start (last free slot taken right after the workflow started) | execution **Failed** (exit-2 path dead) | Running → Pending → Running → Finished | as phase 1, retried after a delay |
| REST API restarted with a new JWT key under a running scheduler | **scheduler stopped** (401 out of the main loop) | client logged in again, scheduler kept running | as phase 1 |
| Execution observed `Finished` before its log was stored | 1–4 per run | 1–4 per run | none (scheduler stores the log, then sets the status) |
| Two instances of `CVUT.demo01`, `--count 12 --delay 15 --max-concurrent 8` | – | not supported: the second instance replaces the first in the nameserver, max. 4 run; stopping it makes the other unreachable | unchanged (planned for phase 4) |

Remaining issues (addressed in later phases): one scheduler per deployment only; several instances of a model server do not share the load; retry delays are lost on restart.
