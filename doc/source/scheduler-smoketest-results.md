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

## Phase 4: several model servers, execution profiles

Same local stack (`/usr/bin/mongod` 8.0), branches `feature/execution-profiles` (mupif) and `feature/scheduler-model-placement` (mupifDB); model servers of mupif example 13 started with the `server.py` of the example (two instances of `CVUT.demo01`, 4 slots each) and the same model under another name, `CVUT.demo01-alt` (2 slots). `Workflow01p` is `Workflow01` with `ModelID: demo_multiplicator`, `Timeout` 60 s and three execution profiles: 0 prefers `CVUT.demo01-alt` (optional tag), 1 requires `CVUT.demo01` (its instances only), 2 requires `Runtime_days` (no server has it).

| Scenario | Command / action | Phase 4 |
|---|---|---|
| **A** baseline, one model server | `--register --count 3 --delay 2` | all Finished; `ModelServers` of each record `m1 → CVUT.demo01`; execution log shows the candidates with their load and the server chosen; scheduler log tags the profile and the servers |
| second instance | `server.py` started again | registered as `CVUT.demo01~<host>-<pid>`; the first one stays reachable as `CVUT.demo01` |
| **B** two instances | `--count 12 --delay 15 --max-concurrent 8 --check-order` | first run: 2 executions requeued (exit 2) and started out of order: the scheduler took more executions while the first 8 were still allocating their models (fixed: in-flight executions are counted). After the fix: all Finished with 1 attempt, in order, 8 at once, 6 on each instance; workflows finding the chosen instance full went to the other one ("did not accept the model (no free slot); trying the next one") |
| **C** instance crashed | `--count 12 --delay 20 --expect any`, second instance killed (SIGKILL) while 8 ran | executions taken afterwards all went to the surviving instance ("1 compatible model server"); the stale nameserver entry is skipped as unreachable every pass (a warning each time); model jobs of the killed server kept running and 3 of its 4 executions finished. 3 executions failed on the workflow `Timeout` of 30 s (20 s runs under load; not related to the crash) |
| **P0** profile 0 | `--wid Workflow01p --profile 0 --count 4 --delay 5` | `CVUT.demo01-alt` ranked first (1 preferred tag): 2 on it, the other 2 fell back to `CVUT.demo01` |
| **P1** profile 1 | `--profile 1` | only the `CVUT.demo01` instances are candidates; `-alt` never used |
| **P** default profile | `--profile -1` | all three servers candidates (the `-alt` one through `ModelID`) |
| **P2** unmet profile | profile 2 | "no compatible model server registered and reachable": attempts counted with the retry delay, as for an unreachable server |
| invalid profile | `executions/create` with index 5; a record edited to index 7 | creation refused (HTTP 422); the edited execution Failed at once with the log "Execution profile #7 requested, but the workflow has 3 execution profile(s)" |
| **E1** scheduler stopped | `--count 6 --delay 15`, SIGTERM to the scheduler while they run, restart | all Finished (attempt 2); `ModelServers` of each record from the new attempt (cleared when the execution became Running); each execution listed once in the statistics (fixed: an execution started again had two entries) |
| monitor | `--wid Workflow01p --count 10 --delay 25`, mupif-monitor built against the local API | all 10 slots in use; Scheduler page: model servers of the last executions; Model servers page: "Instance of" and "Scheduler executions" (4, 2, 4) matching the servers' running jobs; no console errors |

Observed limits (not fixed): workflows starting in the same second read the same loads and tend to pick the same server (all 4 of P1 on one instance); the fallback keeps it correct, but the spread within a burst is uneven. A crashed instance registered under an instance name stays in the nameserver until removed by hand.

## Other checks

| Check | Before | Phase 1 | Phase 2 |
|---|---|---|---|
| No resources at start (last free slot taken right after the workflow started) | execution **Failed** (exit-2 path dead) | Running → Pending → Running → Finished | as phase 1, retried after a delay |
| REST API restarted with a new JWT key under a running scheduler | **scheduler stopped** (401 out of the main loop) | client logged in again, scheduler kept running | as phase 1 |
| Execution observed `Finished` before its log was stored | 1–4 per run | 1–4 per run | none (scheduler stores the log, then sets the status) |
| Two instances of `CVUT.demo01`, `--count 12 --delay 15 --max-concurrent 8` | – | not supported: the second instance replaces the first in the nameserver, max. 4 run; stopping it makes the other unreachable | unchanged; supported in phase 4 (above) |

Remaining issues (addressed in later phases): one scheduler per deployment only; retry delays are lost on restart.

Phase 4 implements the sharing of load among instances of a model server; see its results above.
