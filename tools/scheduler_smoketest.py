#!/usr/bin/env python3
r"""
End-to-end smoke test of the MupifDB workflow scheduler against a real local deployment.

The script talks to the stack only through the public REST client (:mod:`mupifDB.restApiControl`),
so it exercises exactly the path a user takes: create executions, set their inputs, schedule
them, and wait until the scheduler has driven every execution to a final state. It then prints a
summary table and verifies a set of invariants; the exit status is non-zero if any check fails,
so the script can be used from ``make smoketest`` or CI.

Required deployment (see the ``modelserver13`` and ``smoketest`` targets in the Makefile):

* Pyro nameserver, MongoDB, the REST API and the scheduler (``make run``);
* the model server from mupif example 13 (``mupif/examples/13/server.py``), registered as
  ``CVUT.demo01`` with ``maxJobs=4``. Its model multiplies ``Value_1*Value_2`` and sleeps for the
  optional ``Delay`` input, which makes it easy to load the scheduler in a controlled way;
* the workflow ``Workflow01`` (``mupifDB/api/demo/workflowdemo01.py``) in the database;
  ``--register`` uploads it when missing.

Environment variables (read by the REST client at import time):

* ``MUPIFDB_REST_SERVER`` -- REST API base URL, e.g. ``http://127.0.0.1:11003``;
* ``API_CREDENTIALS_FILE`` -- JSON file with ``username``/``password`` of an account that may
  create executions (and upload workflows, with ``--register``).

Scenarios used during the scheduler refactoring (letters match the refactoring plan):

* **A** baseline: ``--count 3 --delay 2``, everything must finish with output ``Value_1*Value_2``;
* **B** overload: ``--count 12 --delay 15 --max-concurrent 4``, more executions than model
  server slots; all must finish and never more than ``maxJobs`` may run at once;
* **C** model server down: stop the model server, run with ``--expect any``, restart it later;
* **D** workflow crash: ``--delay 30`` and kill the execution subprocess from another shell;
  the execution must end as ``Failed`` and not stay ``Running``;
* **E** scheduler restart: ``--delay 30``, then stop/kill the scheduler mid-run and restart it;
* **K** missing input: ``--skip-input Value_2``; the execution must fail.

Example::

    MUPIFDB_REST_SERVER=http://127.0.0.1:11003 API_CREDENTIALS_FILE=creds.json \
        python3 tools/scheduler_smoketest.py --register --count 3 --delay 2
"""

import argparse
import datetime
import json
import os
import sys
import time
from dataclasses import dataclass, field, asdict
from typing import Optional

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')
from mupifDB import restApiControl  # noqa: E402

#: Statuses after which the scheduler will not touch an execution again on its own.
#: ``Created`` is final only once the execution has been active: a fresh execution starts as
#: ``Created`` too, and returns there when the scheduler gives up after too many attempts.
FINAL_STATUSES = ('Finished', 'Failed', 'Created')

#: Statuses showing that the scheduler has seen the execution.
ACTIVE_STATUSES = ('Pending', 'Scheduled', 'Running')

#: Name of the output of ``Workflow01`` holding ``Value_1*Value_2``.
RESULT_OUTPUT = 'Multiplication_result'


@dataclass
class ExecutionReport:
    """
    State of one execution as observed by the smoke test.

    :param weid: execution ID
    :param submitted: order in which the execution was scheduled (0 = first)
    """

    weid: str
    submitted: int
    status: str = 'Created'
    attempts: int = 0
    start: Optional[datetime.datetime] = None
    end: Optional[datetime.datetime] = None
    result: Optional[float] = None
    has_log: bool = False
    #: set once the execution was observed in one of :data:`ACTIVE_STATUSES`
    activated: bool = False
    #: set if the execution was observed as ``Finished``/``Failed`` before its log was stored;
    #: users polling the record (e.g. the web UI) see the same inconsistent state
    final_before_log: bool = False
    extra: dict = field(default_factory=dict)

    def isFinal(self) -> bool:
        """
        Tell whether the scheduler is done with this execution.

        :return: true for ``Finished``/``Failed``, and for ``Created`` after the execution was active
        """
        return self.status in FINAL_STATUSES and (self.status != 'Created' or self.activated or self.attempts > 0)


def propertyInput(value: float, data_id: str = 'PID_Time', unit: str = 's') -> dict:
    """
    Build the serialized ``ConstantProperty`` used to set a scalar execution input.

    The format is the one the web UI sends (``webapi/index.py``), so the scheduler and the
    execution script see exactly what a real user would produce.

    :param value: scalar value
    :param data_id: name of the :class:`mupif.DataID` member, without the ``mupif.DataID.`` prefix
    :param unit: unit string understood by mupif
    :return: dictionary accepted by :func:`restApiControl.setExecutionInputObject`
    """
    return {'ClassName': 'ConstantProperty', 'ValueType': 'Scalar', 'DataID': data_id, 'Unit': unit, 'Value': value, 'Time': None}


def ensureWorkflow(wid: str, register: bool) -> int:
    """
    Make sure workflow *wid* exists in the database and return its newest version.

    :param wid: workflow ID (``Workflow01`` for the demo)
    :param register: upload ``mupifDB/api/demo/workflowdemo01.py`` into use case ``Demo`` when missing
    :return: workflow version to use for new executions
    :raises SystemExit: if the workflow is missing and *register* is false
    """
    try:
        return restApiControl.getWorkflowRecord(wid, -1).Version
    except Exception:
        if not register:
            sys.exit(f'Workflow {wid} not found in the database; re-run with --register.')
    demo = os.path.dirname(os.path.abspath(__file__)) + '/../mupifDB/api/demo/workflowdemo01.py'
    restApiControl.postWorkflowFiles('Demo', demo, [])
    return restApiControl.getWorkflowRecord(wid, -1).Version


def submit(args: argparse.Namespace, version: int) -> list[ExecutionReport]:
    """
    Create, configure and schedule ``args.count`` executions of the demo workflow.

    Executions are created one by one so that their submission order is well defined; the
    scheduler is expected to start them in this order (oldest first).

    :param args: parsed command line
    :param version: workflow version
    :return: one report per execution, in submission order
    """
    inputs = {('Value_1', '1'): args.value1, ('Value_2', '2'): args.value2, ('Delay', 'delay'): args.delay}
    reports = []
    for i in range(args.count):
        weid = restApiControl.createExecution(args.wid, version=version, ip='127.0.0.1')['inserted_id']
        for (name, objid), value in inputs.items():
            if name in args.skip_input:
                continue
            restApiControl.setExecutionInputObject(weid, name, objid, propertyInput(value))
        restApiControl.scheduleExecution(weid)
        reports.append(ExecutionReport(weid=weid, submitted=i))
        print(f'[{i}] scheduled {weid}', flush=True)
    return reports


def refresh(rep: ExecutionReport) -> None:
    """
    Update *rep* in place from the current execution record (and its outputs once finished).

    :param rep: report to update
    """
    rec = restApiControl.getExecutionRecord(rep.weid)
    rep.status, rep.attempts, rep.start, rep.end = rec.Status, rec.Attempts, rec.StartDate, rec.EndDate
    rep.has_log = bool(rec.ExecutionLog)
    rep.activated |= rep.status in ACTIVE_STATUSES
    rep.final_before_log |= rep.status in ('Finished', 'Failed') and not rep.has_log
    if rep.status == 'Finished' and rep.result is None:
        for item in restApiControl.getExecutionOutputRecord(rep.weid):
            if item.Name == RESULT_OUTPUT and item.Object:
                rep.result = item.Object.get('Value')


def wait(reports: list[ExecutionReport], timeout: float, poll: float, settle: float) -> bool:
    """
    Poll until every execution is final (see :meth:`ExecutionReport.isFinal`) or *timeout* expires.

    A status line is printed whenever the distribution of statuses changes.

    :param reports: executions to watch
    :param timeout: maximum waiting time in seconds
    :param poll: polling interval in seconds
    :param settle: delay before the final re-read of all records, in seconds
    :return: true if all executions are final, false on timeout
    """
    t0, last = time.time(), None
    while True:
        for rep in reports:
            if not rep.isFinal():
                refresh(rep)
        counts = {}
        for rep in reports:
            counts[rep.status] = counts.get(rep.status, 0) + 1
        if counts != last:
            print(f'{time.time()-t0:6.1f}s  ' + '  '.join(f'{k}={v}' for k, v in sorted(counts.items())), flush=True)
            last = counts
        if all(rep.isFinal() for rep in reports):
            # finishing steps (e.g. storing the log) may still be running for the last
            # executions; let them settle and take the final state from a fresh read
            time.sleep(settle)
            for rep in reports:
                refresh(rep)
            return True
        if time.time() - t0 > timeout:
            return False
        time.sleep(poll)


def maxConcurrency(reports: list[ExecutionReport]) -> int:
    """
    Compute the largest number of executions that were running at the same instant.

    Uses the ``StartDate``/``EndDate`` interval of each execution (set by the REST API when the
    status changes to ``Running`` and to ``Finished``/``Failed``). Executions without both dates
    are ignored.

    :param reports: finished executions
    :return: maximum overlap of the running intervals
    """
    events = []
    for rep in reports:
        if rep.start and rep.end:
            events += [(rep.start, 1), (rep.end, -1)]
    # at equal timestamps process ends before starts, so back-to-back jobs do not count as overlapping
    events.sort(key=lambda e: (e[0], e[1]))
    cur = best = 0
    for _, delta in events:
        cur += delta
        best = max(best, cur)
    return best


def check(args: argparse.Namespace, reports: list[ExecutionReport], completed: bool) -> list[str]:
    """
    Verify the invariants requested on the command line.

    :param args: parsed command line
    :param reports: final state of all executions
    :param completed: whether :func:`wait` finished before its timeout
    :return: list of human-readable failures (empty when everything passed)
    """
    failures = []
    if not completed:
        failures.append(f'timeout: not all executions reached a final state within {args.timeout}s')
    if args.expect == 'finished':
        for rep in reports:
            if rep.status != 'Finished':
                failures.append(f'{rep.weid}: status {rep.status}, expected Finished')
            elif rep.result is None or abs(rep.result - args.value1 * args.value2) > 1e-9:
                failures.append(f'{rep.weid}: result {rep.result}, expected {args.value1*args.value2}')
    elif args.expect == 'failed':
        failures += [f'{rep.weid}: status {rep.status}, expected Failed' for rep in reports if rep.status != 'Failed']
    for rep in reports:
        if rep.status in ('Finished', 'Failed') and not rep.has_log:
            failures.append(f'{rep.weid}: no ExecutionLog stored')
    if args.max_concurrent is not None and (mc := maxConcurrency(reports)) > args.max_concurrent:
        failures.append(f'{mc} executions ran concurrently, at most {args.max_concurrent} allowed')
    if args.check_order:
        started = sorted((r for r in reports if r.start), key=lambda r: r.start)
        if [r.submitted for r in started] != sorted(r.submitted for r in started):
            failures.append('executions did not start in submission order: ' + ','.join(str(r.submitted) for r in started))
    return failures


def printTable(reports: list[ExecutionReport]) -> None:
    """
    Print one line per execution: order, ID, status, attempts, start/end time, duration and result.

    :param reports: executions to print
    """
    fmt = '{:>3} {:<24} {:<9} {:>3} {:>8} {:>8} {:>7} {:>8} {:>3}'
    print(fmt.format('#', 'weid', 'status', 'att', 'start', 'end', 'dur[s]', 'result', 'log'))
    for r in reports:
        dur = f'{(r.end - r.start).total_seconds():.1f}' if r.start and r.end else ''
        print(fmt.format(r.submitted, r.weid, r.status, r.attempts, r.start.strftime('%H:%M:%S') if r.start else '',
                         r.end.strftime('%H:%M:%S') if r.end else '', dur, '' if r.result is None else f'{r.result:g}', 'yes' if r.has_log else 'no'))
    print(f'max. concurrently running: {maxConcurrency(reports)}')
    if seen := sum(r.final_before_log for r in reports):
        print(f'note: {seen} execution(s) were observed as final before their log was stored')


def main() -> int:
    """
    Command-line entry point.

    :return: process exit status (0 if all checks passed)
    """
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[1], formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--wid', default='Workflow01', help='workflow ID to execute (default: %(default)s)')
    ap.add_argument('--register', action='store_true', help='upload the demo workflow if it is not in the database')
    ap.add_argument('--count', type=int, default=3, help='number of executions to submit (default: %(default)s)')
    ap.add_argument('--delay', type=float, default=2., help='Delay input in seconds, i.e. model run time (default: %(default)s)')
    ap.add_argument('--value1', type=float, default=3., help='Value_1 input (default: %(default)s)')
    ap.add_argument('--value2', type=float, default=2., help='Value_2 input (default: %(default)s)')
    ap.add_argument('--skip-input', action='append', default=[], metavar='NAME', help='leave this input unset (repeatable)')
    ap.add_argument('--expect', choices=['finished', 'failed', 'any'], default='finished',
                    help='required final status of every execution (default: %(default)s)')
    ap.add_argument('--max-concurrent', type=int, default=None, metavar='N', help='fail if more than N executions ran at once')
    ap.add_argument('--check-order', action='store_true', help='fail if executions did not start in submission order')
    ap.add_argument('--timeout', type=float, default=300., help='seconds to wait for all executions (default: %(default)s)')
    ap.add_argument('--poll', type=float, default=1., help='polling interval in seconds (default: %(default)s)')
    ap.add_argument('--settle', type=float, default=3., help='wait before re-reading final states, in seconds (default: %(default)s)')
    ap.add_argument('--json', metavar='FILE', help='also write the per-execution reports to FILE as JSON')
    args = ap.parse_args()

    version = ensureWorkflow(args.wid, args.register)
    reports = submit(args, version)
    completed = wait(reports, args.timeout, args.poll, args.settle)
    printTable(reports)
    if args.json:
        with open(args.json, 'w') as f:
            json.dump([asdict(r) for r in reports], f, indent=2, default=str)
    failures = check(args, reports, completed)
    for msg in failures:
        print('FAIL: ' + msg)
    print('RESULT: ' + ('FAILED' if failures else 'OK'))
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())
