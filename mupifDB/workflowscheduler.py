import sys
import os
mupifDBModDir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(mupifDBSrcDir := (mupifDBModDir+'/..'))

import time
import atexit
import tempfile
import multiprocessing
import subprocess
import threading
import enum
import pidfile
import zipfile
import ctypes
import json
import jsonpickle
import textwrap
from typing import Tuple

from mupifDB import restApiControl, restLogger, my_email

from pathlib import Path
import shutil
import datetime

import Pyro5.api
import Pyro5.errors
import mupif as mp

import logging

log=logging.getLogger('scheduler') 
log.setLevel(logging.INFO)
log.addHandler(restLogger.RestLogHandler())

# logging.getLogger().setLevel(logging.DEBUG)

# decrease verbosity here
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("urllib3").propagate = False

LOOP_SLEEP_SEC=20
# delay before the scheduler starts working, giving the other services (nameserver, REST API) of a
# freshly started deployment time to come up; applied in main(), never at import time
STARTUP_DELAY_SEC=5

schedulerStatFile = "/var/lib/mupif/persistent/scheduler-stat.json"

# Pyro nameserver proxy and its URI (passed to workflow subprocesses as MUPIF_NS); set by main(),
# before the worker pool is forked, so that workers inherit them
ns = None
ns_uri = None

poolsize = 30
# set to end the main scheduler loop (see SchedulerMonitor.stop); an Event rather than a flag, so
# that the loop's sleep between iterations is interrupted as soon as a stop is requested
stopEvent = threading.Event()


import pydantic
from typing import Literal,List,Optional
import threading
import multiprocessing


from mupifDB.scheduler.stats import SchedulerStat
SchedulerStat.capacity=poolsize


import functools

def pyro_only(no_remote=False):
    def deco(fn):
        @functools.wraps(fn)
        def inner(*args, **kwargs):
            import Pyro5.callcontext
            if not Pyro5.callcontext.current_context.client: raise PermissionError('Must only be called via Pyro')
            if no_remote:
                import ipaddress
                import psutil
                import socket
                addr=ipaddress.ip_address(Pyro5.callcontext.current_context.client_sock_addr[0]) # type: ignore
                if addr.is_loopback: pass
                else:
                    ips=set([ipaddress.ip_address(rec.address) for rec in sum(psutil.net_if_addrs().values(),[]) if rec.family in (socket.AF_INET,socket,socket.AF_INET6)])
                    if addr not in ips: raise PermissionError(f'Must only be called locally (not from {addr})')
            return fn(*args,**kwargs)
        return inner
    return deco


@Pyro5.api.expose
class SchedulerMonitor(object):
    """
    Communication point about scheduler statistics. No locking necessary, since all access is done through Pyro which serializes the calls.
    """

    # class attribute, holding the URI of the instance once exposed over Pyro
    URI: str|None=None

    def __init__(self, ns): # , schedulerStat,lock):
        self.ns = ns
        self.stat = SchedulerStat()
        self.lock = threading.RLock()
        global schedulerStatFile
        if (Path(schedulerStatFile).is_file()):
            try:
                self.stat=SchedulerStat.load_from_file(schedulerStatFile)
            except:
                log.exception(f'Failure reading persistent schedule statistics from {schedulerStatFile}, starting from scratch.')
        # the totals and the history carry over from the previous run, but nothing is queued or
        # running in this new process yet
        self.stat.tasks.running=0
        self.stat.tasks.scheduled=0
        # size the history lists (empty in a new SchedulerStat) before anything is stored in them
        self.stat.advanceTime()

    def runServer(self):
        log.info("SchedulerMonitor: runningServer")
        return mp.pyroutil.runServer(ns=self.ns, appName="mupif.scheduler", app=self, metadata={"type:scheduler"})

    def advanceTime(self):
        with self.lock: self.stat.advanceTime()

    @pyro_only()
    def getStatistics(self,raw=False):
        # raw=True makes it suitable for reconstructing the model on the other side
        # the default raw=False will return data translated to the old format
        with self.lock:
            self.advanceTime()
            s=self.stat.model_copy(deep=True)
        if raw: return s.model_dump(mode='json')
        return dict(
            runningTasks     = s.tasks.running,
            scheduledTasks   = s.tasks.scheduled,
            processedTasks   = s.tasks.processed,
            finishedTasks    = s.tasks.finished,
            failedTasks      = s.tasks.failed,
            lastJobs         = [list(j.model_dump(mode='json').values()) for j in s.lastJobs],
            currentLoad      = s.load,
            processedTasks48 = s.hist48h.processed,
            pooledTasks48    = s.hist48h.pooled,
            finishedTasks48  = s.hist48h.finished,
            failedTasks48    = s.hist48h.failed,
            load48           = s.hist48h.load,
        )

    @staticmethod
    def getExecutions(status='Running'):
        try:
            return restApiControl.getExecutionRecords(status=status)
        except Exception as e:
            log.error(repr(e))
            return []
    def stop(self):
        """
        Ask the scheduler to stop and unregister it from the nameserver.

        The main loop finishes its current iteration and exits; executions already handed to the
        worker pool are waited for (see :func:`stopPool`). Exposed over Pyro like the other public
        methods of this class.
        """
        stopEvent.set()
        self.ns.remove("mupif.scheduler")
    # no-op: runServer wants this for some reason?
    def registerPyro(self,*,daemon,ns,uri,appName,exclusiveDaemon): 
        pass

    # The update methods below are called by the main loop and by the pool workers (other
    # processes, hence through Pyro) and keep these invariants: "scheduled" counts executions
    # handed to the pool but not started, "running" those whose workflow subprocess runs;
    # "processed" = "finished" + "failed" counts executions that reached a final status. Pyro
    # serves calls from several threads, so every update holds the lock.

    @pyro_only(no_remote=True)
    def updateEnqueued(self,we_id):
        """An execution was handed to the worker pool."""
        with self.lock:
            self.stat.advanceTime()
            self.stat.tasks.scheduled+=1
            self.stat.hist48h.pooled[-1]+=1
            self.stat.sync()

    @pyro_only(no_remote=True)
    def updateDropped(self,we_id):
        """An enqueued execution ended before its workflow subprocess was started."""
        with self.lock:
            self.stat.advanceTime()
            self.stat.tasks.scheduled=max(0,self.stat.tasks.scheduled-1)
            self.stat.sync()

    @pyro_only(no_remote=True)
    def updateRunning(self,we_id,wid):
        """The workflow subprocess of an enqueued execution is starting."""
        with self.lock:
            self.stat.advanceTime()
            self.stat.tasks.scheduled=max(0,self.stat.tasks.scheduled-1)
            self.stat.tasks.running+=1
            self.stat.lastJobNew(SchedulerStat.JobInfo(we_id=we_id,wid=wid,status='Running',started=datetime.datetime.now(),finished=None))
            self.stat.sync()

    @pyro_only(no_remote=True)
    def updateFinished(self,retCode,we_id):
        """
        The workflow subprocess ended with exit code *retCode*.

        The outcome follows :func:`statusForExitCode`: an execution returned to Pending (no
        resources) is not counted as processed, an execution killed by a signal counts as failed.
        """
        status=statusForExitCode(retCode)
        with self.lock:
            self.stat.advanceTime()
            self.stat.tasks.running=max(0,self.stat.tasks.running-1)
            if status!='Pending':
                self.stat.tasks.processed+=1
                self.stat.hist48h.processed[-1]+=1
                if status=='Finished':
                    self.stat.tasks.finished+=1
                    self.stat.hist48h.finished[-1]+=1
                else:
                    self.stat.tasks.failed+=1
                    self.stat.hist48h.failed[-1]+=1
            self.stat.lastJobDone(we_id=we_id,status=status,finished=datetime.datetime.now())
            self.stat.sync()

    @pyro_only(no_remote=True)
    def persistStat(self):
        with self.lock:
            self.stat.advanceTime()
            self.stat.save_to_file(schedulerStatFile)




def copyLogToDB (we_id, workflowLogName):
    """
    Upload the workflow log to the database and link it to the execution as ``ExecutionLog``.

    The log is stored as a file only; it is not repeated line by line in the scheduler's own log,
    where it only duplicated the workflow output (users read it from the execution record).

    :param we_id: execution ID
    :param workflowLogName: path of the log file written by the workflow subprocess
    """
    try:
        log.info("Copying log files to database")
        with open(workflowLogName, 'rb') as f:
            logID = restApiControl.uploadBinaryFile(f)
            if logID is not None:
                restApiControl.setExecutionParameter(we_id, 'ExecutionLog', logID)
            log.info("Copying log files done")
    except Exception as e:
        log.error(repr(e))


def updateMonitor(monitor, method: str, *args) -> None:
    """
    Report to the statistics monitor; best effort, never raises.

    Statistics must not change the outcome of an execution: e.g. when the scheduler process was
    killed while its workers kept running, the monitor is gone, and a failing statistics call would
    otherwise turn a finished execution into a failed one.

    :param monitor: Pyro proxy of :class:`SchedulerMonitor`
    :param method: name of the monitor method to call
    :param args: arguments of the call
    """
    try:
        getattr(monitor, method)(*args)
    except Exception:
        log.exception(f'Statistics update {method}{args} failed (ignored).')


class NotScheduledError(Exception):
    """The execution handed to a worker is no longer in the ``Scheduled`` state; it is left alone."""


def executeWorkflow(we_id: str) -> None:
    """
    Run execution *we_id* in this pool worker; never let an execution stay ``Scheduled``.

    Errors are handled here because the pool would only log them: an error before the workflow
    subprocess was launched (REST API unreachable, records missing, ...) returns the execution to
    ``Pending`` so that it is retried, an error after the launch marks it ``Failed``. An execution
    that is not ``Scheduled`` any more (changed by someone else meanwhile) is not touched.

    :param we_id: execution ID; the execution was set to ``Scheduled`` before it was enqueued
    """
    progress = {'launched': False}
    try:
        log.info("executeWorkflow invoked")
        return executeWorkflow_inner1(we_id, progress)
    except NotScheduledError:
        log.exception("Execution %s is not Scheduled any more, not running it." % we_id)
        updateMonitor(Pyro5.api.Proxy(SchedulerMonitor.URI), 'updateDropped', we_id)
    except Exception:
        log.exception("Execution of workflow %s failed." % we_id)
        if not progress['launched']:
            updateMonitor(Pyro5.api.Proxy(SchedulerMonitor.URI), 'updateDropped', we_id)
        try:
            if progress['launched']:
                restApiControl.setExecutionStatus(we_id, 'Failed')
                my_email.sendEmailAboutExecutionStatus(we_id)
            else:
                restApiControl.setExecutionStatus(we_id, 'Pending', revertPending=True)
        except Exception:
            # REST API unreachable: nothing more can be done from here, the record is recovered
            # when the scheduler restarts (orphaned Scheduled/Running executions)
            log.exception("Could not update status of execution %s after the failure." % we_id)

def executeWorkflow_inner1(we_id: str, progress: dict) -> None:
    we_rec = restApiControl.getExecutionRecord(we_id)
    if we_rec is None:
        log.error("Workflow Execution record %s not found" % we_id)
        raise KeyError("Workflow Execution record %s not found" % we_id)
    else:
        log.info("Workflow Execution record %s found" % we_id)

    workflowVersion = we_rec.WorkflowVersion
    wid = we_rec.WorkflowID
    workflow_record = restApiControl.getWorkflowRecord(wid=wid, version=workflowVersion)
    if workflow_record is None:
        log.error("Workflow document with wid %s, verison %s not found" % (wid, workflowVersion))
        raise KeyError("Workflow document with ID %s, version %s not found" % (wid, workflowVersion))
    else:
        log.info("Workflow document with wid %s, id %s, version %s found" % (wid, we_rec.dbID, workflowVersion))

    # check if status is "Scheduled"
    if we_rec.Status == 'Scheduled':
        return executeWorkflow_inner2(we_id,we_rec,workflow_record,progress)
    else:
        raise NotScheduledError("WEID %s not scheduled for execution (status %s)" % (we_id, we_rec.Status))

def executeWorkflow_inner2(we_id: str, we_rec, workflow_record, progress: dict) -> None:
    '''Process workflow which is already scheduled; sets progress['launched'] when the workflow subprocess starts'''
    _mon=Pyro5.api.Proxy(SchedulerMonitor.URI)
    wid = we_rec.WorkflowID
    completed = 1  # todo check
    log.info("we_rec status is Scheduled, processing")
    # execute the selected workflow
    # take workflow source and run python interpreter on it in a temporary directory
    tempRoot = '/tmp'
    log.info("Creating temp dir")
    with tempfile.TemporaryDirectory(dir=tempRoot, prefix='mupifDB') as tempDir:
        # if (1):  # uncomment this to keep temdDir
        #     tempDir = tempfile.mkdtemp(dir=tempRoot, prefix='mupifDB_')
        log.info("temp dir %s created" % (tempDir,))
        workflowLogName = tempDir+'/workflow.log'
        execScript = Path(tempDir+'/workflow_execution_script.py')
        # copy workflow source to tempDir
        try:
            executeWorkflow_copyInputs(we_id,workflow_record,tempDir,execScript)
        except Exception as e:
            log.exception('Error in executeWorkflow_inner2')
            # set execution code to failed ...yes or no?
            restApiControl.setExecutionStatus(we_id,'Failed')
            my_email.sendEmailAboutExecutionStatus(we_id)
            return None
        # execute
        log.info("Executing we_id %s, tempdir %s" % (we_id, tempDir))
        # update status
        updateMonitor(_mon, 'updateRunning', we_id, wid)
        # updateStatRunning(lock, schedulerStat, we_id, wid)
        #runningJobs[we_id]=wid # for runtime monitoring
        restApiControl.setExecutionStatus(we_id,'Running')
        restApiControl.setExecutionAttemptsCount(we_id, we_rec.Attempts+1)
        # uses the same python interpreter as the current process
        cmd = [sys.executable, execScript, '-eid', str(we_id)]
        # log.info(cmd)
        with open(workflowLogName, 'w') as workflowLog:
            ll = 10*'='
            workflowLog.write(textwrap.dedent(f'''
                {ll} WORKFLOW STARTING at {(t0:=datetime.datetime.now()).isoformat(timespec='seconds')} {ll}
                {ll} command is {cmd} {ll}'''))
            env = os.environ.copy()
            if 'PYTHONPATH' in env:
                env['PYTHONPATH'] += f'{os.pathsep}{mupifDBSrcDir}'
            else:
                env['PYTHONPATH'] = mupifDBSrcDir
            env['MUPIF_NS'] = ns_uri
            env['API_CREDENTIALS_FILE'] = os.environ.get('API_CREDENTIALS_FILE', "None")

            progress['launched'] = True
            completed = subprocess.call(cmd, cwd=tempDir, stderr=subprocess.STDOUT, stdout=workflowLog, env=env)
            workflowLog.write(textwrap.dedent(f'''
                {ll} WORKFLOW FINISHED at {(t1:=datetime.datetime.now()).isoformat(timespec='seconds')} {ll}
                {ll} duration: {str((dt:=(t1-t0))-datetime.timedelta(microseconds=dt.microseconds))} {ll}
                {ll} exit status of {cmd}: {completed} ({'ERROR' if completed!=0 else 'SUCCESS'}) {ll}'''))

        # log.info(tempDir)
        log.info('command:' + str(cmd) + ' Return Code:'+str(completed))

        # store execution log
        logID = None

        p = Path(tempDir)
        for it in p.iterdir():
            log.info(it)

        try:
            copyLogToDB(we_id, workflowLogName)
        except:
            log.info("Copying log files was not successful")

        # update status
        updateMonitor(_mon, 'updateFinished', completed, we_id)
        # updateStatFinished(lock, schedulerStat, completed, we_id)
        # del runningJobs[we_id] # remove we_id from running jobs; for monitoring
    status = statusForExitCode(completed)
    log.info("Updating we_id %s status to %s (exit code %s)" % (we_id, status, completed))
    if status == 'Pending':
        log.warning("Workflow execution %s could not be initialized due to lack of resources" % we_id)
        restApiControl.setExecutionStatus(we_id, 'Pending', revertPending=True)
    else:
        log.warning("Workflow execution %s %s" % (we_id, status))
        restApiControl.setExecutionStatus(we_id, status)
        my_email.sendEmailAboutExecutionStatus(we_id)


def statusForExitCode(exitCode: int) -> Literal['Finished', 'Failed', 'Pending']:
    """
    Map the exit code of ``workflow_execution_script.py`` to the execution status to set.

    The script exits with 0 when the workflow finished, 1 when it failed and 2 when a model
    server had no free resources (see its module documentation). Every other code -- notably the
    negative codes of a process killed by a signal (OOM killer, manual kill), or an interpreter
    crash -- also means the workflow did not finish and is reported as ``Failed``, so that no
    execution is ever left in ``Running``.

    :param exitCode: return code of the execution subprocess (negative: killed by that signal)
    :return: ``'Finished'``, ``'Failed'`` or ``'Pending'`` (retry later, the attempt count is kept)
    """
    if exitCode == 0:
        return 'Finished'
    if exitCode == 2:
        return 'Pending'
    return 'Failed'


def executeWorkflow_copyInputs(we_id,workflow_record,tempDir,execScript) -> None:
    python_script_filename = workflow_record.modulename + ".py"

    fc, fn = restApiControl.getBinaryFileByID(workflow_record.GridFSID)
    with open(tempDir + '/' + fn, "wb") as f:
        f.write(fc)
        f.close()

    if fn.split('.')[-1] == 'py':
        log.info("downloaded .py file..")
        if fn == python_script_filename:
            log.info("Filename check OK")
        else:
            log.info("Filename check FAILED")

    elif fn.split('.')[-1] == 'zip':
        log.info("downloaded .zip file, extracting..")
        log.info(fn)
        zf = zipfile.ZipFile(tempDir + '/' + fn, mode='r')
        filenames = zipfile.ZipFile.namelist(zf)
        log.info("Zipped files:")
        log.info(filenames)
        zf.extractall(path=tempDir)
        if python_script_filename in filenames:
            log.info("Filename check OK")
        else:
            log.error("Filename check FAILED")

    else:
        log.error("Unsupported file extension")

    log.info("Copying executor script.")

    shutil.copy(mupifDBModDir+'/workflow_execution_script.py', execScript)


def stopPool(var_pool):
    try:
        log.info("Stopping the scheduler, waiting for workers to terminate")
        # nothing runs once the pool is gone; the processed total carries over to the next run
        restApiControl.setStatScheduler(runningTasks=0, scheduledTasks=0, load=0)
        var_pool.close()  # close pool
        var_pool.join()  # wait for completion
        log.info("All tasks finished, exiting")
    except Exception as e:
        log.error(repr(e))


def workflowJobmanagers(wid: str, version: int) -> list[str]:
    """
    List the jobmanagers (model servers) a workflow allocates its models from.

    :param wid: workflow ID
    :param version: workflow version
    :return: one entry per model with a remote jobmanager (a jobmanager serving two models of the
             workflow appears twice); models running locally in the workflow process are omitted
    :raises Exception: if the workflow record cannot be read
    """
    workflow = restApiControl.getWorkflowRecord(wid, int(version))
    return [m.Jobmanager for m in workflow.Models if m.Jobmanager]


def freeJobSlots(jobmanager: str) -> int | None:
    """
    Ask a jobmanager how many more jobs it can run now.

    :param jobmanager: name of the jobmanager in the nameserver
    :return: number of free job slots, or None if the jobmanager is unknown or cannot be reached
    """
    try:
        return int(mp.pyroutil.connectModelServer(ns, jobmanager).getNumberOfFreeJobs())
    except Exception:
        log.exception(f'Cannot get free job slots of jobmanager {jobmanager}')
        return None


class ResourceBudget:
    """
    Free jobmanager slots for one scheduling pass.

    The number of free slots of each jobmanager is read once per pass (on first use) and every
    execution enqueued in the pass takes one slot per model it needs. Without this, all pending
    executions of a pass were checked against the same free slots -- the slots are only taken
    when the workflow subprocess allocates its models -- so many more executions were started than
    the model servers could serve. Slots taken by executions enqueued in an earlier pass but not
    started yet are not known here; such an execution finds no free slot when it starts, exits with
    code 2 and is returned to Pending.
    """

    #: results of :meth:`reserve`
    OK, BUSY, UNAVAILABLE = 'ok', 'busy', 'unavailable'

    def __init__(self):
        #: jobmanager name -> free slots left in this pass (None: jobmanager not reachable)
        self.free: dict[str, int | None] = {}
        #: (workflow ID, version) -> jobmanagers needed (see workflowJobmanagers), cached for the pass
        self.workflows: dict[tuple, list[str]] = {}

    def reserve(self, wid: str, version: int) -> str:
        """
        Take the slots needed by one execution of the given workflow, if all are available.

        :param wid: workflow ID of the execution
        :param version: workflow version
        :return: :attr:`OK` if the slots were taken (the execution may be started); :attr:`BUSY` if
                 the jobmanagers are reachable but have too few free slots (the execution just has
                 to wait); :attr:`UNAVAILABLE` if a jobmanager or the workflow record cannot be
                 reached (a real problem: counted as a failed attempt by the caller)
        """
        key = (wid, int(version))
        if key not in self.workflows:
            try:
                self.workflows[key] = workflowJobmanagers(wid, version)
            except Exception:
                log.exception(f'Cannot read models of workflow {wid} version {version}')
                return self.UNAVAILABLE
        needed: dict[str, int] = {}
        for jm in self.workflows[key]:
            needed[jm] = needed.get(jm, 0) + 1
        for jm in needed:
            if jm not in self.free:
                self.free[jm] = freeJobSlots(jm)
        if any(self.free[jm] is None for jm in needed):
            return self.UNAVAILABLE
        if any(self.free[jm] < n for jm, n in needed.items()):
            return self.BUSY
        for jm, n in needed.items():
            self.free[jm] -= n
        return self.OK


#: upper bound on the number of interrupted executions recovered at startup (the REST API pages its results)
RECOVERY_LIMIT = 100000


def runningExecutionProcesses() -> set[str]:
    """
    Find execution subprocesses alive on this host.

    :return: execution IDs of all running ``workflow_execution_script.py -eid <ID>`` processes
    """
    import psutil
    weids = set()
    for proc in psutil.process_iter(['cmdline']):
        cmd = proc.info['cmdline'] or []
        if any(c.endswith('workflow_execution_script.py') for c in cmd) and '-eid' in cmd[:-1]:
            weids.add(cmd[cmd.index('-eid') + 1])
    return weids


def recoverInterruptedExecutions() -> None:
    """
    Return executions left ``Scheduled`` or ``Running`` by a previous scheduler run to ``Pending``.

    Executions in these states belong to the scheduler (there is a single scheduler per
    deployment), so at startup they can only be leftovers of a previous run that crashed or was
    killed: queued in its worker pool but never started, or interrupted while running. They are
    returned to ``Pending`` (keeping their attempt count) and go through the normal scheduling.
    An execution whose workflow process is still alive on this host is left alone: the worker of a
    killed scheduler survives and finishes it.
    """
    alive = runningExecutionProcesses()
    for status in ('Scheduled', 'Running'):
        try:
            records = restApiControl.getExecutionRecords(status=status, num_limit=RECOVERY_LIMIT)
        except Exception:
            log.exception(f'Could not list {status} executions to recover')
            continue
        for rec in records:
            if rec.dbID in alive:
                log.info(f'{rec.dbID} is {status}, its workflow process is still running; left alone')
                continue
            log.warning(f'{rec.dbID} was left {status} by a previous scheduler run; returning it to Pending')
            try:
                restApiControl.setExecutionStatus(rec.dbID, 'Pending', revertPending=True)
            except Exception:
                log.exception(f'Could not return {rec.dbID} to Pending')


def scheduler_schedule_pending(pool):
    # retrieve weids with status "Scheduled" from DB
    try:
        # oldest submitted first: executions are started in the order they were submitted
        pending_executions = restApiControl.getPendingExecutions(num_limit=poolsize*10, sort='SubmittedDate')
    except Exception as e:
        log.exception('Error checking pending executions')
        pending_executions = []

    monitor=Pyro5.api.Proxy(SchedulerMonitor.URI)
    budget = ResourceBudget()

    for wed in pending_executions:
        weid = wed.dbID
        assert weid is not None # make pyright happy
        log.info(f'{weid} found as pending ({wed.Attempts=})')
        # check number of attempts for execution
        if int(wed.Attempts) > 12:
            try:
                restApiControl.setExecutionStatus(weid,'Created')
                my_email.sendEmailAboutExecutionStatus(weid)
            except Exception as e:
                log.exception('')
        else:
            resources = budget.reserve(wed.WorkflowID, wed.WorkflowVersion)
            if resources == ResourceBudget.OK:
                # change status to Scheduled, then add the corresponding weid to the pool; a worker
                # only runs Scheduled executions, so there is no point in enqueuing one whose status
                # could not be changed -- it stays Pending and is tried again in the next iteration
                try:
                    restApiControl.setExecutionStatus(weid,'Scheduled')
                except Exception:
                    log.exception(f'Could not set {weid} to Scheduled; not enqueuing it')
                    continue

                result = pool.apply_async(executeWorkflow, args=(weid,), callback=procFinish, error_callback=procError)
                updateMonitor(monitor, 'updateEnqueued', weid)
                # log.info(result.get())
                log.info(f"WEID {weid} added to the execution pool")
            elif resources == ResourceBudget.BUSY:
                # the model servers are working on other executions: wait without using up an
                # attempt, otherwise a long queue alone would exhaust the attempt limit
                log.info(f"WEID {weid} waits for free model server slots")
            else:
                log.info(f"WEID {weid} cannot be scheduled due to unavailable resources")
                try:
                    restApiControl.setExecutionAttemptsCount(weid, wed.Attempts + 1)
                except Exception as e:
                    log.exception('Failure increasing the attempt count (for execution with resources unavailable)')


    # display progress (consider use of tqdm)
    lt = time.localtime(time.time())
    try:
        stat=SchedulerStat.model_validate(monitor.getStatistics(raw=True))
        log.info(f'Scheduled/Running/Load: {stat.tasks.scheduled}/{stat.tasks.running}/{stat.load}')
    except Exception as e:
        log.exception('')


# callbacks for the task pool
def procInit():     pass
def procFinish(r):  pass
def procError(e):   log.error('Error running pool task:',exc_info=e)


def runLoop(pool, monitor) -> None:
    """
    Schedule pending executions every :data:`LOOP_SLEEP_SEC` seconds until :data:`stopEvent` is set.

    :param pool: worker pool which runs the executions
    :param monitor: Pyro proxy of the :class:`SchedulerMonitor`, used to persist the statistics
    """
    while not stopEvent.is_set():
        try:
            # add new executions (Pending)
            scheduler_schedule_pending(pool)
            # lazy update of persistent statistics, done in main thread thus thread safe
            monitor.persistStat()
        except Exception:
            # e.g. REST API or nameserver temporarily unreachable: an error in one iteration must
            # not end the scheduler (executions already in the pool keep running); try again in
            # the next iteration
            log.exception('Error in scheduler iteration; continuing with the next one')
        # returns early when a stop is requested
        stopEvent.wait(LOOP_SLEEP_SEC)


def main():
    """
    Run the scheduler until it is stopped.

    Everything with side effects happens here rather than at import time, so that the module can be
    imported (e.g. by tests) without sleeping, connecting to the nameserver or renaming the process.
    """
    global ns, ns_uri
    time.sleep(STARTUP_DELAY_SEC)
    multiprocessing.current_process().name='mupifDB-scheduler'
    # show remote traceback when remote calls fail
    sys.excepthook = Pyro5.errors.excepthook
    ns = mp.pyroutil.connectNameserver()
    ns_uri = str(ns._pyroUri)

    import requests.adapters
    import urllib3
    # FIXME: session not used at all
    adapter = requests.adapters.HTTPAdapter(max_retries=urllib3.Retry(total=8, backoff_factor=.05))
    session = requests.Session()
    for proto in ('http://', 'https://'):
        session.mount(proto, adapter)
    # (the statistics in the database are set from the persisted ones when the monitor starts)

    if 1:
        _monitor = SchedulerMonitor(ns)
        _monitor.stat.sync()
        SchedulerMonitor.URI=_monitor.runServer()
        monitor=Pyro5.api.Proxy(SchedulerMonitor.URI)



        # https://github.com/mupif/mupifDB/issues/14
        # explicitly use forking (instead of spawning) so that there are no leftover monitoring processes
        # if scheduler gets killed externally (such as in the docker by supervisor, which won't reap
        # orphan processes
        # even though fork is the default for POSIX, mp.simplejobmanager was setting the (global)
        # default to spawn, therefore it was used here as well. So better to be explicit (and also
        # fix the — perhaps unnecessary now? — global setting in simplejobmanager)
        pool = multiprocessing.get_context('fork').Pool(processes=poolsize, initializer=procInit)
        atexit.register(stopPool, pool)
        try:
            with pidfile.PIDFile(filename='mupifDB_scheduler_pidfile'):
                log.info("Starting MupifDB Workflow Scheduler")

                sys.excepthook = Pyro5.errors.excepthook

                try:
                    log.info("Recovering executions interrupted by a previous scheduler run…")
                    recoverInterruptedExecutions()
                    log.info("Done")

                    log.info("Entering main loop to check for Pending executions")
                    runLoop(pool, monitor)

                except Exception as err:
                    log.exception("Error in workflow execution")
                    stopPool(pool)
                except:
                    log.exception("Unknown error encountered?!")
                    stopPool(pool)
        except pidfile.AlreadyRunningError:
            log.error('Already running.')
    log.info("Exiting MupifDB Workflow Scheduler\n")

if __name__ == '__main__':
    main()

