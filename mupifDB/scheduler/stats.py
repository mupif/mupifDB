"""
Scheduler statistics: counters of queued, running and processed executions with their history (48 h by hours, 2 h by minutes).

Moved from ``mupifDB/workflowscheduler.py``; the statistics file format is unchanged. The meaning of
the counters is kept by :class:`SchedulerMonitor`, which is the only code changing them.
"""

import datetime
import logging
import os
import time
from typing import ClassVar, List, Literal, Optional

import mupif as mp
import pydantic

from mupifDB import restApiControl

# the logger of the scheduler, as before the move
log=logging.getLogger('scheduler')


class SchedulerStat(mp.BareData):
    """
    Statistics of the scheduler: current counters, last executions, an hourly 48 h history and a 2 h history by minutes.

    Persisted as JSON across restarts (:meth:`save_to_file`, :meth:`load_from_file`) and copied to
    the database (:meth:`dbValues`, :meth:`storeInDb`). Not thread-safe: :class:`SchedulerMonitor` serializes access.
    """

    #: number of executions that can run at once; the load is the percentage of it in use. Set by
    #: the scheduler at startup; a class variable, so it is not stored in the statistics file.
    capacity: ClassVar[int]=30

    class Tasks(mp.BareData):
        """Current counters (see :class:`SchedulerMonitor` for when they change)."""

        #: workflow processes running now
        running: int=0
        #: executions handed to the worker pool and not started yet
        scheduled: int=0
        #: executions that reached a final status (= finished + failed), since the statistics began
        processed: int=0
        #: executions that ended as Finished
        finished: int=0
        #: executions that ended as Failed
        failed: int=0
        #: executions waiting in the database queue (status ``Pending``), sampled every scheduler
        #: iteration (:meth:`SchedulerMonitor.sample`); the queue is shared by the deployment, it is
        #: not a count of this scheduler's work
        pending: int=0
        #: executions waiting to run: :attr:`pending` (not taken yet) + :attr:`scheduled` (taken by
        #: this scheduler, workflow process not started yet); computed by :meth:`updateGauges`
        waiting: int=0
        #: executions with status ``Running`` in the database, sampled with :attr:`pending` (None:
        #: not known yet). Normally equal to :attr:`running`; it is higher if some ``Running``
        #: executions are not processes of this scheduler (e.g. a workflow that outlived a previous
        #: scheduler run, or a record left behind by a crash), which is how the running counts of
        #: the scheduler and of the database could differ without any explanation before
        dbRunning: Optional[int]=None
    tasks: Tasks=Tasks()
    #: percentage of :attr:`capacity` running now
    load: float=0.

    class JobInfo(mp.BareData):
        """One of the last executions started (shown by the monitoring pages)."""

        we_id: str
        wid: str
        status: Literal['Running','Finished','Failed','Pending']  # Pending: returned to the queue (no resources)
        started: datetime.datetime
        finished: Optional[datetime.datetime]
    #: the last executions started, oldest first (see :meth:`lastJobNew`)
    lastJobs: List[JobInfo]=[]

    class Hist(mp.BareData):
        """
        History in fixed time periods: the last element of each list is the current period.

        Each list has :attr:`count` elements; :meth:`advance` shifts them when a new period has
        begun, dropping the oldest values. There are two kinds of lists:

        * counters (:attr:`COUNTERS`): number of events in the period, e.g. executions processed;
          a new period starts at 0;
        * gauges (:attr:`GAUGES`): the **peak** level reached in the period, e.g. the most
          executions running at once. A new period starts at the current level, not at 0: the
          level only changes when something happens, so a quiet hour with long executions still
          shows them running (its level is carried forward from the previous period).
        """

        #: names of the lists counting events in each period
        COUNTERS: ClassVar[tuple[str,...]]=('processed','pooled','finished','failed')
        #: names of the lists keeping the peak level in each period
        GAUGES: ClassVar[tuple[str,...]]=('load','running','waiting')

        #: length of one period in seconds
        interval: int=60*60
        #: number of periods kept
        count: int=48
        #: index of the current period (seconds since the epoch // :attr:`interval`)
        headPeriod: int=0
        #: executions that reached a final status (counter)
        processed: List[int]=[]
        #: executions taken by the scheduler (counter)
        pooled: List[int]=[]
        #: executions that ended as Finished (counter)
        finished: List[int]=[]
        #: executions that ended as Failed (counter)
        failed: List[int]=[]
        #: peak load in percent of the capacity (gauge; before the gauges were introduced it was
        #: the load at the last update in the period)
        load: List[float]=[]
        #: peak number of workflow processes running (gauge)
        running: List[int]=[]
        #: peak number of executions waiting to run, see :attr:`SchedulerStat.Tasks.waiting` (gauge)
        waiting: List[int]=[]
        #: period of the last "clock went back" warning (not stored: private attribute)
        _pastWarned: Optional[int]=pydantic.PrivateAttr(default=None)

        def lists(self):
            """Return (name, list) of all counter and gauge lists."""
            return [(name,getattr(self,name)) for name in self.COUNTERS+self.GAUGES]

        def reset(self):
            """Set all lists to :attr:`count` zeros."""
            self.moveBy(2*self.count)
            self.headPeriod=0

        def moveBy(self,n,levels:Optional[dict]=None):
            """
            Shift all lists by *n* periods, filling the new periods with zeros (counters) or the current levels (gauges).

            :param n: number of periods; *n* >= :attr:`count` clears the history
            :param levels: current level of each gauge by name (missing: 0); the level has not
                           changed during the periods passed since the last update, so each of
                           them gets it as its peak
            """
            levels=levels or {}
            for name,seq in self.lists():
                fill=levels.get(name,0) if name in self.GAUGES else 0
                seq[:]=seq[n:]+min(n,self.count)*[fill] # modify in-place

        def selfCheck(self):
            """
            Reset each list which does not have :attr:`count` elements (new or damaged statistics).

            Only the wrong list is reset, so that a statistics file written before a list was
            added (e.g. the running/waiting gauges) keeps the history of the other lists.
            """
            for name,seq in self.lists():
                if len(seq)!=self.count: seq[:]=self.count*[0]

        def advance(self,levels:Optional[dict]=None):
            """
            Make the last element of each list correspond to the current period.

            If the wall clock went back to an earlier period (NTP step, VM or WSL clock resync, a
            statistics file written on a host with a clock ahead), the history stays where it is:
            values keep going into the current last period until real time catches up with it.
            This used to raise, and since every counter update advances the history first, an
            exception here skipped the update itself: e.g. a finished execution was never
            subtracted from the running ones, so the scheduler reported it as running forever.

            :param levels: current gauge levels for the new periods, see :meth:`moveBy`
            """
            self.selfCheck()
            per=int(time.time()//self.interval)
            if per==self.headPeriod: return # nothing to do
            elif per<self.headPeriod:
                # warn only once per backward step, not on every update until time catches up
                if per!=self._pastWarned:
                    log.warning(f'Clock went back by {self.headPeriod-per} period(s) of {self.interval} s; keeping the statistics history where it is')
                    self._pastWarned=per
                return
            else: self.moveBy(per-self.headPeriod,levels)
            self.headPeriod=per

        def recordLevel(self,name,value):
            """Raise the peak of gauge *name* in the current period to *value*, if it is higher."""
            seq=getattr(self,name)
            seq[-1]=max(seq[-1],value)

        def periodStart(self):
            """Return the start of the current (last) period in seconds since the epoch."""
            return self.headPeriod*self.interval

    #: hourly history of the last 48 hours
    hist48h: Hist=Hist(interval=3600,count=48)
    #: history of the last 2 hours by minutes (detail of the current load; absent from statistics
    #: files of older versions, which then start with an empty one)
    hist2h: Hist=Hist(interval=60,count=120)

    def histories(self):
        """Return all histories (they are updated together)."""
        return (self.hist48h,self.hist2h)

    def levels(self):
        """Return the current level of each gauge of the histories, by gauge name."""
        return dict(load=self.load,running=self.tasks.running,waiting=self.tasks.waiting)

    def countEvent(self,name,n=1):
        """Add *n* events to counter *name* (e.g. ``processed``) in the current period of every history."""
        for h in self.histories(): getattr(h,name)[-1]+=n

    def advanceTime(self):
        """
        Advance the histories to the current period (call before updating them).

        Periods begun since the last update get the current gauge levels: nothing has changed
        since, so these were the levels during the whole time.
        """
        for h in self.histories(): h.advance(self.levels())

    def lastJobNew(self, job: JobInfo, max=5) -> None:
        """
        Record a started execution, keeping only the last *max* ones.

        :param job: the execution
        :param max: number of executions kept
        """
        self.lastJobs.append(job)
        self.lastJobs=self.lastJobs[-max:]

    def lastJobDone(self, we_id, status: Literal['Finished','Failed','Pending'], finished: datetime.datetime):
        """
        Record the end of a recorded execution; ignored if it is no longer among the last ones.

        :param we_id: execution ID
        :param status: final status, or ``Pending`` if it was returned to the queue
        :param finished: end time
        """
        match=[j for j in self.lastJobs if j.we_id==we_id]
        if not match: return # job already gone
        if len(match)>1:
            log.error(f'Multiple lastJobs with {we_id=}??')
            return
        match[0].finished=finished
        match[0].status=status

    def updateGauges(self) -> None:
        """
        Compute the derived levels (:attr:`load`, :attr:`Tasks.waiting`) and record the peaks in the current history periods.

        Called after every change of the counters, so a peak cannot be missed between two
        samples, however short it was.
        """
        self.load=int(100*self.tasks.running*1./self.capacity)
        self.tasks.waiting=self.tasks.pending+self.tasks.scheduled
        for h in self.histories():
            for name,value in self.levels().items(): h.recordLevel(name,value)

    def dbValues(self) -> dict:
        """Return the counters copied to the database (``PATCH scheduler_statistics``), as keyword arguments of ``setStatScheduler``."""
        return dict(runningTasks=self.tasks.running, scheduledTasks=self.tasks.scheduled, processedTasks=self.tasks.processed,
                    load=int(self.load), waitingTasks=self.tasks.waiting, pendingTasks=self.tasks.pending)

    @staticmethod
    def storeInDb(values: dict) -> bool:
        """
        Copy counters (from :meth:`dbValues`) to the database in one request; best effort, never raises.

        :return: whether they were stored
        """
        try:
            restApiControl.setStatScheduler(**values)
            return True
        except Exception:
            # statistics must not break scheduling (a failure here used to end the main loop)
            log.exception('Could not store scheduler statistics in the database')
            return False

    @staticmethod
    def load_from_file(f):
        """
        Read statistics saved by :meth:`save_to_file`.

        :param f: file name
        :return: the statistics
        :raises Exception: if the file cannot be read or does not contain valid statistics
        """
        with open(f,'r') as f: return SchedulerStat.model_validate_json(f.read())

    def save_to_file(self,f):
        """
        Write the statistics as JSON, atomically.

        The data go to a temporary file in the same directory, which then replaces *f*: a crash
        during the write leaves the previous statistics intact instead of a truncated file (which
        would be discarded at the next start, losing all statistics).

        :param f: file name
        """
        tmp=f'{f}.tmp'
        with open(tmp,'w') as out:
            out.write(self.model_dump_json())
        os.replace(tmp,f)
