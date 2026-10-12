"""
Scheduler statistics: counters of queued, running and processed executions with a 48 h history.

Moved from ``mupifDB/workflowscheduler.py``; the statistics file format is unchanged. The meaning of
the counters is kept by :class:`SchedulerMonitor`, which is the only code changing them.
"""

import datetime
import logging
import time
from typing import ClassVar, List, Literal, Optional

import mupif as mp

from mupifDB import restApiControl

# the logger of the scheduler, as before the move
log=logging.getLogger('scheduler')


class SchedulerStat(mp.BareData):
    """
    Statistics of the scheduler: current counters, last executions and an hourly 48 h history.

    Persisted as JSON across restarts (:meth:`save_to_file`, :meth:`load_from_file`) and copied to
    the database by :meth:`sync`. Not thread-safe: :class:`SchedulerMonitor` serializes access.
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
        History of counts in fixed time periods: the last element is the current period.

        Each list has :attr:`count` elements; :meth:`advance` shifts them when a new period has
        begun, dropping the oldest values.
        """

        #: length of one period in seconds
        interval: int=60*60
        #: number of periods kept
        count: int=48
        #: index of the current period (seconds since the epoch // :attr:`interval`)
        headPeriod: int=0
        processed: List[int]=[]
        pooled: List[int]=[]
        finished: List[int]=[]
        failed: List[int]=[]
        load: List[float]=[]

        def reset(self):
            """Set all lists to :attr:`count` zeros."""
            self.moveBy(2*self.count)
            self.headPeriod=0

        def moveBy(self,n):
            """
            Shift all lists by *n* periods, filling the new periods with zeros.

            :param n: number of periods; *n* >= :attr:`count` clears the history
            """
            def m(seq,n): seq[:]=seq[n:]+min(n,self.count)*[0] # modify in-place
            m(self.processed,n); m(self.pooled,n); m(self.finished,n); m(self.failed,n); m(self.load,n)

        def selfCheck(self):
            """Reset the history if any list does not have :attr:`count` elements (new or damaged statistics)."""
            if set([len(s) for s in (self.processed,self.pooled,self.finished,self.failed,self.load)])!=set([self.count]): self.reset()

        def advance(self):
            """
            Make the last element of each list correspond to the current period.

            :raises RuntimeError: if the clock went back to an earlier period
            """
            self.selfCheck()
            per=int(time.time()//self.interval)
            if per==self.headPeriod: return # nothing to do
            elif per<self.headPeriod: raise RuntimeError('Traveling to the past?')
            else: self.moveBy(per-self.headPeriod)
            self.headPeriod=per
    #: hourly history of the last 48 hours
    hist48h: Hist=Hist(interval=3600,count=48)

    def advanceTime(self):
        """Advance the history to the current period (call before updating it)."""
        self.hist48h.advance()

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

    def updateLoad(self) -> None:
        """Compute :attr:`load` from the running executions and store it in the current history period."""
        self.load=int(100*self.tasks.running*1./self.capacity)
        self.hist48h.load[-1]=self.load

    def sync(self):
        """Update the load and copy the counters to the database; best effort, never raises."""
        self.updateLoad()
        try:
            restApiControl.setStatScheduler(
                runningTasks=self.tasks.running,
                scheduledTasks=self.tasks.scheduled,
                processedTasks=self.tasks.processed,
                load=self.load
            )
        except Exception:
            # statistics must not break scheduling (a failure here used to end the main loop)
            log.exception('Could not store scheduler statistics in the database')

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
        Write the statistics as JSON.

        :param f: file name
        """
        open(f,'w').write(self.model_dump_json())
