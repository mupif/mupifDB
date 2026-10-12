"""
Scheduler statistics: counters of queued, running and processed executions with a 48 h history.

Moved from ``mupifDB/workflowscheduler.py``; the statistics file format is unchanged.
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
        running: int=0
        scheduled: int=0
        processed: int=0
        finished: int=0
        failed: int=0
    tasks: Tasks=Tasks()
    load: float=0.
    class JobInfo(mp.BareData):
        we_id: str
        wid: str
        status: Literal['Running','Finished','Failed','Pending']  # Pending: returned to the queue (no resources)
        started: datetime.datetime
        finished: Optional[datetime.datetime]
    lastJobs: List[JobInfo]=[]
    class Hist(mp.BareData):
        interval: int=60*60
        count: int=48
        headPeriod: int=0
        processed: List[int]=[]
        pooled: List[int]=[]
        finished: List[int]=[]
        failed: List[int]=[]
        load: List[float]=[]
        def reset(self):
            self.moveBy(2*self.count)
            self.headPeriod=0
        def moveBy(self,n):
            def m(seq,n): seq[:]=seq[n:]+min(n,self.count)*[0] # modify in-place
            m(self.processed,n); m(self.pooled,n); m(self.finished,n); m(self.failed,n); m(self.load,n)
        def selfCheck(self):
            if set([len(s) for s in (self.processed,self.pooled,self.finished,self.failed,self.load)])!=set([self.count]): self.reset()
        def advance(self):
            self.selfCheck()
            per=int(time.time()//self.interval)
            if per==self.headPeriod: return # nothing to do
            elif per<self.headPeriod: raise RuntimeError('Traveling to the past?')
            else: self.moveBy(per-self.headPeriod)
            self.headPeriod=per
    hist48h: Hist=Hist(interval=3600,count=48)

    def advanceTime(self):
        self.hist48h.advance()
    def lastJobNew(self, job: JobInfo, max=5) -> None:
        self.lastJobs.append(job)
        self.lastJobs=self.lastJobs[-max:]
    def lastJobDone(self, we_id, status: Literal['Finished','Failed','Pending'], finished: datetime.datetime):
        match=[j for j in self.lastJobs if j.we_id==we_id]
        if not match: return # job already gone
        if len(match)>1:
            log.error('Multiple lastJobs with {we_id=}??')
            return
        match[0].finished=finished
        match[0].status=status

    def updateLoad(self) -> None:
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
        with open(f,'r') as f: return SchedulerStat.model_validate_json(f.read())
    def save_to_file(self,f):
        open(f,'w').write(self.model_dump_json())
