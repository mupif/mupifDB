"""
Keeping the leases of a scheduler: heartbeat, renewal, lost leases, takeover of dead schedulers' executions.

Each execution a scheduler runs is held under a lease (see :mod:`mupifDB.api.leases`) which expires
unless it is renewed. :class:`LeaseKeeper` renews them in a thread of its own -- not in the main
loop, whose passes can be slow (model servers not answering) and which ends before the running
workflows during a shutdown -- every ``renew_interval`` seconds, in one call together with the
scheduler's heartbeat. It also

* stops the jobs whose lease is gone (another scheduler took the execution over after the lease
  expired, e.g. while this one was cut off from the database): their workflows are stopped and
  nothing more is written for them;
* stops the scheduler if its identity was taken by another process (it was considered dead);
* takes over, every ``takeover_interval`` seconds, executions whose lease expired and whose
  scheduler is not alive, and its own ones whose lease expired because no job holds them any more
  (:meth:`Scheduler.recoverExpired`) -- but only after this scheduler has
  reached the database without interruption for a whole lease period. After a full outage of the
  REST API or the database every lease has expired; the owners renew within one renewal interval
  once it is back, while nobody may take over before a lease period passed, so an outage never
  makes the deployment run its executions again. In a partial partition the schedulers which kept
  their contact take over once the leases of the cut-off one expired.

No scheduler stops its workflows because it lost contact (no self-fencing): writes made under a
lost lease are refused by the API, so a cut-off scheduler cannot overwrite the work of the new
holder, and workflows survive outages of the REST API.
"""

import logging
import threading
import time
from typing import Any, Callable

log = logging.getLogger('scheduler')


class LeaseKeeper:
    """
    Renews the heartbeat and leases of a scheduler, stops jobs whose lease was lost, takes over executions of dead schedulers.

    :param scheduler: the :class:`~mupifDB.scheduler.core.Scheduler`
    :param clock: monotonic clock (seconds); replaced in tests
    """

    def __init__(self, scheduler: Any, clock: Callable[[], float] = time.monotonic):
        self.scheduler = scheduler
        self.clock = clock
        #: time since which every renewal succeeded (None: no contact, or interrupted)
        self.contactSince: float | None = None
        #: time of the last successful renewal (sent)
        self.lastSuccess: float | None = None
        #: time of the last takeover pass
        self.lastTakeover: float = -1e30
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        #: a renewal failed: reported once until one succeeds again
        self._failing = False

    @property
    def config(self):
        return self.scheduler.config

    def mayTakeOver(self, now: float) -> bool:
        """Whether this scheduler has reached the database without interruption for a whole lease period."""
        return self.contactSince is not None and now - self.contactSince >= self.config.lease_seconds

    def step(self) -> None:
        """One renewal (and, when due, a takeover pass); never raises."""
        s = self.scheduler
        sent = self.clock()
        with s.lock:
            jobs = [j for j in s.jobs.values() if j.lease is not None]
        try:
            identity, held = s.backend.heartbeat([j.lease for j in jobs], stats=s.monitor.heartbeatStats(), draining=s.draining.is_set())
        except Exception as e:
            if not self._failing:
                log.warning(f'Could not renew the heartbeat and leases ({e}); retrying every {self.config.renew_interval:.0f} s')
                self._failing = True
            self.contactSince = None
            return
        if self._failing:
            log.warning('Heartbeat and leases renewed again')
            self._failing = False
        # a gap longer than the lease (e.g. the process was suspended) interrupts the contact too
        if self.lastSuccess is None or self.contactSince is None or sent - self.lastSuccess > self.config.lease_seconds:
            self.contactSince = sent
        self.lastSuccess = sent
        if not identity:
            log.critical(f'The scheduler identity {self.config.scheduler_id} was taken by another process (this one was considered dead): stopping')
            for j in jobs:
                j.stop('lease-lost')
            s.stop()
            return
        for j in jobs:
            if j.lease.weid not in held and not j.outcomeStored:
                log.warning(f'{j.weid}: lease lost (taken over by another scheduler); stopping its workflow, nothing more is written for it',
                            extra={'weid': j.weid})
                j.stop('lease-lost')
        now = self.clock()
        if now - self.lastTakeover >= self.config.takeover_interval and not s.stopEvent.is_set():
            self.lastTakeover = now
            if self.mayTakeOver(now):
                s.recoverExpired(self.config.takeover_limit)

    def start(self) -> None:
        """Start the renewal thread (renews at once, then every ``renew_interval``)."""
        def loop():
            while True:
                self.step()
                if self._stop.wait(self.config.renew_interval):
                    return
        self._thread = threading.Thread(target=loop, name='lease-keeper', daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the renewal thread (after the last job ended)."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(10)
