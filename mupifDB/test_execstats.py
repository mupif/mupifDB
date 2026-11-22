"""Tests of :mod:`mupifDB.api.execstats` and the monitoring endpoint publishing it (no MongoDB needed)."""

import sys
import types
from pathlib import Path


class _FakeDb:
    """Database stub: ``WorkflowExecutions.aggregate`` returns the counts by status like MongoDB's ``$group``."""

    def __init__(self, statuses):
        self.statuses = statuses
        self.WorkflowExecutions = types.SimpleNamespace(aggregate=self._aggregate)

    def _aggregate(self, pipeline):
        assert pipeline == [{"$group": {"_id": "$Status", "count": {"$sum": 1}}}]
        counts = {}
        for s in self.statuses:
            counts[s] = counts.get(s, 0) + 1
        return [{'_id': k, 'count': v} for k, v in counts.items()]


def test_counts_by_status():
    """Every status is counted; statuses without executions are 0; nothing else is returned."""
    from mupifDB.api.execstats import executionStatusCounts
    st = executionStatusCounts(_FakeDb(['Pending', 'Pending', 'Running', 'Finished', 'Failed', 'Finished', 'Created']))
    assert st.model_dump() == dict(totalExecutions=7, finishedExecutions=2, failedExecutions=1, createdExecutions=1,
                                   pendingExecutions=2, scheduledExecutions=0, runningExecutions=1)


def test_safeapi_execution_status2(monkeypatch):
    """The monitoring API serves the counts at /execution-status2/ without authentication."""
    from fastapi.testclient import TestClient
    # safeapi is started from its directory (``uvicorn safeapi:app``), so it is imported as a top-level module
    monkeypatch.syspath_prepend(str(Path(__file__).parent / 'api'))
    monkeypatch.setenv('MUPIF_VPN_NAME', 'test')
    sys.modules.pop('safeapi', None)
    import safeapi
    monkeypatch.setattr(safeapi, 'mongoClient', types.SimpleNamespace(MuPIF=_FakeDb(['Pending', 'Running'])))
    with TestClient(safeapi.app) as client:
        r = client.get('/execution-status2/')
    assert r.status_code == 200
    assert r.json()['pendingExecutions'] == 1 and r.json()['totalExecutions'] == 2
