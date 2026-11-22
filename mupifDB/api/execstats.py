"""
Counts of workflow executions by status, shared by the REST API (:mod:`mupifDB.api.main`) and the
unauthenticated monitoring API (:mod:`mupifDB.api.safeapi`).

Only aggregate numbers leave this module (no execution IDs, workflows or users), so the result
can be published by the monitoring API without exposing any data of the users.
"""

from mupifDB import models


def executionStatusCounts(mongoDb) -> models.MupifDBStatus_Model.ExecutionStatistics_Model:
    """
    Count the executions in the database by status, with one aggregation query.

    :param mongoDb: the MuPIF database (``pymongo`` database object, e.g. ``MongoClient(...).MuPIF``)
    :return: total number of executions and the numbers by status; statuses without executions
             are 0
    """
    res = mongoDb.WorkflowExecutions.aggregate([
        {"$group": {"_id": "$Status", "count": {"$sum": 1}}}
    ])
    vals = {}
    tot = 0
    for r in res:
        vals[r['_id']] = r['count']
        tot += r['count']
    return models.MupifDBStatus_Model.ExecutionStatistics_Model(
        totalExecutions=tot,
        finishedExecutions=vals.get('Finished', 0),
        failedExecutions=vals.get('Failed', 0),
        createdExecutions=vals.get('Created', 0),
        pendingExecutions=vals.get('Pending', 0),
        scheduledExecutions=vals.get('Scheduled', 0),
        runningExecutions=vals.get('Running', 0),
    )
