#!/usr/bin/env python3
"""
Initiate a single-member MongoDB replica set unless it is initiated already.

Used by ``make mongo-setup`` for the local development stack instead of ``mongosh`` (which is
a separate download). A replica set is needed for MongoDB transactions and change streams.

Usage::

    python3 tools/mongo_replset_init.py [HOST:PORT] [SET_NAME]

Defaults are ``127.0.0.1:11002`` and ``rs0``, matching the ``mongo`` target of the Makefile.
The script is idempotent: running it against an already initiated replica set does nothing.
"""

import sys

import pymongo
import pymongo.errors


def initReplicaSet(host: str = '127.0.0.1:11002', name: str = 'rs0') -> bool:
    """
    Initiate replica set *name* with the single member *host*, if not done already.

    :param host: ``host:port`` of the mongod started with ``--replSet name``
    :param name: replica set name (must match the ``--replSet`` option of mongod)
    :return: true if the replica set was initiated now, false if it already existed
    :raises pymongo.errors.PyMongoError: if mongod cannot be reached or refuses the configuration
    """
    # directConnection: the replica set does not exist yet, so the driver must not try to discover it
    admin = pymongo.MongoClient(f'mongodb://{host}/?directConnection=true', serverSelectionTimeoutMS=10000).admin
    try:
        admin.command('replSetGetStatus')
        return False
    except pymongo.errors.OperationFailure:
        # NotYetInitialized: the only expected failure here
        admin.command('replSetInitiate', {'_id': name, 'members': [{'_id': 0, 'host': host}]})
        return True


if __name__ == '__main__':
    created = initReplicaSet(*sys.argv[1:3])
    print('replica set ' + ('initiated' if created else 'already initiated'))
