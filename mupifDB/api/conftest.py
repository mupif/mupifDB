"""Fixtures of the API tests: a throwaway MongoDB server (skipped when ``mongod`` is missing)."""

import os
import shutil
import socket
import subprocess
import tempfile

import pymongo
import pytest

#: mongod binary: $MUPIFDB_TEST_MONGOD, else the one on $PATH, else the traditional system location
MONGOD = os.environ.get('MUPIFDB_TEST_MONGOD') or shutil.which('mongod') or '/usr/bin/mongod'


@pytest.fixture(scope='session')
def mongo():
    """A mongod on a free port for the whole test session; yields a client (its port: ``client.PORT``)."""
    if not os.path.exists(MONGOD):
        pytest.skip(f'mongod not found ({MONGOD})')
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    tmp = tempfile.mkdtemp(prefix='mupifdb-test-api-')
    proc = subprocess.Popen([MONGOD, '--port', str(port), '--bind_ip', '127.0.0.1', '--dbpath', tmp, '--quiet'],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    client = pymongo.MongoClient(f'mongodb://127.0.0.1:{port}', serverSelectionTimeoutMS=20000)
    client.PORT = port
    try:
        client.admin.command('ping')
        yield client
    finally:
        client.close()
        proc.terminate()
        proc.wait(10)
        shutil.rmtree(tmp, ignore_errors=True)
