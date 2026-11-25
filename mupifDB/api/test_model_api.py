"""
Tests of the model package endpoints of the REST API (mupifDB.api.main) against a throwaway MongoDB.

As in test_lease_api: the API module is imported once the test MongoDB runs; authentication is
replaced by a fixed user (an admin, or a plain user where publishing must be refused).
"""

import importlib
import io
import os
import stat
import sys
import zipfile

import pytest

from mupif import modelpackage as mpk

MANIFEST = '''
ID = "{id}"
Version = "{version}"
Entrypoint = "mymodel.model:MyModel"
RequiresHost = ["abaqus"]
[Runtime]
Type = "pixi"
'''


def archive(id='oofem-thermal', version='1.0', extra=()):
    'A package archive (as mupif.modelpackage.buildArchive would make), with extra (name, content) members.'
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as zf:
        zf.writestr(mpk.MANIFEST_NAME, MANIFEST.format(id=id, version=version))
        zf.writestr('mymodel/__init__.py', '')
        zf.writestr('mymodel/model.py', f'VERSION = {version!r}\n')
        for info, content in extra:
            zf.writestr(info, content)
    return buf.getvalue()


@pytest.fixture(scope='module')
def api(mongo):
    """``(TestClient, main module, users)``: the API on the test MongoDB; ``users['admin']``, ``users['plain']``."""
    os.environ['MUPIFDB_MONGODB_PORT'] = str(mongo.PORT)
    from fastapi.testclient import TestClient
    if 'mupifDB.api.main' in sys.modules:
        main = importlib.reload(sys.modules['mupifDB.api.main'])
    else:
        import mupifDB.api.main as main
    main.db.Models.drop()
    users = {'admin': main.User_Model(mail='admin@test', name='a', surname='a', password='x', rights_admin=True),
             'plain': main.User_Model(mail='server@test', name='s', surname='s', password='x', rights_admin=False)}
    current = {'user': users['admin']}
    main.app.dependency_overrides[main.get_current_authenticated_user] = lambda: current['user']
    with TestClient(main.app) as client:
        client.current = current
        yield client, main, users


def publish(client, data, name='p.zip'):
    return client.post('/api/models', files={'package': (name, data, 'application/zip')})


def test_publish_and_resolve(api):
    client, main, users = api
    data = archive(version='1.9')
    r = publish(client, data)
    assert r.status_code == 200, r.text
    rec = r.json()
    assert (rec['ID'], rec['Version'], rec['Entrypoint'], rec['Runtime'], rec['RequiresHost']) == ('oofem-thermal', '1.9', 'mymodel.model:MyModel', {'Type': 'pixi', 'Environment': 'default'}, ['abaqus'])
    assert rec['Digest'] == mpk.digest(data) and rec['Size'] == len(data) and rec['Publisher'] == 'admin@test'
    assert rec['FileName'] == 'oofem-thermal-1.9.zip' and rec['_id']
    # the archive is downloadable by its GridFS ID, unchanged (servers verify the digest)
    f = client.get(f'/api/file/{rec["GridFSID"]}')
    assert f.status_code == 200 and f.content == data
    assert publish(client, archive(version='1.10')).status_code == 200
    # latest by version order (1.10 after 1.9), or a given version
    assert client.get('/api/models/oofem-thermal/versions/latest').json()['Version'] == '1.10'
    assert client.get('/api/models/oofem-thermal/versions/1.9').json()['Digest'] == rec['Digest']
    assert [r['Version'] for r in client.get('/api/models/oofem-thermal').json()] == ['1.9', '1.10']
    assert [r['Version'] for r in client.get('/api/models', params={'id': 'oofem-thermal'}).json()] == ['1.9', '1.10']
    assert client.get('/api/models/oofem-thermal/versions/7').status_code == 404
    assert client.get('/api/models/nope/versions/latest').status_code == 404
    assert client.get('/api/models/nope').status_code == 404


def test_version_immutable(api):
    client, main, users = api
    assert publish(client, archive(id='immutable', version='1')).status_code == 200
    # the same version again, even with other content: refused
    r = publish(client, archive(id='immutable', version='1', extra=[('more.py', 'x')]))
    assert r.status_code == 409
    assert len(client.get('/api/models/immutable').json()) == 1


def test_publish_needs_admin(api):
    client, main, users = api
    client.current['user'] = users['plain']
    try:
        assert publish(client, archive(id='by-plain')).status_code == 403
        # reading is allowed (execution servers use a plain account)
        assert client.get('/api/models').status_code == 200
    finally:
        client.current['user'] = users['admin']
    assert client.get('/api/models/by-plain').status_code == 404


def test_invalid_packages_refused(api):
    client, main, users = api
    link = zipfile.ZipInfo('link')
    link.external_attr = (stat.S_IFLNK | 0o777) << 16
    bad = {
        'not a zip': b'nonsense',
        'unsafe path': archive(id='bad1', extra=[('../evil.py', 'x')]),
        'link': archive(id='bad2', extra=[(link, '/etc/passwd')]),
        'invalid manifest': archive(id='bad id with spaces'),
        'reserved version': archive(id='bad3', version='latest'),
    }
    for what, data in bad.items():
        r = publish(client, data)
        assert r.status_code == 400, (what, r.text)
    assert not [r for r in client.get('/api/models').json() if r['ID'].startswith('bad')]


def test_too_large(api, monkeypatch):
    client, main, users = api
    monkeypatch.setattr(main, 'MAX_MODEL_PACKAGE_BYTES', 10)
    assert publish(client, archive(id='large')).status_code == 413


def test_execution_records_model_packages(api):
    'The execution script stores ModelPackages through set_param (a dict of dicts); cleared when the execution runs again.'
    client, main, users = api
    weid = str(main.db.WorkflowExecutions.insert_one({'Status': 'Running', 'Attempts': 0, 'WorkflowID': 'wf', 'WorkflowVersion': 1,
                                                      'CreatedDate': '2026-01-01T10:00:00', 'RequestedBy': 'u', 'Inputs': '', 'Outputs': ''}).inserted_id)
    pkg = {'ID': 'oofem-thermal', 'Version': '1.2', 'Digest': 'sha256:' + 'a' * 64}
    r = client.patch(f'/api/executions/{weid}/set_param', json={'key': 'ModelPackages', 'value': {'m': pkg}})
    assert r.status_code == 200, r.text
    assert main.db.WorkflowExecutions.find_one({'_id': main.bson.objectid.ObjectId(weid)})['ModelPackages'] == {'m': pkg}
    assert client.get(f'/api/executions/{weid}').json()['entity']['ModelPackages'] == {'m': pkg}
    # model servers are still a dict of strings
    assert client.patch(f'/api/executions/{weid}/set_param', json={'key': 'ModelServers', 'value': {'m': 'Exec.node1'}}).status_code == 200
