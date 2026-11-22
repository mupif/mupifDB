"""
Unit tests of the REST client's handling of rejected access tokens (:mod:`mupifDB.api.client_util`).

No REST API is needed: ``requests`` is replaced by a fake that accepts only the token ``new``.
"""
import types

import pytest


class _FakeResponse:
    """Minimal stand-in for :class:`requests.Response`, enough for ``client_util._check``."""

    def __init__(self, status, method, url):
        self.status_code = status
        self.reason = 'OK' if status == 200 else 'Unauthorized'
        self.text = '{}'
        self.request = types.SimpleNamespace(method=method.upper(), url=url)

    def json(self):
        return {}


@pytest.fixture
def fakeApi(monkeypatch):
    """
    Return the list of ``Authorization`` headers sent; the fake API accepts only ``Bearer new``.

    Logging in again (through ``client_mupif._reauthenticate``) yields the token ``new``.
    """
    from mupifDB.api import client_util, client_mupif
    sent = []

    def fakeRequest(method):
        def request(url, headers=None, **kw):
            auth = (headers or {}).get('Authorization')
            sent.append(auth)
            return _FakeResponse(200 if auth == 'Bearer new' else 401, method, url)
        return request

    for method in ('get', 'post', 'patch', 'put', 'delete'):
        monkeypatch.setattr(client_util.requests, method, fakeRequest(method))
    monkeypatch.setattr(client_mupif, 'getAuthToken', lambda: {'access_token': 'new'})
    return sent


def test_rejected_token_is_renewed_once(fakeApi):
    """
    A request whose token is rejected (401) is repeated once with a token from a new login.

    Regression for finding #16: the client kept a rejected token until its expiry time (up to an
    hour), so e.g. after the REST API restarted with a new JWT key every request failed with 401.
    """
    from mupifDB.api import client_util
    client_util.rPatchRaw('api/executions/E1/set_param', headers={'Authorization': 'Bearer old'}, json={})
    assert fakeApi == ['Bearer old', 'Bearer new']


def test_request_without_token_is_not_repeated(fakeApi):
    """A request sent without a token is not repeated: its 401 is reported as an error."""
    from mupifDB.api import client_util
    with pytest.raises(RuntimeError, match='401'):
        client_util.rGetRaw('api/executions', headers={})
    assert fakeApi == [None]


def test_upload_is_rewound_before_repeating(fakeApi, tmp_path):
    """A file upload repeated after a new login sends the whole file again, not an exhausted stream."""
    from mupifDB.api import client_util
    path = tmp_path / 'log.txt'
    path.write_text('workflow log')
    with open(path, 'rb') as f:
        f.read()                            # as if consumed by the first, rejected request
        client_util.rPostRaw('api/file/', headers={'Authorization': 'Bearer old'}, files={'file': f})
        assert f.tell() == 0
