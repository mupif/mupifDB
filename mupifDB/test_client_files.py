"""Unit tests of file-download helpers of the REST client (:mod:`mupifDB.api.client_mupif`)."""

import pytest


@pytest.mark.parametrize('header,name', [
    # what the REST API sends (StreamingResponse in api/main.py)
    ("attachment; filename=\"files.zip\"; filename*=utf-8''files.zip", 'files.zip'),
    ("attachment; filename=\"workflow.py\"; filename*=utf-8''work%20flow.py", 'work flow.py'),
    ('attachment; filename="quoted name.zip"', 'quoted name.zip'),
    ('attachment; filename=file.vtu', 'file.vtu'),
    ('attachment', ''),
])
def test_file_name_from_content_disposition(header, name):
    """
    The file name is taken from the header, not the rest of the header line.

    The client used to take everything after ``filename=``, i.e. ``"files.zip"; filename*=utf-8''files.zip``
    for every download from the REST API.
    """
    from mupifDB.api.client_mupif import fileNameFromContentDisposition
    assert fileNameFromContentDisposition(header) == name
