"""Tests of :func:`mupifDB.scheduler.staging.stageWorkflow` with an in-memory package store."""

import io
import types
import zipfile

import pytest

from mupifDB.scheduler.staging import StagingError, stageWorkflow

_RECORD = types.SimpleNamespace(GridFSID='F1', modulename='mywf')


def _zip(members: dict[str, bytes]) -> bytes:
    """Build a zip archive with the given members."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return buf.getvalue()


def _fetch(content: bytes, name: str):
    """Fake download returning *content* under the file name *name*."""
    return lambda fileId: (content, name)


def test_python_module(tmp_path):
    """A single module is written under its own name."""
    path = stageWorkflow(_RECORD, tmp_path, _fetch(b'x=1\n', 'mywf.py'))
    assert path == tmp_path / 'mywf.py' and path.read_bytes() == b'x=1\n'


def test_zip_archive(tmp_path):
    """An archive is extracted; the module must be at its top level."""
    data = _zip({'mywf.py': b'x=1\n', 'data/input.txt': b'42'})
    path = stageWorkflow(_RECORD, tmp_path, _fetch(data, 'mywf.zip'))
    assert path.read_bytes() == b'x=1\n' and (tmp_path / 'data' / 'input.txt').read_bytes() == b'42'


@pytest.mark.parametrize('content,name', [
    (b'x=1\n', 'other.py'),                                  # module name does not match the record
    (_zip({'sub/mywf.py': b''}), 'mywf.zip'),                # module not at the top level
    (b'binary', 'mywf.tar.gz'),                              # unsupported package type
])
def test_unusable_package_is_rejected(tmp_path, content, name):
    """
    A package that cannot work fails at staging with a clear message.

    It used to be only logged ("Filename check FAILED") and the workflow then failed when the
    execution script could not import it.
    """
    with pytest.raises(StagingError):
        stageWorkflow(_RECORD, tmp_path, _fetch(content, name))


@pytest.mark.parametrize('member', ['../escape.py', '/etc/escape.py', 'a/../../escape.py', 'C:/escape.py'])
def test_unsafe_archive_paths_are_rejected(tmp_path, member):
    """Archive members pointing outside the working directory are rejected before anything is extracted."""
    data = _zip({'mywf.py': b'', member: b'evil'})
    (tmp_path / 'work').mkdir()
    with pytest.raises(StagingError):
        stageWorkflow(_RECORD, tmp_path / 'work', _fetch(data, 'mywf.zip'))
    assert not (tmp_path / 'escape.py').exists()
    assert not (tmp_path / 'work' / 'mywf.py').exists()


def test_server_file_name_cannot_escape(tmp_path):
    """A file name with directory parts (from the server) is reduced to its base name."""
    (tmp_path / 'work').mkdir()
    path = stageWorkflow(_RECORD, tmp_path / 'work', _fetch(b'x=1\n', '../../mywf.py'))
    assert path == tmp_path / 'work' / 'mywf.py' and path.exists()
