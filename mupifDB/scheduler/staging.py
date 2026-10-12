"""
Staging of a workflow package into the working directory of an execution.

A workflow is stored in the database as a single Python module (``<modulename>.py``) or as a zip
archive containing that module (and any other files it needs) at its top level. Used by the
scheduler for every execution and by ``workflow_execution_script.py --download_workflow`` when the
script is run by hand.
"""

import logging
import os
import shutil
import threading
import uuid
import zipfile
from pathlib import Path, PurePosixPath
from typing import Callable

log = logging.getLogger('scheduler')


class StagingError(Exception):
    """The workflow package cannot be used (wrong name, unsupported type, unsafe archive)."""


def _defaultFetch(fileId: str) -> tuple[bytes, str]:
    """Download a file from the database through the REST API: ``(content, file name)``."""
    from mupifDB import restApiControl
    return restApiControl.getBinaryFileByID(fileId)


def _checkArchiveMember(name: str) -> None:
    """
    Reject archive members that would be extracted outside the target directory.

    :param name: member name as stored in the zip archive
    :raises StagingError: for absolute paths and paths containing ``..``
    """
    path = PurePosixPath(name.replace('\\', '/'))
    if path.is_absolute() or '..' in path.parts or (path.parts and ':' in path.parts[0]):
        raise StagingError(f'Unsafe path in workflow archive: {name!r}')


class PackageCache:
    """
    Workflow packages downloaded once and kept on disk, least recently used ones pruned [S10].

    A package is identified by its file ID in the database: an uploaded file never changes (a new
    upload gets a new ID), so a cached copy never becomes stale. Layout: ``<directory>/<file ID>/<file
    name>``. Entries are filled under a temporary name and renamed into place, so a reader never
    sees a partial entry, also with several scheduler processes sharing the directory. Within the
    process, a lock per file ID makes concurrent job threads download a package only once.
    """

    def __init__(self, directory: str | os.PathLike, maxBytes: int):
        """
        Use (and create) the cache directory.

        :param directory: cache directory
        :param maxBytes: total size of the cached packages above which the least recently used are removed
        """
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.maxBytes = maxBytes
        self._lock = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}

    def get(self, fileId: str, fetch: Callable[[str], tuple[bytes, str]]) -> Path:
        """
        Return the path of the cached package *fileId*, downloading it first if needed.

        :param fileId: file ID of the package in the database
        :param fetch: ``file ID -> (content, file name)``
        :return: path of the package file in the cache
        """
        if not fileId or '/' in fileId or fileId.startswith('.'):
            raise StagingError(f'Invalid package file ID {fileId!r}')
        with self._lock:
            lock = self._locks.setdefault(fileId, threading.Lock())
        with lock:
            entry = self.directory / fileId
            if entry.is_dir() and (files := list(entry.iterdir())):
                os.utime(entry)                     # mark as recently used
                return files[0]
            content, fileName = fetch(fileId)
            fileName = os.path.basename(fileName.replace('\\', '/')) or 'package'
            tmp = self.directory / f'.{fileId}.{uuid.uuid4().hex}'
            tmp.mkdir()
            (tmp / fileName).write_bytes(content)
            try:
                os.rename(tmp, entry)
            except OSError:
                # filled meanwhile by another process: use that one
                shutil.rmtree(tmp, ignore_errors=True)
            log.info(f'Workflow package {fileId} ({fileName}, {len(content)} bytes) downloaded into the cache')
        self.prune(keep=fileId)
        return next((self.directory / fileId).iterdir())

    def prune(self, keep: str | None = None) -> None:
        """
        Remove the least recently used packages while the cache is larger than its limit.

        :param keep: file ID never removed (the one just used)
        """
        entries = []
        for entry in self.directory.iterdir():
            if entry.is_dir() and not entry.name.startswith('.'):
                size = sum(f.stat().st_size for f in entry.iterdir() if f.is_file())
                entries.append((entry.stat().st_mtime, size, entry))
        total = sum(size for _, size, _ in entries)
        for mtime, size, entry in sorted(entries, key=lambda e: e[0]):
            if total <= self.maxBytes:
                break
            if entry.name == keep:
                continue
            shutil.rmtree(entry, ignore_errors=True)
            total -= size
            log.info(f'Workflow package {entry.name} removed from the cache')


def stageWorkflow(workflowRecord, destDir: str | os.PathLike, fetch: Callable[[str], tuple[bytes, str]] = _defaultFetch,
                  cache: PackageCache | None = None) -> Path:
    """
    Download the package of a workflow into *destDir* and check that it contains the workflow module.

    :param workflowRecord: workflow record; uses ``GridFSID`` (package file) and ``modulename``
    :param destDir: existing directory to unpack into
    :param fetch: ``file ID -> (content, file name)``; default: download through the REST API
    :param cache: package cache to use (None: download every time)
    :return: path of the workflow module (``<destDir>/<modulename>.py``)
    :raises StagingError: if the package is not ``<modulename>.py`` or a zip archive with that module
                          at its top level, or if the archive contains unsafe paths
    """
    destDir = Path(destDir)
    moduleFile = workflowRecord.modulename + '.py'
    if cache is not None:
        cached = cache.get(workflowRecord.GridFSID, fetch)
        content, fileName = cached.read_bytes(), cached.name
    else:
        content, fileName = fetch(workflowRecord.GridFSID)
    # the name comes from the server (Content-Disposition): never let it point elsewhere
    fileName = os.path.basename(fileName.replace('\\', '/'))
    if fileName.endswith('.py'):
        if fileName != moduleFile:
            raise StagingError(f'Workflow file is {fileName!r}, expected {moduleFile!r}')
        (destDir / fileName).write_bytes(content)
    elif fileName.endswith('.zip'):
        archive = destDir / fileName
        archive.write_bytes(content)
        with zipfile.ZipFile(archive) as zf:
            names = zf.namelist()
            for name in names:
                _checkArchiveMember(name)
            if moduleFile not in names:
                raise StagingError(f'Workflow archive {fileName!r} does not contain {moduleFile!r} at its top level (contains: {names})')
            zf.extractall(path=destDir)
        log.info(f'Workflow archive {fileName} extracted: {names}')
    else:
        raise StagingError(f'Unsupported workflow file {fileName!r} (expected .py or .zip)')
    return destDir / moduleFile
