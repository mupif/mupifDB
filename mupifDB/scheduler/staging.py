"""
Staging of a workflow package into the working directory of an execution.

A workflow is stored in the database as a single Python module (``<modulename>.py``) or as a zip
archive containing that module (and any other files it needs) at its top level. Used by the
scheduler for every execution and by ``workflow_execution_script.py --download_workflow`` when the
script is run by hand.
"""

import logging
import os
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


def stageWorkflow(workflowRecord, destDir: str | os.PathLike, fetch: Callable[[str], tuple[bytes, str]] = _defaultFetch) -> Path:
    """
    Download the package of a workflow into *destDir* and check that it contains the workflow module.

    :param workflowRecord: workflow record; uses ``GridFSID`` (package file) and ``modulename``
    :param destDir: existing directory to unpack into
    :param fetch: ``file ID -> (content, file name)``; default: download through the REST API
    :return: path of the workflow module (``<destDir>/<modulename>.py``)
    :raises StagingError: if the package is not ``<modulename>.py`` or a zip archive with that module
                          at its top level, or if the archive contains unsafe paths
    """
    destDir = Path(destDir)
    moduleFile = workflowRecord.modulename + '.py'
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
