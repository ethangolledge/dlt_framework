"""Folders and buckets by URL (``file://``, ``s3://``, ``gs://``, ``az://``), opened the way dlt's
filesystem source opens them, so one credentials setting serves every source type."""

import os
import posixpath
from typing import Any
from urllib.parse import urlparse

import dlt
from dlt.common.storages import FilesystemConfiguration, fsspec_from_config

from dlt_framework.core.config import resolve
from dlt_framework.core.errors import ConfigurationError

# What dlt imports to open a bucket, mapped to the extra that installs it.
_PROTOCOL_MODULES = {
    "s3": {"s3fs": "filesystem"},
    "gs": {"gcsfs": "gcs"},
    "gcs": {"gcsfs": "gcs"},
    "az": {"adlfs": "azure"},
    "abfs": {"adlfs": "azure"},
}


def requires(url: str) -> dict[str, str]:
    """Modules opening ``url`` needs beyond the core, mapped to the extra that installs them."""
    return _PROTOCOL_MODULES.get(urlparse(url).scheme, {})


def folder(name: str) -> str:
    """The folder or bucket configured as ``folders.<name>``, as a URL.

    Pipelines that meet at a folder (one downloads into it, another reads it) name it here, so
    where it lives is stated once per environment, not in code. A local path is taken from the
    working directory, as dlt finds ``.dlt/``, and returned as an absolute ``file://`` URL.
    """
    configured: Any = dlt.config.get(f"folders.{name}")
    if not configured:
        raise ConfigurationError(f"Set folders.{name} to a folder or bucket URL")
    url = str(configured)
    if FilesystemConfiguration.is_local_path(url):
        return FilesystemConfiguration.make_file_url(os.path.abspath(url))
    return url.rstrip("/")


def open_bucket(url: str) -> tuple[Any, str]:
    """The fsspec filesystem and path for ``url``, with ``sources.filesystem.credentials``."""
    config = resolve(FilesystemConfiguration(bucket_url=url), "sources", "filesystem")
    return fsspec_from_config(config)


def exists(url: str) -> bool:
    fs, path = open_bucket(url)
    return bool(fs.exists(path))


def write(url: str, data: bytes) -> None:
    """Write ``data`` to the file at ``url``, replacing it, creating folders where needed."""
    fs, path = open_bucket(url)
    fs.makedirs(posixpath.dirname(path), exist_ok=True)
    fs.pipe_file(path, data)
