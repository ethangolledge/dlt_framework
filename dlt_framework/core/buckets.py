"""Folders and buckets by URL (``file://``, ``s3://``, ``gs://``, ``az://``), opened the way dlt's
filesystem source opens them, so one credentials setting serves every source type."""

import posixpath
from typing import Any
from urllib.parse import urlparse

from dlt.common.storages import FilesystemConfiguration, fsspec_from_config

from dlt_framework.core.config import resolve

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
