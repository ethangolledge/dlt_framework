"""What happens to a document's file once its rows are committed at the destination."""

from abc import ABC, abstractmethod
from dataclasses import dataclass

from dlt.common.storages.fsspec_filesystem import FileItemDict

from dlt_framework.core import buckets


class Disposal(ABC):
    @abstractmethod
    def __call__(self, item: FileItemDict) -> None:
        """Dispose of a file whose rows are committed."""

    def requires(self) -> dict[str, str]:
        """Modules this needs beyond the core, mapped to the extra that installs them."""
        return {}


@dataclass(frozen=True, slots=True)
class Keep(Disposal):
    """Leave the file where it is."""

    def __call__(self, item: FileItemDict) -> None:
        pass


@dataclass(frozen=True, slots=True)
class Delete(Disposal):
    """Remove the file: nothing is stored once its rows have landed."""

    def __call__(self, item: FileItemDict) -> None:
        item.fsspec.rm(item["file_url"])


@dataclass(frozen=True, slots=True)
class Archive(Disposal):
    """Move the file under ``bucket_url``, keeping its path relative to where it was read."""

    bucket_url: str

    def __call__(self, item: FileItemDict) -> None:
        buckets.write(f"{self.bucket_url.rstrip('/')}/{item['relative_path']}", item.read_bytes())
        item.fsspec.rm(item["file_url"])

    def requires(self) -> dict[str, str]:
        return buckets.requires(self.bucket_url)
