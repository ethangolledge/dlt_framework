"""Where a pipeline loads to: one class per supported destination."""

import atexit
import threading
from abc import ABC, abstractmethod
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

import dlt
from dlt.common.destination import Destination

from dlt_framework.core.config import resolve
from dlt_framework.core.errors import ConfigurationError, require_identifier


@dataclass(frozen=True, kw_only=True)
class Target(ABC):
    """A destination and the dataset (database schema) inside it.

    Credentials are resolved by dlt from the environment or ``.dlt/secrets.toml``, for example
    ``DESTINATION__POSTGRES__CREDENTIALS``. They never live in pipeline definitions. A new
    destination is one subclass naming its dlt ``destination`` and the driver it ``requires``.
    """

    destination: ClassVar[str]
    """The dlt destination name, which also keys its credentials (``destination.<name>``)."""

    dataset: str

    def __post_init__(self) -> None:
        require_identifier(self.dataset, "Dataset")

    @abstractmethod
    def requires(self) -> dict[str, str]:
        """Modules loading here needs beyond the core, mapped to the extra that installs them."""

    def factory(self, staging_layout: str) -> Destination[Any, Any]:
        """The dlt destination, with a staging dataset private to the calling run type."""
        destination = Destination.from_reference(
            self.destination, self.credentials(), staging_dataset_name_layout=staging_layout
        )
        assert destination is not None, "from_reference returns None only for a None reference"
        return destination

    def credentials(self) -> Any:
        """What dlt connects with; ``None`` lets dlt resolve them from config."""
        return None

    def loading(self) -> AbstractContextManager[Any]:
        """Held while a run loads here."""
        return nullcontext()


@dataclass(frozen=True, kw_only=True)
class Postgres(Target):
    destination: ClassVar[str] = "postgres"

    def requires(self) -> dict[str, str]:
        return {"psycopg2": "postgres"}


@dataclass(frozen=True, kw_only=True)
class DuckDb(Target):
    """A DuckDB file, ``destination.duckdb.credentials``. Its name must differ from the dataset:
    DuckDB cannot tell a catalog from a schema of the same name, and dlt fails obscurely.

    DuckDB lets one process write a file. Pipelines running in threads of that process share one
    connection to it, and dlt gives each thread its own cursor on it: connections opened by each
    pipeline at the same moment race, and DuckDB refuses some of them. They also load to it one
    at a time, while still extracting and normalizing at once: DuckDB refuses schema changes made
    at the same moment (a catalog write-write conflict), as when two pipelines first load one
    dataset.
    """

    destination: ClassVar[str] = "duckdb"

    def requires(self) -> dict[str, str]:
        return {"duckdb": "duckdb"}

    def credentials(self) -> Any:
        return self._file().connection

    def loading(self) -> AbstractContextManager[Any]:
        return self._file().loading

    def _file(self) -> "_DuckDbFile":
        """This process's handle on the configured file, opened on first use.

        DuckDB's own settings (``memory_limit``, ``threads``) are dlt's
        ``destination.duckdb.credentials.global_config``, which dlt applies only to connections
        it opens itself. They are set here as dlt sets them, ``SET GLOBAL`` once the file is open:
        every connection to the file in this process shares them, and DuckDB refuses a second
        connection opened with a different connect-time config. Without a ``memory_limit`` DuckDB
        takes up to 80% of the machine's memory.
        """
        import duckdb  # the duckdb extra
        from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials

        configured: Any = dlt.secrets.get("destination.duckdb.credentials")
        if not configured:
            raise ConfigurationError("Set destination.duckdb.credentials to the DuckDB file")
        settings = resolve(DuckDbCredentials(), "destination", "duckdb", "credentials")
        path = Path(str(configured).removeprefix("duckdb:///")).resolve()
        if path.stem.casefold() == self.dataset.casefold():
            raise ConfigurationError(
                f"DuckDB file {str(path)!r} and dataset {self.dataset!r} share a name; rename one"
            )
        with _DUCKDB_LOCK:
            if path not in _DUCKDB:
                connection = duckdb.connect(str(path))
                for key, value in (settings.global_config or {}).items():
                    connection.execute(f"SET GLOBAL {key} = $value", {"value": value})
                atexit.register(connection.close)
                _DUCKDB[path] = _DuckDbFile(connection, threading.Lock())
            return _DUCKDB[path]


@dataclass(frozen=True, slots=True)
class _DuckDbFile:
    connection: Any
    loading: threading.Lock


_DUCKDB: dict[Path, _DuckDbFile] = {}  # this process's open DuckDB files
_DUCKDB_LOCK = threading.Lock()
