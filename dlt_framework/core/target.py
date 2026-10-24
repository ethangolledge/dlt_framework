"""Where a pipeline loads to."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dlt.common.destination import Destination

from dlt_framework.core.errors import ConfigurationError, require_identifier

# The driver each supported destination's extra installs: import name -> extra.
_DRIVERS: dict[str, dict[str, str]] = {
    "duckdb": {"duckdb": "duckdb"},
    "postgres": {"psycopg2": "postgres"},
}


@dataclass(frozen=True, slots=True)
class Target:
    """A destination type and the dataset (database schema) inside it.

    Credentials are resolved by dlt from the environment or ``.dlt/secrets.toml``, for example
    ``DESTINATION__POSTGRES__CREDENTIALS``. They never live in pipeline definitions.
    """

    destination: str
    dataset: str

    def __post_init__(self) -> None:
        require_identifier(self.dataset, "Dataset")

    def requires(self) -> dict[str, str]:
        """Modules loading here needs beyond the core, mapped to the extra that installs them."""
        return _DRIVERS.get(self.destination, {})

    def factory(self, staging_layout: str) -> Destination[Any, Any]:
        """The dlt destination, with a staging dataset private to the calling run type."""
        self._check_duckdb_catalog()
        destination = Destination.from_reference(
            self.destination, staging_dataset_name_layout=staging_layout
        )
        assert destination is not None, "from_reference returns None only for a None reference"
        return destination

    def _check_duckdb_catalog(self) -> None:
        """DuckDB cannot tell a catalog from a schema of the same name; dlt fails obscurely."""
        if self.destination != "duckdb":
            return
        database = os.environ.get("DESTINATION__DUCKDB__CREDENTIALS", "").removeprefix("duckdb:///")
        if Path(database).stem.casefold() == self.dataset.casefold():
            raise ConfigurationError(
                f"DuckDB file {database!r} and dataset {self.dataset!r} share a name; rename one"
            )
