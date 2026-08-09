from pathlib import Path, PurePosixPath

import dlt
import pytest

from pipeline.core.discovery import discover_source, instantiate_source
from pipeline.core.errors import SourceDefinitionError
from pipeline.core.models import SourceDefinition


FIXTURE_ROOT = Path(__file__).parents[1] / "fixtures" / "sources"
FIXTURE_PACKAGE = "fixtures.sources"


def test_discovers_source_factory_by_filename() -> None:
    definition = discover_source(
        "alpha",
        source_root=FIXTURE_ROOT,
        source_package=FIXTURE_PACKAGE,
    )

    assert definition.relative_path == PurePosixPath("rest/alpha.py")
    assert definition.module_name == "fixtures.sources.rest.alpha"
    assert callable(definition.factory)


def test_instantiates_dlt_source() -> None:
    definition = discover_source(
        "alpha.py",
        source_root=FIXTURE_ROOT,
        source_package=FIXTURE_PACKAGE,
    )

    source = instantiate_source(definition)

    assert source.name == "alpha"


@pytest.mark.parametrize(
    "selector",
    [
        "",
        " rest/alpha.py",
        "/rest/alpha.py",
        "../alpha.py",
        "rest\\alpha.py",
        "rest/alpha.json",
        "client-source",
    ],
)
def test_rejects_invalid_selectors(selector: str) -> None:
    with pytest.raises(SourceDefinitionError):
        discover_source(
            selector,
            source_root=FIXTURE_ROOT,
            source_package=FIXTURE_PACKAGE,
        )


def test_rejects_missing_source() -> None:
    with pytest.raises(SourceDefinitionError, match="No source named missing"):
        discover_source(
            "missing",
            source_root=FIXTURE_ROOT,
            source_package=FIXTURE_PACKAGE,
        )


def test_requires_qualified_name_when_source_name_is_ambiguous() -> None:
    with pytest.raises(SourceDefinitionError, match="rest/shared.py, shopify/shared.py"):
        discover_source(
            "shared",
            source_root=FIXTURE_ROOT,
            source_package=FIXTURE_PACKAGE,
        )


def test_rejects_non_dlt_source() -> None:
    definition = SourceDefinition(
        PurePosixPath("rest/not_dlt.py"),
        "not_dlt",
        lambda: object(),
    )

    with pytest.raises(SourceDefinitionError, match="must return"):
        instantiate_source(definition)


def test_accepts_optional_factory_arguments() -> None:
    @dlt.source(name="optional_argument")
    def factory(value: str = "default"):
        return dlt.resource([{"value": value}], name="rows")

    definition = SourceDefinition(PurePosixPath("rest/optional.py"), "optional", factory)

    source = instantiate_source(definition)

    assert source.name == "optional"
    assert source.schema.name == "optional"


def test_source_arguments_use_filename_environment_namespace(monkeypatch) -> None:
    monkeypatch.setenv("SOURCES__CONFIGURED__API_KEY", "secret")
    definition = discover_source(
        "configured",
        source_root=FIXTURE_ROOT,
        source_package=FIXTURE_PACKAGE,
    )

    source = instantiate_source(definition)

    assert list(source.resources["credentials"]) == [{"api_key": "secret"}]
