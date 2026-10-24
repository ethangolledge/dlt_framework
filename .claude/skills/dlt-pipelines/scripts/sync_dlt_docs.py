"""Vendor the dlt documentation that matches the pinned dlt version into this skill.

The output is plain Markdown that an agent can grep offline: MDX imports and tab components are
removed, and ``<!--@@@DLT_SNIPPET path::name-->`` placeholders are replaced by the referenced code.

Usage:
    uv run python .claude/skills/dlt-pipelines/scripts/sync_dlt_docs.py --version 1.29.1
"""

import argparse
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

REPOSITORY = "https://github.com/dlt-hub/dlt.git"
DOCS_ROOT = Path("docs/website/docs")
OUTPUT = Path(__file__).resolve().parents[1] / "dlt-docs"
# Apache-2.0 section 4(b): modified files must say they were changed.
MODIFIED = "MDX imports and components removed; DLT_SNIPPET placeholders replaced by their code"

# Paths relative to DOCS_ROOT. Directories are copied recursively (Markdown only).
INCLUDED = (
    "general-usage/credentials",
    "general-usage/customising-pipelines",
    "general-usage/dataset-access/dataset.md",
    "general-usage/incremental",
    "general-usage/data-quality-lifecycle.md",
    "general-usage/destination-tables.md",
    "general-usage/destination.md",
    "general-usage/full-loading.md",
    "general-usage/glossary.md",
    "general-usage/incremental-loading.md",
    "general-usage/merge-loading.md",
    "general-usage/naming-convention.md",
    "general-usage/pipeline.md",
    "general-usage/resource.md",
    "general-usage/schema-contracts.md",
    "general-usage/schema-evolution.md",
    "general-usage/schema.md",
    "general-usage/source.md",
    "general-usage/state.md",
    "reference/command-line-interface.md",
    "reference/explainers/how-dlt-works.md",
    "reference/frequently-asked-questions.md",
    "reference/performance.md",
    "running-in-production/running.md",
    "walkthroughs/add-incremental-configuration.md",
    "walkthroughs/add_credentials.md",
    "walkthroughs/adjust-a-schema.md",
    "walkthroughs/dispatch-to-multiple-tables.md",
    "walkthroughs/run-a-pipeline.md",
    "tutorial/load-data-from-an-api.md",
    "tutorial/rest-api.md",
    "tutorial/sql-database.md",
    "dlt-ecosystem/staging.md",
    "dlt-ecosystem/destinations/duckdb.md",
    "dlt-ecosystem/destinations/postgres.md",
    "dlt-ecosystem/verified-sources/rest_api",
    "dlt-ecosystem/verified-sources/sql_database",
    "dlt-ecosystem/verified-sources/filesystem",
    "dlt-ecosystem/verified-sources/mongodb.md",
    "dlt-ecosystem/verified-sources/arrow-pandas.md",
)

_SNIPPET = re.compile(r"<!--@@@DLT_SNIPPET (?P<path>[^:]+)::(?P<name>[\w-]+)-->")
_MDX_IMPORT = re.compile(r"^import .+ from ['\"].+['\"];?\n", re.MULTILINE)
_MDX_COMPONENT = re.compile(
    r"^\s*</?(?:Tabs|TabItem|Header|Link|DocCardList|Admonition)\b[^>]*/?>\n?", re.MULTILINE
)
_MULTILINE_TABS = re.compile(r"^<Tabs\n(?:.*\n)*?\s*>\n", re.MULTILINE)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--version", required=True, help="dlt git tag, for example 1.29.1")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as checkout:
        commit = _checkout_docs(args.version, Path(checkout))
        docs = Path(checkout) / DOCS_ROOT
        if OUTPUT.exists():
            shutil.rmtree(OUTPUT)
        for relative in INCLUDED:
            source = docs / relative
            if not source.exists():
                raise SystemExit(f"{relative} does not exist in dlt {args.version}")
            for page in sorted(source.rglob("*.md")) if source.is_dir() else [source]:
                _write_page(page, docs)
        shutil.copyfile(Path(checkout) / "LICENSE.txt", OUTPUT / "LICENSE.txt")
    (OUTPUT / "VERSION").write_text(
        f"dlt {args.version}\ncommit {commit}\nsource {REPOSITORY}\n"
        f"licence Apache-2.0 (LICENSE.txt)\nmodified {MODIFIED}\n"
    )
    print(f"Vendored dlt {args.version} documentation into {OUTPUT}")


def _checkout_docs(version: str, checkout: Path) -> str:
    _git(
        "clone",
        "--quiet",
        "--depth",
        "1",
        "--branch",
        version,
        "--filter=blob:none",
        "--sparse",
        REPOSITORY,
        str(checkout),
    )
    _git("-C", str(checkout), "sparse-checkout", "set", str(DOCS_ROOT))
    return _git("-C", str(checkout), "rev-parse", "HEAD").strip()


def _write_page(page: Path, docs: Path) -> None:
    text = page.read_text(encoding="utf-8")
    text = _SNIPPET.sub(lambda match: _snippet(page.parent / match["path"], match["name"]), text)
    text = _MULTILINE_TABS.sub("", text)
    text = _MDX_COMPONENT.sub("", _MDX_IMPORT.sub("", text))
    target = OUTPUT / page.relative_to(docs)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def _snippet(source: Path, name: str) -> str:
    lines = source.read_text(encoding="utf-8").splitlines()
    start = _marker_index(lines, f"@@@DLT_SNIPPET_START {name}", source)
    end = _marker_index(lines, f"@@@DLT_SNIPPET_END {name}", source)
    body = lines[start + 1 : end]
    indent = min((len(line) - len(line.lstrip()) for line in body if line.strip()), default=0)
    code = "\n".join(line[indent:] for line in body)
    return f"```py\n{code}\n```"


def _marker_index(lines: list[str], marker: str, source: Path) -> int:
    for index, line in enumerate(lines):
        if line.strip().endswith(marker):
            return index
    raise SystemExit(f"Snippet marker {marker!r} not found in {source}")


def _git(*arguments: str) -> str:
    return subprocess.run(("git", *arguments), check=True, capture_output=True, text=True).stdout


if __name__ == "__main__":
    main()
