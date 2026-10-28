"""The browser dashboard: its data, the folder it is built into, and the page itself, opened in
a headless browser where one is installed."""

import json
import shutil
import subprocess
from pathlib import Path

import polars as pl
import pytest
import scenarios
from scenarios import NOW

from dashboard.web import build, snapshot
from dlt_framework.core.monitoring import Frames
from dlt_framework.core.monitoring.records import RECORDS

STATIC = Path(build.__file__).with_name("static")


def combined() -> Frames:
    parts = [scenarios.hand_runs(), scenarios.scheduled(), scenarios.backfilled()]
    return Frames(
        **{
            record.name: pl.concat([getattr(p, record.name) for p in parts], how="diagonal_relaxed")
            for record in RECORDS
        }
    )


@pytest.fixture(scope="module")
def data() -> dict:
    return snapshot.summary(combined(), NOW)


# ---- the data --------------------------------------------------------------------------------


def test_the_snapshot_is_plain_json(data) -> None:
    assert json.loads(json.dumps(data)) == data


def test_each_pipeline_says_what_it_is_and_how_it_is_doing(data) -> None:
    pipelines = {p["id"]: p for p in data["pipelines"]}
    assert set(pipelines) == {"form4", "insider", "members", "filings", "orders", "shop"}
    orders = pipelines["orders"]
    assert (orders["state"], orders["source_type"], orders["destination"]) == (
        "warning",
        "rest",
        "duckdb",
    )
    (resource,) = orders["resources"]
    assert (resource["name"], resource["write"], resource["cursor"]) == (
        "orders",
        "Merge",
        "updated_at",
    )
    assert resource["rows"] == 71 * 5_000 and len(resource["cursor_path"]) == 71
    assert orders["tables"] == [
        {"table": "orders", "rows": 355_000, "last": "2026-10-28T11:00:00Z"}
    ]
    assert pipelines["form4"]["state"] == "error" and pipelines["form4"]["headline"].startswith(
        "failing"
    )


def test_runs_and_incidents_cover_all_history_with_their_causes(data) -> None:
    assert len(data["runs"]) == 21 + 72 + 5
    run = next(r for r in data["runs"] if r["id"] == "form4-signal")
    assert (run["outcome"], run["cause"], run["rerun"]) == (
        "failed",
        "stopped (SIGTERM)",
        "pipeline run form4",
    )
    form4 = next(i for i in data["incidents"] if i["pipeline"] == "form4")
    assert form4["open"] and form4["latest"] == "form4-signal" and len(form4["runs"]) == 4


def test_missing_ranges_and_jobs_carry_their_commands(data) -> None:
    kinds = {m["kind"] for m in data["missing"]}
    assert kinds == {"failed", "empty", "off", "twice", "hole"}
    assert all(m["command"] for m in data["missing"] if m["fixable"])
    (history,) = data["histories"]
    assert history["fraction"] == pytest.approx(0.6) and len(history["cells"]) == 120
    failed = next(j for j in data["jobs"] if j["run"] == "job-failed")
    assert failed["resume"].startswith("pipeline backfill shop --resource orders --from")


def test_checks_say_what_they_found_and_what_they_could_not_check(data) -> None:
    assert {f["check"] for f in data["findings"]} >= {"memory.tight", "release.slower"}
    assert data["unchecked"] == [
        {"title": "a resource that holds the others up", "needs": ["extract times per load"]}
    ]


def test_a_run_in_full_holds_its_error_steps_memory_and_loads() -> None:
    frames = combined()
    failed = snapshot.detail(frames, "form4-signal")
    assert failed["error"]["cause_type"] == "SignalReceivedException"
    full = snapshot.detail(frames, "orders-070")
    assert [s["step"] for s in full["steps"]] == ["extract", "load"]
    assert 1 < len(full["samples"]) <= snapshot.SAMPLES
    (load,) = full["loads"]
    assert load["cursor"] == ["2026-10-28T09:00:00+00:00", "2026-10-28T10:00:00+00:00"]


def test_memory_samples_are_thinned_evenly_keeping_the_first_and_last() -> None:
    kept = snapshot._evenly(1000, 240)
    assert len(kept) == 240 and {0, 999} <= kept


# ---- the folder ------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def site(tmp_path_factory) -> Path:
    folder = tmp_path_factory.mktemp("site")
    build.build(folder, combined(), NOW)
    return folder


def test_the_folder_holds_the_page_its_data_and_each_run(site) -> None:
    assert {path.name for path in site.iterdir()} == {
        "index.html",
        "app.js",
        "app.css",
        "theme.css",
        "data.js",
        "runs",
    }
    text = (site / "data.js").read_text()
    assert text.startswith("window.DASH_DATA = ") and "</" not in text.replace("<\\/", "")
    assert len(list((site / "runs").iterdir())) == 98
    assert "--ok: #39ff14;" in (site / "theme.css").read_text()


def test_run_ids_become_safe_file_names() -> None:
    assert build.safe("../../etc/passwd") == "______etc_passwd"


def test_the_page_loads_nothing_from_elsewhere() -> None:
    for name in ("index.html", "app.js", "app.css"):
        text = (STATIC / name).read_text()
        assert (
            "http://" not in text.replace("http://www.w3.org/2000/svg", "")
            and "https://" not in text
        ), name


@pytest.mark.skipif(not shutil.which("node"), reason="node is not installed")
def test_the_script_parses() -> None:
    subprocess.run(["node", "--check", str(STATIC / "app.js")], check=True)


# ---- the page, in a headless browser ----------------------------------------------------------

BROWSER = (
    shutil.which("chromium") or shutil.which("chromium-browser") or shutil.which("google-chrome")
)


def page(site: Path, route: str) -> str:
    """The page's DOM once its script has run, at ``route``."""
    found = subprocess.run(
        [
            BROWSER,
            "--headless=new",
            "--disable-gpu",
            "--no-sandbox",
            "--virtual-time-budget=3000",
            "--dump-dom",
            f"file://{site}/index.html#{route}",
        ],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    return found.stdout


@pytest.mark.skipif(BROWSER is None, reason="no headless browser installed")
@pytest.mark.parametrize(
    ("route", "shown"),
    [
        (
            "/overview",
            ["1 FAILING", "2 AT RISK", "Health by day", "Open incidents", "pipeline run form4"],
        ),
        ("/pipelines", ["6 of 6", "rest → duckdb", "no cursor: all of it each run"]),
        (
            "/pipeline/orders",
            ["What it reads and writes", "updated_at", "Cursor position", "shop.orders"],
        ),
        (
            "/runs?pipeline=form4&from=2026-10-25&to=2026-10-25",
            ["4 run(s) in range", "stopped by SIGTERM"],
        ),
        ("/run/form4-signal", ["FAILED", "signal 15, SIGTERM", "pipeline run form4"]),
        ("/run/orders-070", ["Timeline", "What it loaded", "Destination tables"]),
        ("/backfills", ["4 range(s) missing", "FAILED", "HOLE", "pipeline backfill shop"]),
        ("/incidents", ["2 open", "Resolved", "Causes by day"]),
        ("/tuning", ["Findings", "memory at 90%", "Not checked"]),
    ],
)
def test_every_page_renders_what_it_must(site, route, shown) -> None:
    dom = page(site, route)
    assert "loading…" not in dom.split("<main", 1)[1][:200], f"{route} did not render"
    missing = [text for text in shown if text not in dom]
    assert missing == [], f"{route} lacks {missing}"
