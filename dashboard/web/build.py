"""Write the browser app to a folder: the page, its styles and script, and the data.

The data is written as scripts (``data.js``, and ``runs/<id>.js`` for each run in full), not
JSON fetched by the page: a page opened straight from disk may not fetch files, but it may load
scripts. So the folder works opened from disk, or served by anything that serves files. Nothing
is loaded from elsewhere: no fonts, no libraries.

The folder holds monitoring data: keep it out of version control.
"""

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dashboard.theme import COLOURS
from dashboard.web import snapshot
from dlt_framework.core.monitoring import Frames
from dlt_framework.core.monitoring.health import Signal

STATIC = Path(__file__).with_name("static")


def build(
    folder: Path, frames: Frames, now: datetime | None = None, signals: list[Signal] | None = None
) -> int:
    """Write the app into ``folder``; returns how many runs it holds in full."""
    now = now or datetime.now(UTC)
    folder.mkdir(parents=True, exist_ok=True)
    for name in ("index.html", "app.js", "app.css"):
        shutil.copyfile(STATIC / name, folder / name)
    (folder / "theme.css").write_text(_theme(), encoding="utf-8")
    data = snapshot.summary(frames, now, signals)
    (folder / "data.js").write_text(f"window.DASH_DATA = {_json(data)};\n", encoding="utf-8")
    runs = folder / "runs"
    if runs.exists():
        shutil.rmtree(runs)
    runs.mkdir()
    for run in data["runs"]:
        detail = _json(snapshot.detail(frames, run["id"]))
        (runs / f"{safe(run['id'])}.js").write_text(f"window.DASH_RUN({detail});\n", "utf-8")
    return len(data["runs"])


def safe(run_id: str) -> str:
    """A run id as a file name: run ids are hex, but they come from data, so nothing else."""
    return "".join(char if char.isalnum() or char in "-_" else "_" for char in run_id)


def _theme() -> str:
    """The theme's colours as CSS variables, so the page and the terminal share one palette."""
    names = "\n".join(f"  --{name}: {value};" for name, value in COLOURS.items())
    return f":root {{\n{names}\n}}\n"


def _json(value: Any) -> str:
    # "</" could end the script tag the data is loaded by; escaped, it reads the same
    return json.dumps(value, default=str, separators=(",", ":")).replace("</", "<\\/")
