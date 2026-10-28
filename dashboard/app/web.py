"""The web app, on Streamlit: ``pipeline-dashboard serve``, or ``streamlit run`` on this file.

Only the controls are Streamlit's; every page is the same styled text the terminal shows,
encoded as HTML. Choices live in the URL, so a view can be shared or bookmarked.
"""

import argparse
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode

import streamlit as st

from dashboard.app.session import RANGES, Choices, draw, load
from dashboard.model.facts import Scope, View
from dashboard.pages import PAGES
from dashboard.render.encode import FONTS, html, stylesheet
from dashboard.theme import COLOURS
from dlt_framework.core.monitoring import Frames

OUTCOMES = ("succeeded", "retried", "failed", "lost", "running")
REFRESH_S = 60

_CHROME = f"""
{FONTS}
<style>
.stApp, [data-testid="stSidebar"] {{ background: {COLOURS["background"]}; }}
[data-testid="stSidebar"] {{ border-right: 1px solid {COLOURS["dim"]}; }}
.stApp, .stApp * {{ font-family: 'JetBrains Mono', ui-monospace, monospace; }}
h1, h2, h3 {{ font-family: 'VT323', monospace !important; color: {COLOURS["ok"]};
              text-shadow: 0 0 8px {COLOURS["ok"]}88; letter-spacing: 2px; }}
label, .stMarkdown p, [data-testid="stCaptionContainer"] {{ color: {COLOURS["muted"]} !important; }}
[data-testid="stHeader"] {{ background: transparent; }}
.block-container {{ padding-top: 1rem; max-width: 100%; }}
{{SHEET}}
</style>
"""


def main() -> None:
    args = _arguments()
    st.set_page_config(page_title="dlt//ops", page_icon="▓", layout="wide")
    choices = _controls(_frames(args.uri, args.demo))
    st.markdown(
        _CHROME.replace("{SHEET}", stylesheet(scanlines=st.session_state.get("scanlines", True))),
        unsafe_allow_html=True,
    )
    live = st.session_state.get("live", True)
    if live:
        st.fragment(run_every=f"{REFRESH_S}s")(_page)(args, choices)
    else:
        _page(args, choices)


def _page(args: argparse.Namespace, choices: Choices) -> None:
    block = draw(_frames(args.uri, args.demo), choices)
    st.markdown(html(block, PAGES[choices.page].title, _href(choices)), unsafe_allow_html=True)


def _href(choices: Choices) -> Callable[[str], str]:
    """Links one level down, as this app's own URL with the same range."""

    def href(target: str) -> str:
        kind, _, key = target.partition("/")
        chosen = {"focus": key} if kind == "pipeline" else {"run": key}
        return "?" + urlencode({"page": kind, "since": choices.since, **chosen})

    return href


@st.cache_data(ttl=REFRESH_S, show_spinner="reading monitoring frames…")
def _frames(uri: str | None, demo: bool) -> Frames:
    return load(uri, demo)


def _controls(frames: Frames) -> Choices:
    """The sidebar, seeded from and written back to the URL."""
    query = st.query_params
    now = datetime.now(UTC)
    with st.sidebar:
        st.markdown("### ▓▒░ DLT//OPS")
        keys = list(PAGES)
        page = st.radio(
            "page",
            keys,
            index=_index(keys, query.get("page")),
            format_func=lambda key: PAGES[key].title,
        )
        since = st.select_slider(
            "range",
            RANGES,
            value=query.get("since", "24h") if query.get("since") in RANGES else "24h",
        )
        known = View.build(frames, Scope.last("all", now), now).pipelines
        pipelines = st.multiselect(
            "pipelines", known, default=[p for p in query.get_all("pipeline") if p in known]
        )
        modes = st.multiselect("modes", ["run", "backfill"], default=query.get_all("mode"))
        outcomes = st.multiselect(
            "outcomes", OUTCOMES, default=[o for o in query.get_all("outcome") if o in OUTCOMES]
        )
        run_id = focus = None
        if PAGES[page].picks == "run":
            run_id = _run_picker(
                frames, Scope.last(since, now, pipelines=tuple(pipelines)), now, query.get("run")
            )
        if PAGES[page].picks == "pipeline" and known:
            focus = str(st.selectbox("pipeline", known, index=_index(known, query.get("focus"))))
        size = st.slider("columns", 100, 240, int(query.get("columns", 160)), step=10)
        st.toggle("live (every minute)", value=True, key="live")
        st.toggle("scanlines", value=True, key="scanlines")
        if st.button("↻ read the frames again"):
            _frames.clear()
    st.query_params.from_dict(
        {
            "page": page,
            "since": since,
            "pipeline": pipelines,
            "mode": modes,
            "outcome": outcomes,
            "columns": str(size),
            **({"run": run_id} if run_id else {}),
            **({"focus": focus} if focus else {}),
        }
    )
    return Choices(
        page, since, tuple(pipelines), tuple(modes), tuple(outcomes), run_id, focus, size
    )


def _run_picker(frames: Frames, scope: Scope, now: datetime, chosen: str | None) -> str | None:
    """A run to show: failed and lost first, then newest, labelled readably."""
    runs = (
        View.build(frames, scope, now)
        .runs.sort(["rank", "started_at"], descending=[False, True])
        .head(200)
    )
    if runs.is_empty():
        return None
    options = runs["run_id"].to_list()
    labels = {row["run_id"]: _label(row) for row in runs.iter_rows(named=True)}
    return str(
        st.selectbox("run", options, index=_index(options, chosen), format_func=labels.__getitem__)
    )


def _label(run: dict[str, Any]) -> str:
    when = f"{run['started_at']:%m-%d %H:%M}"
    return f"{when} {run['pipeline_id']} {run['outcome']} {run['run_id'][:8]}"


def _index(options: list[str], chosen: str | None) -> int:
    return options.index(chosen) if chosen in options else 0


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri")
    parser.add_argument("--demo", action="store_true")
    return parser.parse_args(sys.argv[1:])


main()
