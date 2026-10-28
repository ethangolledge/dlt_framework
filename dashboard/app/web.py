"""The web app, on Streamlit: ``pipeline-dashboard serve``, or ``streamlit run`` on this file.

Only the controls are Streamlit's; every page is the same styled text the terminal shows,
encoded as HTML. Choices live in the URL, so a view can be shared or bookmarked.
"""

import argparse
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from urllib.parse import urlencode

import streamlit as st

from dashboard.app.session import RANGES, Choices, draw, load
from dashboard.model.facts import Scope, View
from dashboard.pages import PAGES
from dashboard.render.encode import FONTS, html, stylesheet
from dashboard.theme import COLOURS
from dlt_framework.core.monitoring import Frames

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
    choices = _controls(_frames(args.uri))
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
    block = draw(_frames(args.uri), choices)
    st.markdown(html(block, PAGES[choices.page].title, _href(choices)), unsafe_allow_html=True)


def _href(choices: Choices) -> Callable[[str], str]:
    """Links to a pipeline's Runs page, or to a run on it, as this app's own URL."""

    def href(target: str) -> str:
        kind, _, key = target.partition("/")
        pipeline, _, run_id = key.partition("/")
        query = {"page": "runs", "since": choices.since, "focus": pipeline}
        return "?" + urlencode({**query, **({"run": run_id} if kind == "run" else {})})

    return href


@st.cache_data(ttl=REFRESH_S, show_spinner="reading monitoring frames…")
def _frames(uri: str | None) -> Frames:
    return load(uri)


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
        focus = run_id = None
        if PAGES[page].picks == "pipeline" and known:
            focus = str(st.selectbox("pipeline", known, index=_index(known, query.get("focus"))))
            run_id = query.get("run") if query.get("focus") == focus else None
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
            "columns": str(size),
            **({"run": run_id} if run_id else {}),
            **({"focus": focus} if focus else {}),
        }
    )
    return Choices(page, since, tuple(pipelines), tuple(modes), run_id, focus, size)


def _index(options: list[str], chosen: str | None) -> int:
    return options.index(chosen) if chosen in options else 0


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri")
    return parser.parse_args(sys.argv[1:])


main()
