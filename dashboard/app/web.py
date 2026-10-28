"""The web app, on Streamlit: ``pipeline-dashboard serve``, or ``streamlit run`` on this file.

Only the controls are Streamlit's; every page is the same styled text the terminal shows,
encoded as HTML. Choices live in the URL, so a view can be shared or bookmarked.
"""

import argparse
import sys
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from urllib.parse import urlencode

import streamlit as st

from dashboard.app.session import Choices, draw, load
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
        days = {
            **({"from": choices.first.isoformat()} if choices.first else {}),
            **({"to": choices.last.isoformat()} if choices.last else {}),
        }
        query = {"page": "runs", "focus": pipeline, **days}
        return "?" + urlencode({**query, **({"run": run_id} if kind == "run" else {})})

    return href


@st.cache_data(ttl=REFRESH_S, show_spinner="reading monitoring frames…")
def _frames(uri: str | None) -> Frames:
    return load(uri)


def _controls(frames: Frames) -> Choices:
    """The sidebar, seeded from and written back to the URL: the page, the days shown (14 back
    from today unless a range is picked), and on the Runs page one pipeline from a dropdown."""
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
        default = (now.date() - timedelta(days=13), now.date())
        days = st.date_input(
            "days",
            value=(_day(query.get("from")) or default[0], _day(query.get("to")) or default[1]),
            max_value=now.date(),
        )
        # a range while being picked has one end; a finished one has two
        picked = (*days, None, None) if isinstance(days, tuple) else (days, None)
        first, last = picked[0], picked[1]
        known = View.build(frames, Scope.last("all", now), now).pipelines
        focus = run_id = None
        pipelines: list[str] = []
        if PAGES[page].picks == "pipeline" and known:
            focus = str(
                st.selectbox(
                    f"pipeline ({len(known)})", known, index=_index(known, query.get("focus"))
                )
            )
            run_id = query.get("run") if query.get("focus") == focus else None
        else:
            pipelines = st.multiselect(
                "only these pipelines",
                known,
                default=[p for p in query.get_all("pipeline") if p in known],
            )
        modes = st.multiselect("modes", ["run", "backfill"], default=query.get_all("mode"))
        size = st.slider("columns", 100, 240, int(query.get("columns", 160)), step=10)
        st.toggle("live (every minute)", value=True, key="live")
        st.toggle("scanlines", value=True, key="scanlines")
        if st.button("↻ read the frames again"):
            _frames.clear()
    st.query_params.from_dict(
        {
            "page": page,
            "pipeline": pipelines,
            "mode": modes,
            "columns": str(size),
            **({"from": first.isoformat()} if first else {}),
            **({"to": last.isoformat()} if last else {}),
            **({"run": run_id} if run_id else {}),
            **({"focus": focus} if focus else {}),
        }
    )
    return Choices(
        page=page,
        first=first,
        last=last,
        pipelines=tuple(pipelines),
        modes=tuple(modes),
        run_id=run_id,
        focus=focus,
        size=size,
    )


def _day(text: str | None) -> date | None:
    try:
        return date.fromisoformat(text) if text else None
    except ValueError:
        return None


def _index(options: list[str], chosen: str | None) -> int:
    return options.index(chosen) if chosen in options else 0


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri")
    return parser.parse_args(sys.argv[1:])


main()
