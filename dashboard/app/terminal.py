"""``pipeline-dashboard``: any page in a terminal (for SSH, cron, an incident channel), the
web app, or every page as static HTML."""

import argparse
import shutil
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from dashboard.app.session import RANGES, Choices, draw, load
from dashboard.model import incidents
from dashboard.model.facts import Scope, View
from dashboard.pages import PAGES
from dashboard.pages.runs import LISTED
from dashboard.render.encode import FONTS, ansi, html, stylesheet
from dashboard.theme import COLOURS


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    return int(args.command(args) or 0)


def _show(args: argparse.Namespace) -> None:
    choices = Choices(
        page=args.page,
        since=args.since,
        pipelines=tuple(args.pipeline or ()),
        modes=tuple(args.mode or ()),
        run_id=args.run,
        focus=args.subject,
        size=args.width or shutil.get_terminal_size((140, 40)).columns,
    )
    print(
        ansi(
            draw(load(args.uri), choices),
            colour=(not args.no_colour and sys.stdout.isatty()) or args.colour,
        )
    )


def _serve(args: argparse.Namespace) -> int:
    """The web app, through Streamlit (the ``dashboard`` extra)."""
    app = Path(__file__).with_name("web.py")
    forwarded = ["--uri", args.uri] if args.uri else []
    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app),
        "--server.port",
        str(args.port),
        "--",
        *forwarded,
    ]
    return subprocess.call(command)


def _export(args: argparse.Namespace) -> None:
    """Every page as static HTML, for hosting anywhere files are served: the Runs page of each
    pipeline, and of each run it lists or an incident names, so every link leads somewhere."""
    folder = Path(args.folder)
    folder.mkdir(parents=True, exist_ok=True)
    frames, now = load(args.uri), datetime.now(UTC)
    view = View.build(frames, Scope.last(args.since, now), now)
    listed = view.runs.sort("started_at", descending=True).group_by("pipeline_id").head(LISTED)
    named = [run for item in incidents.incidents(view) for run in item.runs]
    runs = {
        *zip(listed["pipeline_id"], listed["run_id"], strict=True),
        *((run["pipeline_id"], run["run_id"]) for run in named),
    }

    def href(target: str) -> str:
        kind, _, key = target.partition("/")
        pipeline, _, run_id = key.partition("/")
        if kind == "run" and (pipeline, run_id) in runs:
            return f"runs-{pipeline}-{run_id}.html"
        return f"runs-{pipeline}.html"

    pages = [(_file(key), Choices(page=key)) for key in PAGES]
    pages += [(f"runs-{name}.html", Choices("runs", focus=name)) for name in view.pipelines]
    pages += [
        (f"runs-{pipeline}-{run_id}.html", Choices("runs", focus=pipeline, run_id=run_id))
        for pipeline, run_id in sorted(runs)
    ]
    for name, choices in pages:
        chosen = replace(choices, since=args.since, size=args.width)
        body = html(draw(frames, chosen, now), PAGES[chosen.page].title, href)
        (folder / name).write_text(_site(PAGES[chosen.page].title, chosen.page, body), "utf-8")
    print(f"{len(pages)} pages written to {folder}/index.html")


def _site(title: str, current: str, body: str) -> str:
    links = " ".join(
        f'<a class="{"on" if key == current else ""}" href="{_file(key)}">{page.title}</a>'
        for key, page in PAGES.items()
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>dlt//ops: {title}</title>
{FONTS}
<style>
body {{ background: {COLOURS["background"]}; color: {COLOURS["text"]}; margin: 0;
       padding: 12px 16px; font-family: 'JetBrains Mono', ui-monospace, monospace; }}
nav {{ margin: 0 0 8px 0; font-size: 13px; }}
nav a {{ color: {COLOURS["muted"]}; text-decoration: none; margin-right: 14px; }}
nav a.on, nav a:hover {{ color: {COLOURS["ok"]}; text-shadow: 0 0 8px {COLOURS["ok"]}; }}
{stylesheet()}
</style></head>
<body><nav>{links}</nav>{body}</body></html>
"""


def _file(key: str) -> str:
    return "index.html" if key == "overview" else f"{key}.html"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline-dashboard")
    commands = parser.add_subparsers(required=True)

    def source(sub: argparse.ArgumentParser) -> None:
        sub.add_argument("--uri", help="monitoring URI; MONITORING__URI when omitted")

    show = commands.add_parser("show", help="draw a page in the terminal")
    show.set_defaults(command=_show)
    show.add_argument("page", choices=list(PAGES), nargs="?", default="overview")
    show.add_argument("subject", nargs="?", help="the pipeline the Runs page shows")
    show.add_argument("--since", choices=RANGES, default="24h")
    show.add_argument("--pipeline", action="append", help="only this pipeline; repeat for several")
    show.add_argument("--mode", action="append", choices=["run", "backfill"])
    show.add_argument("--run", help="the run the Runs page opens")
    show.add_argument("--width", type=int, help="columns; the terminal's when omitted")
    show.add_argument("--no-colour", action="store_true")
    show.add_argument("--colour", action="store_true", help="colour even when not a terminal")
    source(show)

    serve = commands.add_parser("serve", help="the web app (needs the dashboard extra)")
    serve.set_defaults(command=_serve)
    serve.add_argument("--port", type=int, default=8501)
    source(serve)

    exported = commands.add_parser("export", help="every page as static HTML, for any web host")
    exported.set_defaults(command=_export)
    exported.add_argument("folder")
    exported.add_argument("--since", choices=RANGES, default="7d")
    exported.add_argument("--width", type=int, default=170)
    source(exported)
    return parser


def entrypoint() -> None:
    raise SystemExit(main())
