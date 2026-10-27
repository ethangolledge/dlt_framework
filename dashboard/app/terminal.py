"""``pipeline-dashboard``: any page in a terminal (for SSH, cron, an incident channel), the
web app, or the demo frames."""

import argparse
import shutil
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

from dashboard import demo
from dashboard.app.session import RANGES, Choices, draw, load
from dashboard.pages import PAGES
from dashboard.render.encode import FONTS, ansi, html, stylesheet
from dashboard.theme import COLOURS
from dlt_framework.core.monitoring import ParquetSink
from dlt_framework.core.monitoring.records import RECORDS


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    return int(args.command(args) or 0)


def _show(args: argparse.Namespace) -> None:
    frames = load(args.uri, args.demo)
    choices = Choices(
        page=args.page,
        since=args.since,
        pipelines=tuple(args.pipeline or ()),
        modes=tuple(args.mode or ()),
        outcomes=tuple(args.outcome or ()),
        run_id=args.run,
        size=args.width or shutil.get_terminal_size((140, 40)).columns,
    )
    print(
        ansi(
            draw(frames, choices),
            colour=(not args.no_colour and sys.stdout.isatty()) or args.colour,
        )
    )


def _serve(args: argparse.Namespace) -> int:
    """The web app, through Streamlit (the ``dashboard`` extra)."""
    app = Path(__file__).with_name("web.py")
    forwarded = [*(["--uri", args.uri] if args.uri else []), *(["--demo"] if args.demo else [])]
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
    """Every page as static HTML, linked to each other: for hosting anywhere files are served."""
    folder = Path(args.folder)
    folder.mkdir(parents=True, exist_ok=True)
    frames = load(args.uri, args.demo)
    for key, page in PAGES.items():
        choices = Choices(page=key, since=args.since, size=args.width)
        body = html(draw(frames, choices), page.title)
        (folder / _file(key)).write_text(
            _site(page.title, page.summary, key, body), encoding="utf-8"
        )
    print(f"{len(PAGES)} pages written to {folder}/index.html")


def _site(title: str, summary: str, current: str, body: str) -> str:
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
nav {{ margin: 0 0 6px 0; font-size: 13px; }}
nav a {{ color: {COLOURS["muted"]}; text-decoration: none; margin-right: 14px; }}
nav a.on, nav a:hover {{ color: {COLOURS["ok"]}; text-shadow: 0 0 8px {COLOURS["ok"]}; }}
p.summary {{ color: {COLOURS["muted"]}; font-size: 12px; margin: 0 0 8px 0; }}
{stylesheet()}
</style></head>
<body><nav>{links}</nav><p class="summary">{title.upper()} ─ {summary}</p>{body}</body></html>
"""


def _file(key: str) -> str:
    return "index.html" if key == "overview" else f"{key}.html"


def _demo(args: argparse.Namespace) -> None:
    """Write the demo frames to a Parquet folder, as a monitoring sink would."""
    sink, frames = ParquetSink(str(Path(args.folder).resolve())), demo.frames()
    for record in RECORDS:
        sink.write(record.name, getattr(frames, record.name))
    print(
        f"demo frames written to {args.folder}; MONITORING__URI=file://{Path(args.folder).resolve()}"
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline-dashboard")
    commands = parser.add_subparsers(required=True)

    def source(sub: argparse.ArgumentParser) -> None:
        sub.add_argument("--uri", help="monitoring URI; MONITORING__URI when omitted")
        sub.add_argument("--demo", action="store_true", help="show generated demo frames")

    show = commands.add_parser("show", help="draw a page in the terminal")
    show.set_defaults(command=_show)
    show.add_argument("page", choices=list(PAGES), nargs="?", default="overview")
    show.add_argument("--since", choices=RANGES, default="24h")
    show.add_argument("--pipeline", action="append", help="only this pipeline; repeat for several")
    show.add_argument("--mode", action="append", choices=["run", "backfill"])
    show.add_argument(
        "--outcome", action="append", choices=["succeeded", "retried", "failed", "lost", "running"]
    )
    show.add_argument("--run", help="the run to show on pages about one run")
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

    written = commands.add_parser("demo", help="write demo frames to a folder")
    written.set_defaults(command=_demo)
    written.add_argument("folder")
    return parser


def entrypoint() -> None:
    raise SystemExit(main())
