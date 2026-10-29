"""``pipeline-dashboard``: build the browser dashboard, or ask the terminal two questions.

- ``build FOLDER``: the dashboard, written to a folder to open or host (``dashboard/web``).
- ``status``: is ingestion healthy? One line per pipeline, worst first. Exits 1 while any
  pipeline is failing, so a cron job or an orchestrator can alert on it.
- ``run ID``: what happened in one run: how it ended, the error, and the command to run next.

Each reads ``MONITORING__URI`` (as runs do), or ``--uri``.
"""

import argparse
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from dashboard.model import incidents, source, triage
from dashboard.model.facts import BROKEN, Scope, View
from dashboard.render.encode import ansi
from dashboard.render.text import Block, Line, fit, join, span
from dashboard.theme import STATES
from dashboard.units import ago
from dashboard.web import build, snapshot


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    return int(args.command(args) or 0)


def _build(args: argparse.Namespace) -> None:
    frames = source.read(source.monitoring_uri(args.uri))
    held = build.build(Path(args.folder), frames, datetime.now(UTC), source.signals(frames))
    print(f"dashboard written to {args.folder}/index.html ({held} runs); open it in a browser")


def _status(args: argparse.Namespace) -> int:
    frames = source.read(source.monitoring_uri(args.uri))
    now = datetime.now(UTC)
    judged = triage.judge(frames, now, source.signals(frames))
    states = judged.statuses
    counts = {key: sum(s.level == key for s in states) for key in STATES}
    late = "" if judged.lateness else " · lateness not judged: no [health] settings"
    lines: Block = [
        span(
            f"dlt//ops · {now:%Y-%m-%d %H:%M}Z · judged over the last {triage.DAYS} days{late}",
            "muted",
        ),
        join(
            *(
                join(span(f"{STATES[k][0]} {n} {STATES[k][2]}", STATES[k][1]), span("   "))
                for k, n in counts.items()
                if n
            )
        ),
    ]
    width = max((len(s.pipeline) for s in states), default=8) + 2
    for status in states:
        glyph, style, _ = STATES[status.level]
        last = judged.last_success.get(status.pipeline)
        line: Line = join(
            span(f"{glyph} ", style),
            fit(span(status.pipeline, "bright"), width),
            span(status.headline, style if status.level != "ok" else "muted"),
            span(f"   last ok {ago(now - last) if last else 'never'}", "dim"),
        )
        if status.incident:
            line = join(line, span(f"   → $ {incidents.rerun(status.incident.latest)}", "ok"))
        lines.append(line)
    _print(lines, args)
    return 1 if counts["error"] else 0


def _run(args: argparse.Namespace) -> int:
    frames = source.read(source.monitoring_uri(args.uri))
    now = datetime.now(UTC)
    history = View.build(frames, Scope.last("all", now), now).history
    found = history.filter(pl.col("run_id").str.starts_with(args.id))
    if found.height != 1:
        print(f"{found.height} runs start with {args.id!r}: give more of the id", file=sys.stderr)
        return 2
    run = found.row(0, named=True)
    detail = snapshot.detail(frames, run["run_id"])
    error = detail["error"]
    broken = run["outcome"] in BROKEN
    word = "KILLED" if run["outcome"] == "lost" else str(run["outcome"]).upper()
    style = {"succeeded": "ok", "retried": "warn", "running": "info"}.get(run["outcome"], "error")
    lines: Block = [
        join(
            span(f"run {run['run_id']} · ", "muted"),
            span(str(run["pipeline_id"]), "bright"),
            span(f" · {run['mode']} · {run['started_at']:%Y-%m-%d %H:%M:%S}Z", "muted"),
        ),
        join(span(f"{word}  ", style), span(incidents.detail(run) if broken else "", style)),
    ]
    for name, value in (
        (
            "cause",
            f"{error.get('cause_type')}: {error.get('message')}"
            if error.get("cause_type")
            else None,
        ),
        ("our code", error.get("where")),
        ("raised in", error.get("origin")),
        ("window", error.get("window")),
    ):
        if value:
            lines.append(join(span(f"  {name:<10}", "muted"), span(str(value), "text")))
    for load in detail["loads"]:
        before, after = load["cursor"] if load["kind"] == "increment" else load["window"]
        lines.append(
            join(
                span(f"  loaded    {load['resource']}: {load['rows']:,} rows", "text"),
                span(f"  {before or '·'} → {after or '·'}", "dim") if before or after else [],
            )
        )
    if broken:
        lines.append(
            join(span("fix the cause, then  ", "muted"), span(f"$ {incidents.rerun(run)}", "ok"))
        )
    _print(lines, args)
    return 0


def _print(lines: Block, args: argparse.Namespace) -> None:
    colour = (sys.stdout.isatty() and not args.no_colour) or args.colour
    print(ansi(lines, colour=colour))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline-dashboard", description=__doc__.split("\n")[0])
    commands = parser.add_subparsers(required=True)

    def common(sub: argparse.ArgumentParser) -> argparse.ArgumentParser:
        sub.add_argument("--uri", help="monitoring URI; MONITORING__URI when omitted")
        sub.add_argument("--no-colour", action="store_true")
        sub.add_argument("--colour", action="store_true", help="colour even when not a terminal")
        return sub

    built = common(commands.add_parser("build", help="write the browser dashboard to a folder"))
    built.set_defaults(command=_build)
    built.add_argument("folder")
    common(commands.add_parser("status", help="health now, a line a pipeline")).set_defaults(
        command=_status
    )
    one = common(commands.add_parser("run", help="one run: how it ended, and what to run next"))
    one.set_defaults(command=_run)
    one.add_argument("id", help="the run id, or enough of its start to be unique")
    return parser


def entrypoint() -> None:
    raise SystemExit(main())
