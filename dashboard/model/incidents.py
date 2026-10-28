"""Incidents: the unit of trouble.

An incident is a streak of failed or killed runs of one pipeline in one mode, from the first
of them to the next success. It is open until that success comes, then resolved: the open list
clears itself, and the resolved list is the history. Every cause in the streak is counted, so
"killed, then config missing twice, then stopped" is told as that, not as its last line.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import polars as pl

from dashboard.model.facts import View
from dashboard.units import first_line

BROKEN = ("failed", "lost")
OK = ("succeeded", "retried")
NEAR_LIMIT = 0.9  # a killed run this close to its memory limit was probably killed for memory


@dataclass(frozen=True, slots=True)
class Incident:
    pipeline: str
    mode: str
    runs: tuple[dict[str, Any], ...]  # its broken runs, newest first
    resolved_at: datetime | None  # when the success that ended it finished; None while open

    @property
    def open(self) -> bool:
        return self.resolved_at is None

    @property
    def started_at(self) -> datetime:
        return self.runs[-1]["started_at"]

    @property
    def latest(self) -> dict[str, Any]:
        return self.runs[0]

    def lasted(self, now: datetime) -> timedelta:
        return (self.resolved_at or now) - self.started_at

    @property
    def causes(self) -> list[tuple[str, int]]:
        """Each distinct cause and how many runs it broke, most first, then most recent."""
        counts: dict[str, int] = {}
        for run in self.runs:
            label = cause(run)
            counts[label] = counts.get(label, 0) + 1
        return sorted(counts.items(), key=lambda item: -item[1])

    @property
    def summary(self) -> str:
        """``killed, ConfigFieldMissingException ×2, SignalReceivedException``."""  # noqa: RUF002
        return ", ".join(
            f"{name} ×{count}" if count > 1 else name  # noqa: RUF001 - the sign reads as "times"
            for name, count in self.causes
        )


# Signals a run can be stopped by, and what usually sends them.
SIGNALS = {
    2: ("SIGINT", "interrupted, as by Ctrl-C"),
    9: ("SIGKILL", "killed outright, often by the out-of-memory killer"),
    15: ("SIGTERM", "told to stop from outside: a timeout, a deploy, or `kill`"),
}


def cause(run: dict[str, Any]) -> str:
    """A broken run's cause in a word or two: its root exception, ``stopped (SIGTERM)`` for a
    signal, or ``killed`` (``killed near memory limit`` only when its peak says so)."""
    if run["outcome"] == "lost":
        share = run.get("memory_share")
        return "killed near memory limit" if share and share >= NEAR_LIMIT else "killed"
    if (number := signal(run)) is not None:
        return f"stopped ({SIGNALS.get(number, (f'signal {number}', ''))[0]})"
    return str(run["error_cause_type"] or run["error_type"] or "error")


def signal(run: dict[str, Any]) -> int | None:
    """The signal that stopped a run, from dlt's ``Received `signal_code=15```."""
    message = str(run.get("error_message") or "")
    if "signal_code=" not in message:
        return None
    digits = message.split("signal_code=", 1)[1].lstrip("`'\"")
    number = "".join(char for char in digits[:3] if char.isdigit())
    return int(number) if number else None


def detail(run: dict[str, Any]) -> str:
    """Where and why, in one line: ``extract · form4 · Missing 1 field(s) in configuration``."""
    if run["outcome"] == "lost":
        seen = run["error_step"] or "an unknown step"
        return f"no end reported; last seen in {seen}"
    where = [part for part in (run["error_step"], run["error_resource"]) if part]
    if (number := signal(run)) is not None:
        name, meaning = SIGNALS.get(number, (f"signal {number}", "stopped from outside"))
        return " · ".join([*where, f"stopped by {name}: {meaning}"])
    return " · ".join([*where, first_line(run["error_message"])])


def incidents(view: View) -> list[Incident]:
    """Every incident active in the view's range, open first, then newest. A run still running
    neither breaks nor resolves a streak."""
    history = view.history.filter(pl.col("outcome") != "running")
    if view.scope.pipelines:
        history = history.filter(pl.col("pipeline_id").is_in(list(view.scope.pipelines)))
    if view.scope.modes:
        history = history.filter(pl.col("mode").is_in(list(view.scope.modes)))
    found = []
    for (pipeline, mode), own in history.sort("started_at").group_by(
        "pipeline_id", "mode", maintain_order=True
    ):
        streak: list[dict[str, Any]] = []
        for run in own.iter_rows(named=True):
            if run["outcome"] in BROKEN:
                streak.append(run)
            elif streak:
                found.append(
                    Incident(str(pipeline), str(mode), tuple(reversed(streak)), run["ended_at"])
                )
                streak = []
        if streak:
            found.append(Incident(str(pipeline), str(mode), tuple(reversed(streak)), None))
    start = view.scope.start
    active = [item for item in found if item.open or item.resolved_at >= start]  # type: ignore[operator]
    return sorted(active, key=lambda item: (not item.open, -item.latest["started_at"].timestamp()))


def rerun(run: dict[str, Any]) -> str:
    """The command to run once a broken run's cause is fixed: the run again, or a failed
    backfill resumed from the window it stopped at."""
    pipeline = run["pipeline_id"]
    if run.get("mode") == "backfill" and run.get("error_window") and run.get("error_resource"):
        start = str(run["error_window"]).split("[", 1)[-1].split(",", 1)[0].strip()
        resource = str(run["error_resource"]).split(", ")[0]
        return f"pipeline backfill {pipeline} --resource {resource} --from {start}"
    return f"pipeline run {pipeline}"


def by_day(found: list[Incident], days: int, now: datetime) -> list[tuple[str, list[int]]]:
    """Broken runs per cause per day for the last ``days`` days: one cause recurring across
    incidents, or many causes once each."""
    first = (now - timedelta(days=days - 1)).date()
    counts: dict[str, list[int]] = {}
    for incident in found:
        for run in incident.runs:
            day = (run["started_at"].date() - first).days
            if 0 <= day < days:
                counts.setdefault(cause(run), [0] * days)[day] += 1
    return sorted(counts.items(), key=lambda item: -sum(item[1]))
