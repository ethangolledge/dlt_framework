"""Triage: each pipeline's state, and one list of what to do about it, worst first.

Three sources feed the list. The runs themselves: a pipeline whose latest runs failed is
failing, whatever thresholds say. The environment's health signals, where it states
thresholds, for what only they can judge (late, slow). And the advisor. Every item is an
``Advice``, so the overview, a pipeline's page, and the actions page tell the same story in
the same words.
"""

from dataclasses import dataclass
from typing import Any

import polars as pl

from dashboard.model import advisor, failures
from dashboard.model.advisor import Advice
from dashboard.model.facts import View
from dashboard.units import first_line
from dlt_framework.core.monitoring.health import Signal

RANK = {"error": 0, "warning": 1, "info": 2, "ok": 3, "idle": 4}

# Signals only health can raise; the rest (failed, lost, memory, reconcile, gaps) the runs and
# the advisor already say, and the same fact twice is noise.
_SIGNALS = {
    "run_stale": ("reliability", "late", "check its schedule: the orchestrator may have stopped"),
    "run_slow": ("performance", "slower than usual", "open its latest run"),
    "retries_rising": ("reliability", "retrying more", "see Failures for the cause"),
    "rows_anomaly": ("completeness", "unusual row count", "check upstream for a change"),
}


@dataclass(frozen=True, slots=True)
class Status:
    pipeline: str
    level: str  # "error", "warning", "ok", or "idle": no runs in range
    found: tuple[Advice, ...]  # its errors and warnings, worst first

    @property
    def headline(self) -> str:
        """Its worst finding, and how many more."""
        if not self.found:
            return "no runs in range" if self.level == "idle" else "ok"
        more = len(self.found) - 1
        return self.found[0].finding + (f"  +{more}" if more else "")


def actions(view: View, signals: list[Signal] | None) -> list[Advice]:
    """Everything to do, worst first: failing pipelines, then health, then the advisor."""
    broken = failing(view)
    down = {item.pipeline for item in broken}
    advice = [
        item
        for item in advisor.advise(view)
        # a cause recurring in a pipeline failing now is the same story as the failure
        if not (item.area == "reliability" and any(item.concerns(name) for name in down))
    ]
    found = [*broken, *_health(signals or []), *advice]
    return sorted(found, key=lambda item: (RANK[item.level], advisor.AREAS.index(item.area)))


def failing(view: View) -> list[Advice]:
    """Pipelines whose latest runs failed or were lost: how many in a row, why, and the fix."""
    found = []
    for pipeline in _pipelines(view):
        own = view.history.filter(
            (pl.col("pipeline_id") == pipeline)
            & (pl.col("mode") == "run")
            & (pl.col("outcome") != "running")
        ).sort("started_at", descending=True)
        streak = 0
        for outcome in own["outcome"]:
            if outcome not in failures.BROKEN:
                break
            streak += 1
        if not streak:
            continue
        run = own.row(0, named=True)
        found.append(explain(run, streak))
    return found


def explain(run: dict[str, Any], streak: int = 1) -> Advice:
    """A failed or lost run as a finding: what happened, and the fix then the command."""
    lost = run["outcome"] == "lost"
    cause = "killed" if lost else str(run["error_cause_type"] or run["error_type"] or "error")
    finding = (
        f"{'killed' if lost else 'failed'} {streak} runs in a row"
        if streak > 1
        else f"last run {'killed' if lost else 'failed'}"
    )
    rerun = f"$ {failures.next_step(run)}"
    where = run["error_where"]
    return Advice(
        "error",
        "reliability",
        str(run["pipeline_id"]),
        finding if lost else f"{finding}: {cause}",
        f"during {run['error_step'] or 'an unknown step'}, at its memory peak"
        if lost
        else f"{first_line(run['error_message'])}",
        f"give it more memory, then {rerun}"
        if lost
        else f"fix {where}, then {rerun}"
        if where
        else f"read run {str(run['run_id'])[:8]}, then {rerun}",
        run_id=str(run["run_id"]),
    )


def _health(signals: list[Signal]) -> list[Advice]:
    found = []
    for signal in signals:
        if signal.event not in _SIGNALS:
            continue
        area, finding, action = _SIGNALS[signal.event]
        found.append(
            Advice(
                "error" if signal.level == "error" else "warning",
                area,
                signal.pipeline,
                f"{signal.resource}: {finding}" if signal.resource else finding,
                signal.detail,
                action,
            )
        )
    return found


def statuses(view: View, todo: list[Advice]) -> list[Status]:
    """Every pipeline in scope, worst first; a pipeline is as bad as its worst finding."""
    active = set(view.runs["pipeline_id"].drop_nulls())
    found = []
    for pipeline in _pipelines(view):
        mine = tuple(
            item for item in todo if item.concerns(pipeline) and item.level in ("error", "warning")
        )
        level = mine[0].level if mine else "ok" if pipeline in active else "idle"
        found.append(Status(pipeline, level, mine))
    return sorted(found, key=lambda status: (RANK[status.level], status.pipeline))


def _pipelines(view: View) -> list[str]:
    kept = view.scope.pipelines
    return [name for name in view.pipelines if not kept or name in kept]
