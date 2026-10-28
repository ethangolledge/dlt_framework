"""Triage: each production pipeline's state, from three things that do not overlap.

- **Incidents** (``model/incidents.py``): an open one means it is failing.
- **Missing data** (``model/gaps.py``): rows that should be there and are not.
- **Warnings** from the checks, and from health where only its thresholds can judge: late
  (against a stated or regular schedule), slower than usual, unusual rows.

A pipeline is as bad as the worst of these. Health's other signals (failed, killed, memory,
reconcile, gaps) are what incidents, checks, and gaps already say, so they are not read here.
"""

from dataclasses import dataclass

from dashboard.model import gaps
from dashboard.model.checks import Report
from dashboard.model.facts import View
from dashboard.model.incidents import Incident
from dlt_framework.core.monitoring.health import Signal

RANK = {"error": 0, "warning": 1, "ok": 2, "idle": 3}
_HEALTH = {  # event: (level, headline)
    "run_stale": ("error", "late"),
    "run_slow": ("warning", "slower than usual"),
    "rows_anomaly": ("warning", "unusual row count"),
}


@dataclass(frozen=True, slots=True)
class Concern:
    level: str  # "error" or "warning"
    headline: str
    detail: str  # the evidence, for a tooltip


@dataclass(frozen=True, slots=True)
class Status:
    pipeline: str
    level: str  # "error", "warning", "ok", or "idle": no runs in range
    incident: Incident | None  # its open incident in production
    concerns: tuple[Concern, ...]  # worst first, the open incident included

    @property
    def headline(self) -> str:
        if not self.concerns:
            return "no runs in range" if self.level == "idle" else "ok"
        more = len(self.concerns) - 1
        return self.concerns[0].headline + (f"  +{more}" if more else "")


def statuses(
    view: View,
    incidents: list[Incident],
    report: Report,
    missing: list[gaps.Missing],
    signals: list[Signal] | None,
) -> list[Status]:
    """Every pipeline in scope, worst first."""
    active = set(view.runs["pipeline_id"].drop_nulls())
    kept = view.scope.pipelines
    found = []
    for pipeline in (name for name in view.pipelines if not kept or name in kept):
        incident = next(
            (
                item
                for item in incidents
                if item.open and item.pipeline == pipeline and item.mode == "run"
            ),
            None,
        )
        concerns = [
            *_failing(incident),
            *(
                Concern(*_HEALTH[signal.event], signal.detail)
                for signal in signals or []
                if signal.pipeline == pipeline and signal.event in _HEALTH
            ),
            *(
                Concern("warning", finding.title, finding.evidence)
                for finding in report.findings
                if finding.pipeline == pipeline and finding.level == "warning"
            ),
            *_missing(pipeline, missing),
        ]
        concerns.sort(key=lambda concern: RANK[concern.level])
        level = concerns[0].level if concerns else "ok" if pipeline in active else "idle"
        found.append(Status(pipeline, level, incident, tuple(concerns)))
    return sorted(found, key=lambda status: (RANK[status.level], status.pipeline))


def _failing(incident: Incident | None) -> list[Concern]:
    if incident is None:
        return []
    runs = len(incident.runs)
    return [
        Concern(
            "error",
            f"failing: {runs} run(s) since {incident.started_at:%m-%d %H:%M}",
            incident.summary,
        )
    ]


def _missing(pipeline: str, missing: list[gaps.Missing]) -> list[Concern]:
    """Rows missing from a pipeline's tables, as one concern per resource."""
    by_resource: dict[str, list[gaps.Missing]] = {}
    for item in missing:
        if item.pipeline == pipeline and item.kind in gaps.FIXABLE:
            by_resource.setdefault(item.resource_id, []).append(item)
    return [
        Concern(
            "warning",
            f"{resource_id.split('.', 1)[1]}: {len(items)} range(s) missing",
            "; ".join(gaps.edges(item) for item in items[:3]),
        )
        for resource_id, items in by_resource.items()
    ]
