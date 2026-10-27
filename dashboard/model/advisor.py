"""The advisor: rules that turn the facts into changes worth making, each with its evidence.

Health says what is broken now; the advisor says what would make pipelines faster, cheaper, or
complete. Every rule names the measure it read, so a reader can check it before acting.
"""

from dataclasses import dataclass
from typing import Any

import polars as pl

from dashboard.model import capacity, completeness, failures, performance
from dashboard.model.facts import WAITING, View
from dashboard.units import edge, mb

WAITS = 0.3  # a step taking at least this share of run time, while waiting, is worth tuning
HOLDS = 0.7  # a resource last to finish in this share of shared loads holds the unit up
TIGHT, LOOSE = 0.7, 0.3  # p99 memory above / below this share of what was available
RECURS = 3  # failures in one place before it is a pattern
_MOVED = {"hole": "skipped ahead", "reset": "went back"}


@dataclass(frozen=True, slots=True)
class Advice:
    level: str  # "warning": costing time, money, or data; "info": worth knowing
    area: str  # concurrency, memory, destination, completeness, reliability
    pipeline: str
    finding: str
    evidence: str
    action: str


def advise(view: View) -> list[Advice]:
    advice: list[Advice] = []
    workers = _workers(view)
    for row in performance.step_profile(view).iter_rows(named=True):
        if row["share"] is None or row["share"] < WAITS or row["cpu_share"] is None:
            continue
        pipeline, step = row["pipeline_id"], row["step"]
        evidence = f"{row['share']:.0%} of run time, {row['cpu_share']:.0%} of it computing"
        if step == "extract" and row["cpu_share"] <= WAITING:
            setup = workers.get(pipeline, {})
            now, source = setup.get("extract_workers"), setup.get("source") or "<source>"
            advice.append(
                Advice(
                    "warning",
                    "concurrency",
                    pipeline,
                    "extract mostly waits on upstream",
                    evidence,
                    f"raise [sources.{source}.extract] workers (now {now or '?'}) if upstream "
                    "allows parallel reads, split resources into pipelines of their own, or check "
                    "the source for a slow query or a missing index on the cursor column",
                )
            )
        elif step == "load" and row["cpu_share"] <= WAITING:
            advice.append(
                Advice(
                    "warning",
                    "destination",
                    pipeline,
                    "load mostly waits on the destination",
                    evidence,
                    "see Slow tables: index the merge key of the slowest, or merge it as "
                    "delete-insert; on Postgres raise [load] workers",
                )
            )
        elif step == "normalize" and row["cpu_share"] >= 0.8 * (
            workers.get(pipeline, {}).get("normalize_workers") or 1
        ):
            advice.append(
                Advice(
                    "info",
                    "concurrency",
                    pipeline,
                    "normalize is CPU-bound",
                    evidence,
                    "raise [normalize] workers if Capacity shows idle cores",
                )
            )
    for row in performance.critical_path(view).iter_rows(named=True):
        if row["loads"] >= 3 and row["last"] / row["loads"] >= HOLDS and (row["share"] or 0) >= 0.6:
            advice.append(
                Advice(
                    "info",
                    "concurrency",
                    row["pipeline_id"],
                    f"{row['resource']} holds up the resources extracted with it",
                    f"last to finish in {row['last']} of {row['loads']} loads",
                    "give it a pipeline of its own, or chunk it, so the others commit sooner",
                )
            )
    for row in capacity.memory(view).iter_rows(named=True):
        share, pipeline = row["share"], row["pipeline_id"]
        if share is None:
            continue
        peak, available = mb(row["p99_mb"]), mb(row["available_mb"])
        evidence = f"p99 peak {peak} of {available} ({share:.0%}), {row['runs']} runs"
        if share >= TIGHT:
            advice.append(
                Advice(
                    "warning",
                    "memory",
                    pipeline,
                    "runs close to their memory",
                    evidence,
                    f"give it {mb(row['recommended_mb'])}, or chunk its largest resource",
                )
            )
        elif share <= LOOSE and row["limit_mb"] is not None:
            advice.append(
                Advice(
                    "info",
                    "memory",
                    pipeline,
                    "memory mostly idle",
                    evidence,
                    f"its limit could come down to {mb(row['recommended_mb'])}",
                )
            )
    for covered in completeness.coverage(view):
        if covered.commands and covered.gaps[0][1] is not None:
            pipeline = covered.resource_id.split(".", 1)[0]
            advice.append(
                Advice(
                    "warning",
                    "completeness",
                    pipeline,
                    f"{covered.resource_id} history has {len(covered.gaps)} gap(s)",
                    f"{'-' if covered.fraction is None else f'{covered.fraction:.0%}'} covered",
                    covered.commands[0],
                )
            )
    for found in completeness.cursor_breaks(view):
        if found.kind == "reread":
            continue  # expected with lag; shown on Completeness
        advice.append(
            Advice(
                "warning",
                "completeness",
                found.resource_id.split(".", 1)[0],
                f"{found.resource_id} cursor {_MOVED[found.kind]}",
                f"{edge(found.previous_last)} → {edge(found.next_start)} "
                f"at {found.at:%Y-%m-%d %H:%M}Z",
                "backfill the skipped range"
                if found.kind == "hole"
                else "check for a state reset; under Append, de-duplicate",
            )
        )
    for row in completeness.reconciled(view).iter_rows(named=True):
        if (row["duplicates"] or 0) > 0 or row["differing"]:
            advice.append(
                Advice(
                    "warning",
                    "completeness",
                    row["resource_id"].split(".", 1)[0],
                    f"{row['resource_id']} does not match",
                    f"{row['duplicates'] or 0} duplicate keys, "
                    f"{row['differing']} window(s) differ from the source",
                    "see Completeness; backfill the differing windows, check the merge key",
                )
            )
    for row in failures.by_cause(view).iter_rows(named=True):
        if row["count"] < RECURS:
            continue
        killed = row["cause"] == "killed"
        advice.append(
            Advice(
                "warning",
                "memory" if killed else "reliability",
                row["pipelines"],
                "runs keep being killed" if killed else f"{row['cause']} recurs",
                f"{row['count']} {'lost runs' if killed else 'failures at ' + row['where']}",
                "see Capacity: give them more memory, or chunk the largest resource"
                if killed
                else "fix it where it is raised; retrying will not",
            )
        )
    return sorted(advice, key=lambda item: (item.level != "warning", item.area, item.pipeline))


def _workers(view: View) -> dict[str, dict[str, Any]]:
    """Each pipeline's workers as its latest run recorded them."""
    latest = (
        view.frames.pipelines.sort("at")
        .group_by("pipeline_id")
        .agg(pl.col("source", "extract_workers", "normalize_workers", "load_workers").last())
    )
    return {row.pop("pipeline_id"): row for row in latest.to_dicts()}
