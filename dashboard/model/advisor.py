"""The advisor: rules that turn the facts into changes worth making, each with its evidence.

Health says what is broken now; the advisor says what would make pipelines faster, cheaper, or
complete. A finding is a headline a reader takes in at a glance (``memory at 92%``); its
evidence names the measure it read, so it can be checked before acting; its action is the one
change to make, with any alternatives kept aside in ``also``.
"""

from dataclasses import dataclass
from typing import Any

import polars as pl

from dashboard.model import capacity, completeness, failures, performance
from dashboard.model.facts import WAITING, View
from dashboard.units import edge, mb, seconds

WAITS = 0.3  # a step taking at least this share of run time, while waiting, is worth tuning
HOLDS = 0.7  # a resource last to finish in this share of shared loads holds the unit up
TIGHT, LOOSE = 0.7, 0.3  # p99 memory above / below this share of what was available
RECURS = 3  # failures in one place before it is a pattern
SLOWER = 0.2  # a release whose median run is this much slower than the one before
# Most urgent first: what breaks runs, what wastes them, what loses data, what slows them.
AREAS = ("reliability", "memory", "completeness", "performance", "concurrency", "destination")
_MOVED = {"hole": "cursor skipped ahead", "reset": "cursor went back"}


@dataclass(frozen=True, slots=True)
class Advice:
    level: str  # "error": broken now; "warning": costing time, money, or data; "info": tuning
    area: str  # one of AREAS
    pipeline: str  # one pipeline, or several joined by ", "
    finding: str  # the headline
    evidence: str
    action: str
    also: str = ""  # alternatives to the action
    run_id: str | None = None  # the run that shows it, where one does

    def concerns(self, pipeline: str) -> bool:
        return pipeline in self.pipeline.split(", ")


def advise(view: View) -> list[Advice]:
    advice = [
        *_steps(view),
        *_critical_path(view),
        *_memory(view),
        *_releases(view),
        *_completeness(view),
        *_recurring(view),
    ]
    return sorted(
        advice, key=lambda item: (item.level != "warning", AREAS.index(item.area), item.pipeline)
    )


def _steps(view: View) -> list[Advice]:
    found, workers = [], _workers(view)
    for row in performance.step_profile(view).iter_rows(named=True):
        if row["share"] is None or row["share"] < WAITS or row["cpu_share"] is None:
            continue
        pipeline, step = row["pipeline_id"], row["step"]
        setup = workers.get(pipeline, {})
        evidence = f"{row['share']:.0%} of run time, {row['cpu_share']:.0%} computing"
        if step == "extract" and row["cpu_share"] <= WAITING:
            source = setup.get("source") or "<source>"
            found.append(
                Advice(
                    "warning",
                    "concurrency",
                    pipeline,
                    "extract waits on upstream",
                    evidence,
                    f"raise [sources.{source}.extract] workers "
                    f"(now {setup.get('extract_workers') or '?'})",
                    "if upstream allows parallel reads; or split resources into pipelines of "
                    "their own; or look for a slow query or a missing index on the cursor column",
                )
            )
        elif step == "load" and row["cpu_share"] <= WAITING:
            found.append(
                Advice(
                    "warning",
                    "destination",
                    pipeline,
                    "load waits on the destination",
                    evidence,
                    "index the slowest table's merge key (see Speed)",
                    "or merge it as delete-insert; on Postgres raise [load] workers",
                )
            )
        elif step == "normalize" and row["cpu_share"] >= 0.8 * (
            setup.get("normalize_workers") or 1
        ):
            found.append(
                Advice(
                    "info",
                    "concurrency",
                    pipeline,
                    "normalize is CPU-bound",
                    evidence,
                    f"raise [normalize] workers (now {setup.get('normalize_workers') or 1})",
                    "if Capacity shows idle cores",
                )
            )
    return found


def _critical_path(view: View) -> list[Advice]:
    return [
        Advice(
            "info",
            "concurrency",
            row["pipeline_id"],
            f"{row['resource']} finishes last",
            f"last in {row['last']} of {row['loads']} loads",
            f"give {row['resource']} a pipeline of its own",
            "or chunk it, so the resources extracted with it commit sooner",
        )
        for row in performance.critical_path(view).iter_rows(named=True)
        if row["loads"] >= 3 and row["last"] / row["loads"] >= HOLDS and (row["share"] or 0) >= 0.6
    ]


def _memory(view: View) -> list[Advice]:
    found = []
    for row in capacity.memory(view).iter_rows(named=True):
        share, pipeline = row["share"], row["pipeline_id"]
        if share is None:
            continue
        evidence = f"p99 peak {mb(row['p99_mb'])} of {mb(row['available_mb'])}, {row['runs']} runs"
        if share >= TIGHT:
            found.append(
                Advice(
                    "warning",
                    "memory",
                    pipeline,
                    f"memory at {share:.0%}",
                    evidence,
                    f"give it {mb(row['recommended_mb'])}",
                    "or chunk its largest resource",
                )
            )
        elif share <= LOOSE and row["limit_mb"] is not None:
            found.append(
                Advice(
                    "info",
                    "memory",
                    pipeline,
                    f"memory {share:.0%} used",
                    evidence,
                    f"lower its limit to {mb(row['recommended_mb'])}",
                )
            )
    return found


def _releases(view: View) -> list[Advice]:
    """A release that made runs slower than the release before it."""
    found = []
    for key, own in performance.by_version(view).group_by("pipeline_id", maintain_order=True):
        rows = own.filter(pl.col("runs") >= 3).tail(2).to_dicts()
        if len(rows) < 2 or not rows[0]["p50_s"]:
            continue
        before, after = rows
        slower = after["p50_s"] / before["p50_s"] - 1
        if slower >= SLOWER:
            found.append(
                Advice(
                    "warning",
                    "performance",
                    str(key[0]),
                    f"{after['code_version']} is {slower:.0%} slower",
                    f"median run {seconds(before['p50_s'])} on {before['code_version']}, "
                    f"{seconds(after['p50_s'])} since",
                    f"compare {after['code_version']} with {before['code_version']}",
                )
            )
    return found


def _completeness(view: View) -> list[Advice]:
    found = []
    for covered in completeness.coverage(view):
        if covered.commands and covered.gaps[0][1] is not None:
            pipeline, resource = covered.resource_id.split(".", 1)
            portion = "-" if covered.fraction is None else f"{covered.fraction:.0%}"
            found.append(
                Advice(
                    "warning",
                    "completeness",
                    pipeline,
                    f"{resource}: {len(covered.gaps)} history gap(s)",
                    f"{portion} of declared history loaded",
                    f"$ {covered.commands[0]}",
                )
            )
    for broken in completeness.cursor_breaks(view):
        if broken.kind == "reread":
            continue  # expected with lag; shown on Completeness
        pipeline, resource = broken.resource_id.split(".", 1)
        found.append(
            Advice(
                "warning",
                "completeness",
                pipeline,
                f"{resource}: {_MOVED[broken.kind]}",
                f"{edge(broken.previous_last)} → {edge(broken.next_start)} "
                f"at {broken.at:%Y-%m-%d %H:%M}Z",
                "backfill the skipped range"
                if broken.kind == "hole"
                else "check for a state reset; under Append, de-duplicate",
            )
        )
    for row in completeness.reconciled(view).iter_rows(named=True):
        duplicates, differing = row["duplicates"] or 0, row["differing"]
        if not duplicates and not differing:
            continue
        pipeline, resource = row["resource_id"].split(".", 1)
        what = [f"{duplicates} duplicate keys"] if duplicates else []
        what += [f"{differing} window(s) off the source"] if differing else []
        todo = ["check the merge key"] if duplicates else []
        todo += ["backfill the differing windows"] if differing else []
        source = "-" if row["source_rows"] is None else f"{row['source_rows']:,}"
        found.append(
            Advice(
                "warning",
                "completeness",
                pipeline,
                f"{resource}: {', '.join(what)}",
                f"{row['rows']:,} rows here, {source} at the source",
                "; ".join(todo),
            )
        )
    return found


def _recurring(view: View) -> list[Advice]:
    found = []
    for row in failures.by_cause(view).iter_rows(named=True):
        if row["count"] < RECURS:
            continue
        killed = row["cause"] == "killed"
        found.append(
            Advice(
                "warning",
                "memory" if killed else "reliability",
                row["pipelines"],
                "runs keep being killed" if killed else f"{row['cause']} recurs",
                f"{row['count']} lost runs"
                if killed
                else f"{row['count']} times at {row['where']}",
                "give them more memory (see Capacity)"
                if killed
                else "fix it where it is raised; retrying will not",
                "or chunk the largest resource" if killed else "",
            )
        )
    return found


def _workers(view: View) -> dict[str, dict[str, Any]]:
    """Each pipeline's workers as its latest run recorded them."""
    latest = (
        view.frames.pipelines.sort("at")
        .group_by("pipeline_id")
        .agg(pl.col("source", "extract_workers", "normalize_workers", "load_workers").last())
    )
    return {row.pop("pipeline_id"): row for row in latest.to_dicts()}
