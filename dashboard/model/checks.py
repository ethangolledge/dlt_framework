"""Checks: what is worth changing, each one a small class in a registry.

A check states the data it needs (``needs``, as ``frame.column``) and the thresholds it reads
(``Limits``, overridable in ``[dashboard.checks]``). The registry runs every check whose data
is there and lists the others as not checked, with what they need; pages never branch on which
check found what. A check claims only what its evidence shows, and its evidence names the
measure and how many runs it is from.

Failures are not checks: they are incidents (``model/incidents.py``). Missing data is not a
check either: it is a ``Missing`` range (``model/gaps.py``). So nothing is said twice.
"""

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, ClassVar

import polars as pl
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration

from dashboard.model import performance
from dashboard.model.facts import View
from dashboard.units import mb, seconds
from dlt_framework.core.config import resolve
from dlt_framework.core.errors import ConfigurationError

AREAS = ("memory", "reliability", "speed", "concurrency", "destination")

# What each piece of data a check needs is, for the "not checked" list.
NEEDS = {
    "runs.peak_rss_mb": "peak memory per run",
    "runs.available_mb": "a memory limit or the host's memory",
    "runs.memory_limit_mb": "a container memory limit",
    "runs.code_version": "a code version per run",
    "steps.attempt": "step records",
    "steps.cpu_s": "step records",
    "loads.extract_first_at": "extract times per load",
}


@configspec
class Limits(BaseConfiguration):
    """The thresholds checks judge by, overridable in ``[dashboard.checks]``."""

    min_runs: int = 3  # runs a measure needs before a check trusts it
    memory_tight: float = 0.8  # p99 peak over this share of the limit
    memory_idle: float = 0.3  # p99 peak under this share of a container limit
    killed_near: float = 0.9  # a killed run's peak over this share: killed for memory
    waits: float = 0.3  # a step taking this share of run time while waiting is worth tuning
    waiting_cpu: float = 0.2  # CPU share under which a step waits
    holds: float = 0.7  # last to finish in this share of shared loads holds the others up
    slower: float = 0.2  # a release whose median run is this much slower than the one before
    retried_runs: float = 0.5  # share of recent runs that retried a step

    @classmethod
    def resolved(cls) -> "Limits":
        try:
            return resolve(cls(), "dashboard", "checks")
        except ConfigurationError:
            return cls()


@dataclass(frozen=True, slots=True)
class Finding:
    check: str
    level: str  # "warning": costing runs, time, or money; "info": worth tuning
    area: str
    pipeline: str
    title: str  # the headline: "memory at 92% of its limit"
    evidence: str  # the measure, and how many runs
    action: str
    also: str = ""  # alternatives to the action


@dataclass(frozen=True, slots=True)
class Report:
    findings: list[Finding]
    skipped: list[tuple["Check", list[str]]]  # each check not run, and what it lacked


class Check(ABC):
    """One rule. Subclasses register themselves by being defined."""

    key: ClassVar[str]
    title: ClassVar[str]  # what it looks for, for the "not checked" list
    area: ClassVar[str]
    needs: ClassVar[tuple[str, ...]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        CHECKS.append(cls())

    def found(
        self, level: str, pipeline: str, title: str, evidence: str, action: str, also: str = ""
    ) -> Finding:
        return Finding(self.key, level, self.area, pipeline, title, evidence, action, also)

    def lacks(self, view: View) -> list[str]:
        """What it needs that the view does not hold, in words."""
        return sorted({NEEDS.get(need, need) for need in self.needs if not _has(view, need)})

    @abstractmethod
    def find(self, view: View, limits: Limits) -> Iterable[Finding]: ...


CHECKS: list[Check] = []


def run(view: View, limits: Limits | None = None) -> Report:
    """Every check whose data is in the view, worst first."""
    limits = limits or Limits.resolved()
    findings: list[Finding] = []
    skipped: list[tuple[Check, list[str]]] = []
    for check in CHECKS:
        if lacks := check.lacks(view):
            skipped.append((check, lacks))
        else:
            findings.extend(check.find(view, limits))
    findings.sort(key=lambda item: (item.level != "warning", AREAS.index(item.area), item.pipeline))
    return Report(findings, skipped)


def _has(view: View, need: str) -> bool:
    """Whether ``frame.column`` has any value in the view."""
    name, _, column = need.partition(".")
    frame = getattr(view, name, None)
    if not isinstance(frame, pl.DataFrame):
        frame = getattr(view.frames, name)
    return column in frame.columns and frame[column].drop_nulls().len() > 0


def _memory(view: View, limits: Limits) -> list[dict[str, Any]]:
    return [
        row
        for row in performance.memory(view).iter_rows(named=True)
        if row["share"] is not None and row["runs"] >= limits.min_runs
    ]


def _against(row: dict[str, Any]) -> str:
    return "its container limit" if row["limit_mb"] else "the host's memory"


# ---- memory --------------------------------------------------------------------------------


class MemoryTight(Check):
    key, title, area = "memory.tight", "runs close to their memory", "memory"
    needs = ("runs.peak_rss_mb", "runs.available_mb")

    def find(self, view: View, limits: Limits) -> Iterable[Finding]:
        for row in _memory(view, limits):
            if row["share"] >= limits.memory_tight:
                give = mb(row["recommended_mb"])
                yield self.found(
                    "warning",
                    row["pipeline_id"],
                    f"memory at {row['share']:.0%} of {_against(row)}",
                    f"{row['top']} peak {mb(row['top_mb'])} of {mb(row['available_mb'])}, "
                    f"{row['runs']} runs",
                    f"give it {give}" if row["limit_mb"] else f"run it with at least {give}",
                    "or chunk its largest resource",
                )


class MemoryIdle(Check):
    key, title, area = "memory.idle", "containers far bigger than needed", "memory"
    needs = ("runs.peak_rss_mb", "runs.memory_limit_mb")

    def find(self, view: View, limits: Limits) -> Iterable[Finding]:
        for row in _memory(view, limits):
            if row["limit_mb"] and row["share"] <= limits.memory_idle:
                yield self.found(
                    "info",
                    row["pipeline_id"],
                    f"memory {row['share']:.0%} used",
                    f"{row['top']} peak {mb(row['top_mb'])} of {mb(row['limit_mb'])}, "
                    f"{row['runs']} runs",
                    f"lower its limit to {mb(row['recommended_mb'])}",
                )


class KilledForMemory(Check):
    key, title, area = "memory.killed", "runs killed at their memory limit", "memory"
    needs = ("runs.peak_rss_mb", "runs.available_mb")

    def find(self, view: View, limits: Limits) -> Iterable[Finding]:
        killed = view.runs.filter(
            (pl.col("outcome") == "lost") & (pl.col("memory_share") >= limits.killed_near)
        )
        for key, own in killed.group_by("pipeline_id"):
            peaks = ", ".join(mb(value) for value in own["peak_rss_mb"].sort())
            limit = own["available_mb"].max()
            yield self.found(
                "warning",
                str(key[0]),
                f"{own.height} run(s) killed at their memory limit",
                f"peaks {peaks} of {mb(limit)}",  # type: ignore[arg-type]
                "give it more memory (see its peaks under Tuning)",
                "or chunk the resource it was loading",
            )


# ---- reliability ---------------------------------------------------------------------------


class RetriesRising(Check):
    key, title, area = "steps.retried", "steps retried in most recent runs", "reliability"
    needs = ("steps.attempt",)

    def find(self, view: View, limits: Limits) -> Iterable[Finding]:
        for row in performance.step_profile(view).iter_rows(named=True):
            if (
                row["runs"] >= limits.min_runs
                and row["retried_runs"] / row["runs"] >= limits.retried_runs
            ):
                yield self.found(
                    "warning",
                    row["pipeline_id"],
                    f"{row['step']} retried in {row['retried_runs']} of {row['runs']} runs",
                    "see the retried attempts under Tuning",
                    "find the transient cause upstream; raise retries only if it is expected",
                )


# ---- speed ---------------------------------------------------------------------------------


class SlowerRelease(Check):
    key, title, area = "release.slower", "a release that made runs slower", "speed"
    needs = ("runs.code_version",)

    def find(self, view: View, limits: Limits) -> Iterable[Finding]:
        releases = performance.by_version(view.mode("run"))
        for key, own in releases.group_by("pipeline_id", maintain_order=True):
            rows = own.filter(pl.col("runs") >= limits.min_runs).tail(2).to_dicts()
            if len(rows) < 2 or not rows[0]["p50_s"]:
                continue
            before, after = rows
            slower = after["p50_s"] / before["p50_s"] - 1
            if slower >= limits.slower:
                yield self.found(
                    "warning",
                    str(key[0]),
                    f"{after['code_version']} is {slower:.0%} slower",
                    f"median run {seconds(before['p50_s'])} on {before['code_version']} "
                    f"({before['runs']} runs), {seconds(after['p50_s'])} since "
                    f"({after['runs']} runs)",
                    f"compare {after['code_version']} with {before['code_version']}",
                )


class Waiting(Check):
    key, title, area = (
        "steps.waiting",
        "steps that mostly wait on upstream or the destination",
        "concurrency",
    )
    needs = ("steps.cpu_s",)

    def find(self, view: View, limits: Limits) -> Iterable[Finding]:
        workers = _workers(view)
        for row in performance.step_profile(view).iter_rows(named=True):
            if (
                row["step"] not in ("extract", "load")
                or (row["share"] or 0) < limits.waits
                or row["cpu_share"] is None
                or row["cpu_share"] > limits.waiting_cpu
                or row["runs"] < limits.min_runs
            ):
                continue
            setup = workers.get(row["pipeline_id"], {})
            evidence = (
                f"{row['share']:.0%} of run time, {row['cpu_share']:.0%} of it computing, "
                f"{row['runs']} runs"
            )
            if row["step"] == "extract":
                source = setup.get("source") or "<source>"
                action = (
                    f"raise [sources.{source}.extract] workers "
                    f"(now {setup.get('extract_workers') or '?'})"
                )
                also = "if upstream allows parallel reads; or look for a slow query upstream"
                title = "extract waits on upstream"
            else:
                action, also = (
                    "index the slowest table's merge key (see Tuning)",
                    "or merge it as delete-insert; on Postgres raise [load] workers",
                )
                title = "load waits on the destination"
            yield self.found("info", row["pipeline_id"], title, evidence, action, also)


class NormalizeBusy(Check):
    key, title, area = "steps.normalize", "normalize limited by CPU", "concurrency"
    needs = ("steps.cpu_s",)

    def find(self, view: View, limits: Limits) -> Iterable[Finding]:
        workers = _workers(view)
        for row in performance.step_profile(view).iter_rows(named=True):
            count = workers.get(row["pipeline_id"], {}).get("normalize_workers") or 1
            if (
                row["step"] == "normalize"
                and row["runs"] >= limits.min_runs
                and (row["share"] or 0) >= limits.waits
                and (row["cpu_share"] or 0) >= 0.8 * count
            ):
                yield self.found(
                    "info",
                    row["pipeline_id"],
                    "normalize is CPU-bound",
                    f"{row['share']:.0%} of run time, {row['cpu_share']:.1f} cores busy",
                    f"raise [normalize] workers (now {count})",
                    "if the host has idle cores",
                )


class FinishesLast(Check):
    key, title, area = "loads.last", "a resource that holds the others up", "concurrency"
    needs = ("loads.extract_first_at",)

    def find(self, view: View, limits: Limits) -> Iterable[Finding]:
        for row in performance.critical_path(view).iter_rows(named=True):
            if (
                row["loads"] >= limits.min_runs
                and row["last"] / row["loads"] >= limits.holds
                and (row["share"] or 0) >= 0.6
            ):
                yield self.found(
                    "info",
                    row["pipeline_id"],
                    f"{row['resource']} finishes last",
                    f"last to finish in {row['last']} of {row['loads']} loads",
                    f"give {row['resource']} a pipeline of its own",
                    "or chunk it, so the resources extracted with it commit sooner",
                )


def _workers(view: View) -> dict[str, dict[str, Any]]:
    """Each pipeline's workers as its latest run recorded them."""
    latest = (
        view.frames.pipelines.sort("at")
        .group_by("pipeline_id")
        .agg(pl.col("source", "extract_workers", "normalize_workers", "load_workers").last())
    )
    return {row.pop("pipeline_id"): row for row in latest.to_dicts()}
