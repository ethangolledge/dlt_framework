"""Frames to facts: one tidy table per question, every quirk of the raw frames handled here.

The raw frames are append-only events written as runs happen. Facts are what a reader wants:
one row per run with its outcome and duration, steps marked busy or waiting, loads with their
throughput. Dirty data is dealt with once, here:

- records written twice (an interrupted compaction) are de-duplicated on their keys;
- columns older records lack read as null, and every fact works without them;
- a run with no end is ``running`` while younger than its pipeline usually takes, then ``lost``;
- everything is ordered by time, never by the order files were read.
"""

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta

import polars as pl

from dlt_framework.core.monitoring import Frames

ENDED = ("succeeded", "failed", "lost")
OK = ("succeeded", "retried")
BROKEN = ("failed", "lost")
BUSY, WAITING = 0.6, 0.2  # CPU share of a step above which it computed, below which it waited
OPEN_GRACE = timedelta(hours=1)  # how long a run with no history may run before it is lost
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)  # where "all" starts, until the runs say otherwise
_RANK = {"lost": 0, "failed": 1, "retried": 2, "running": 3, "succeeded": 4}


def runs(frames: Frames, now: datetime) -> pl.DataFrame:
    """One row per run, newest first: identity, when, outcome, duration, rows, memory, errors.

    ``outcome`` is ``succeeded``, ``retried`` (succeeded after a step was retried), ``failed``,
    ``lost``, or ``running``; ``empty`` marks a success that loaded no rows. ``available_mb`` is
    the container limit, or the host's memory outside a container (unknown in older records);
    ``memory_share`` is the peak against it.
    """
    events = frames.runs.unique(["run_id", "event"], keep="last", maintain_order=True)
    if events.is_empty():
        return _empty_runs()
    identity = (
        events.sort("at")
        .group_by("run_id")
        .agg(
            pl.col("pipeline_id", "dlt_pipeline", "mode", "host", "code_version").first(),
            pl.col("cpu_count", "host_memory_mb").drop_nulls().first(),
            pl.col("at").filter(pl.col("event") == "started").first().alias("started_at"),
            pl.col("at").min().alias("first_at"),
        )
    )
    ended = (
        events.filter(pl.col("event").is_in(ENDED))
        .sort("at")
        .unique("run_id", keep="last")
        .select(
            "run_id",
            pl.col("event").alias("ended_as"),
            pl.col("at").alias("ended_at"),
            "rows",
            "duration_s",
            "exit_code",
            pl.col("^error_.*$"),
            "peak_rss_mb",
            "memory_limit_mb",
            "cpu_seconds",
        )
    )
    retried = (
        frames.steps.filter(pl.col("attempt") > 1)
        .select("run_id")
        .unique()
        .with_columns(retried=pl.lit(True))
    )
    seen = frames.samples.group_by("run_id").agg(
        seen_at=pl.col("at").max(),
        seen_peak_mb=pl.col("rss_mb").max(),
        seen_limit_mb=pl.col("memory_limit_mb").drop_nulls().last(),
        seen_host_mb=pl.col("system_total_mb").drop_nulls().max(),
    )
    table = (
        identity.join(ended, on="run_id", how="left")
        .join(retried, on="run_id", how="left")
        .join(seen, on="run_id", how="left")
        .with_columns(
            started_at=pl.coalesce("started_at", "first_at"),
            retried=pl.col("retried").fill_null(False),
        )
        .drop("first_at")
    )
    grace = _grace(table)
    table = table.join(grace, on="dlt_pipeline", how="left").with_columns(
        outcome=pl.when(pl.col("ended_as").is_not_null())
        .then(
            pl.when((pl.col("ended_as") == "succeeded") & pl.col("retried"))
            .then(pl.lit("retried"))
            .otherwise(pl.col("ended_as"))
        )
        .when(pl.lit(now) - pl.col("started_at") < pl.col("grace"))
        .then(pl.lit("running"))
        .otherwise(pl.lit("lost")),
    )
    return (
        table.with_columns(
            # A lost run's end is when it was last seen; its own record was written later.
            duration_s=pl.coalesce(
                "duration_s",
                pl.when(pl.col("outcome").is_in(["succeeded", "retried", "failed"])).then(
                    (pl.col("ended_at") - pl.col("started_at")).dt.total_seconds(fractional=True)
                ),
                pl.when(pl.col("outcome") == "running").then(
                    (pl.lit(now) - pl.col("started_at")).dt.total_seconds(fractional=True)
                ),
                (pl.col("seen_at") - pl.col("started_at")).dt.total_seconds(fractional=True),
            ),
            peak_rss_mb=pl.coalesce("peak_rss_mb", "seen_peak_mb"),
            memory_limit_mb=pl.coalesce("memory_limit_mb", "seen_limit_mb"),
            host_memory_mb=pl.coalesce("host_memory_mb", "seen_host_mb"),  # older records
            rank=pl.col("outcome").replace_strict(_RANK, return_dtype=pl.Int8),
        )
        .with_columns(
            ended_at=pl.when(pl.col("outcome").is_in(["lost", "running"]))
            .then(pl.col("started_at") + pl.duration(seconds=pl.col("duration_s").fill_null(0)))
            .otherwise(pl.col("ended_at")),
            available_mb=pl.coalesce("memory_limit_mb", "host_memory_mb"),
            memory_share=pl.col("peak_rss_mb") / pl.coalesce("memory_limit_mb", "host_memory_mb"),
            empty=pl.col("outcome").is_in(OK) & (pl.col("rows") == 0),
        )
        .drop("grace", "ended_as", "seen_at", "seen_peak_mb", "seen_limit_mb", "seen_host_mb")
        .sort("started_at", descending=True)
    )


def _grace(table: pl.DataFrame) -> pl.DataFrame:
    """How long each pipeline's runs may go before an unfinished one counts as lost: 1.5 times
    its p99 duration, and never less than ``OPEN_GRACE``."""
    floor = OPEN_GRACE.total_seconds()
    return (
        table.group_by("dlt_pipeline")
        .agg(p99=pl.col("duration_s").filter(pl.col("ended_as") == "succeeded").quantile(0.99))
        .select(
            "dlt_pipeline",
            grace=pl.duration(
                seconds=pl.max_horizontal(pl.col("p99").fill_null(0) * 1.5, pl.lit(floor))
            ),
        )
    )


def _empty_runs() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "run_id": pl.String,
            "pipeline_id": pl.String,
            "dlt_pipeline": pl.String,
            "mode": pl.String,
            "host": pl.String,
            "code_version": pl.String,
            "cpu_count": pl.Int64,
            "host_memory_mb": pl.Float64,
            "started_at": pl.Datetime("us", "UTC"),
            "ended_at": pl.Datetime("us", "UTC"),
            "outcome": pl.String,
            "rows": pl.Int64,
            "duration_s": pl.Float64,
            "exit_code": pl.Int64,
            "error_type": pl.String,
            "error_step": pl.String,
            "error_resource": pl.String,
            "error_window": pl.String,
            "error_cause_type": pl.String,
            "error_message": pl.String,
            "error_where": pl.String,
            "error_origin": pl.String,
            "error_chain": pl.String,
            "error_notes": pl.String,
            "error_traceback": pl.String,
            "peak_rss_mb": pl.Float64,
            "memory_limit_mb": pl.Float64,
            "cpu_seconds": pl.Float64,
            "retried": pl.Boolean,
            "available_mb": pl.Float64,
            "memory_share": pl.Float64,
            "rank": pl.Int8,
            "empty": pl.Boolean,
        }
    )


def steps(frames: Frames) -> pl.DataFrame:
    """Step attempts with ``cpu_share`` (CPU seconds over wall seconds) and ``nature``: ``busy``
    (computing), ``waiting`` (on upstream or the destination), or ``mixed``."""
    return (
        frames.steps.unique(["run_id", "step", "attempt", "started_at", "resources"], keep="last")
        .with_columns(
            cpu_share=pl.when(pl.col("duration_s") > 0).then(pl.col("cpu_s") / pl.col("duration_s"))
        )
        .with_columns(
            nature=pl.when(pl.col("cpu_share") >= BUSY)
            .then(pl.lit("busy"))
            .when(pl.col("cpu_share") <= WAITING)
            .then(pl.lit("waiting"))
            .otherwise(pl.lit("mixed"))
        )
        .sort("started_at")
    )


def loads(frames: Frames) -> pl.DataFrame:
    """Loads per resource with each resource's own extract seconds and rows a second."""
    return (
        frames.loads.unique(["load_id", "resource_id"], keep="last")
        .with_columns(
            resource=pl.col("resource_id").str.split_exact(".", 1).struct.field("field_1"),
            pipeline_id=pl.col("resource_id").str.split_exact(".", 1).struct.field("field_0"),
            extract_span_s=(
                pl.col("extract_last_at") - pl.col("extract_first_at")
            ).dt.total_seconds(fractional=True),
        )
        .with_columns(
            rows_per_s=pl.when(pl.col("extract_span_s") > 0).then(
                pl.col("rows") / pl.col("extract_span_s")
            )
        )
        .sort("at")
    )


def tables(frames: Frames) -> pl.DataFrame:
    """Destination tables per load, with rows a load second."""
    return (
        frames.tables.unique(["load_id", "table"], keep="last")
        .with_columns(
            rows_per_load_s=pl.when(pl.col("load_s") > 0).then(pl.col("rows") / pl.col("load_s"))
        )
        .sort("at")
    )


def samples(frames: Frames) -> pl.DataFrame:
    """Samples with ``pressure``: the share of memory in use, of the container's limit or of
    the host's memory outside one."""
    return (
        frames.samples.unique(["run_id", "at"], keep="last")
        .with_columns(
            pressure=pl.coalesce(
                pl.col("rss_mb") / pl.col("memory_limit_mb"),
                1 - pl.col("system_available_mb") / pl.col("system_total_mb"),
            )
        )
        .sort("at")
    )


@dataclass(frozen=True, slots=True)
class Scope:
    """What a reader is looking at: a time range and the pipelines and modes kept. An empty tuple
    keeps everything."""

    start: datetime
    end: datetime
    pipelines: tuple[str, ...] = ()
    modes: tuple[str, ...] = ()

    @classmethod
    def last(cls, text: str, now: datetime, **kept: tuple[str, ...]) -> "Scope":
        """``"24h"``, ``"7d"``, ``"90m"``, or ``"all"`` before ``now``."""
        if text == "all":
            return cls(EPOCH, now, **kept)
        amount, unit = int(text[:-1]), text[-1]
        units = {"m": "minutes", "h": "hours", "d": "days", "w": "weeks"}
        return cls(now - timedelta(**{units[unit]: amount}), now, **kept)

    def keep(self, runs: pl.DataFrame) -> pl.DataFrame:
        """Runs active in the range (started before its end, ended after its start) and picked."""
        return self.picked(
            runs.filter((pl.col("started_at") < self.end) & (pl.col("ended_at") >= self.start))
        )

    def picked(self, runs: pl.DataFrame) -> pl.DataFrame:
        """Runs of the pipelines and modes kept, at any time."""
        for column, values in (("pipeline_id", self.pipelines), ("mode", self.modes)):
            if values:
                runs = runs.filter(pl.col(column).is_in(list(values)))
        return runs


@dataclass(frozen=True)
class View:
    """Every fact for one scope, computed once and shared by every page.

    ``history`` holds all runs (trends and baselines need them); the other frames hold only
    the scope's runs.
    """

    scope: Scope
    now: datetime
    history: pl.DataFrame
    runs: pl.DataFrame
    steps: pl.DataFrame
    loads: pl.DataFrame
    tables: pl.DataFrame
    samples: pl.DataFrame
    frames: Frames = field(repr=False)

    @classmethod
    def build(cls, frames: Frames, scope: Scope, now: datetime) -> "View":
        history = runs(frames, now)
        if scope.start == EPOCH and history.height:  # "all": axes start at the first run
            scope = replace(
                scope, start=min(history.select(pl.col("started_at").min()).item(), now)
            )
        kept = scope.keep(history)
        return cls(
            scope=scope,
            now=now,
            history=history,
            runs=kept,
            steps=_of(steps(frames), kept),
            loads=_of(loads(frames), kept),
            tables=_of(tables(frames), kept),
            samples=_of(samples(frames), kept),
            frames=frames,
        )

    @property
    def pipelines(self) -> list[str]:
        return pipeline_names(self.frames)

    def only(self, pipeline: str) -> "View":
        """This view narrowed to one pipeline. ``frames`` stay whole, for facts that read them."""
        return self._narrowed(
            pl.col("pipeline_id") == pipeline, replace(self.scope, pipelines=(pipeline,))
        )

    def mode(self, mode: str) -> "View":
        """This view narrowed to incremental runs (``run``) or to backfills (``backfill``):
        two different processes, never summed together."""
        return self._narrowed(pl.col("mode") == mode, replace(self.scope, modes=(mode,)))

    def _narrowed(self, keep: pl.Expr, scope: "Scope") -> "View":
        runs = self.runs.filter(keep)
        return replace(
            self,
            scope=scope,
            history=self.history.filter(keep),
            runs=runs,
            steps=_of(self.steps, runs),
            loads=_of(self.loads, runs),
            tables=_of(self.tables, runs),
            samples=_of(self.samples, runs),
        )


def pipeline_names(frames: Frames) -> list[str]:
    """Every pipeline the frames know, sorted: the filter's options."""
    return sorted(
        set(frames.runs["pipeline_id"].drop_nulls()) | set(frames.pipelines["pipeline_id"])
    )


def _of(frame: pl.DataFrame, runs: pl.DataFrame) -> pl.DataFrame:
    """The rows of ``frame`` that belong to ``runs``."""
    return frame.filter(pl.col("run_id").is_in(runs["run_id"].implode()))
