# Dashboard design: from the data we have to what an engineer needs

Status: **built, 2026-10-28, as revised in section 0.** Replaces `dashboard-plan.md`.

## 0. Revision: pipelines and runs apart, in the browser

The five ASCII pages of section 4 still mixed two things, and drew them in a medium that could
not draw them well. Revised:

- **Pipelines and runs are different things, on different pages.** A pipeline is lasting: what
  it reads and writes, its resources and cursors, its health over time. A run is one event of
  it. *Pipelines* lists every pipeline; *Pipeline* shows one; *Runs* lists runs; *Run* shows
  one in full, to debug it.
- **The overview is a picture, not a table:** a heatmap of pipelines by day, each day its runs'
  outcomes stacked (a day that broke then recovered reads differently from one that only
  broke), brighter for more runs, with runs and rows per day beneath on the same columns. It
  scales to hundreds of pipelines, with a filter.
- **A browser app, not ASCII.** Hand-written JavaScript and SVG in the same black-and-neon
  look, fed a data snapshot from the Python model (`dashboard/web`). Its controls (the days,
  runs or backfills, search, a dropdown of pipelines) live in the page and in its URL; the old
  static ASCII export had none, which is why no date range could be seen. It loads nothing
  from elsewhere, and works opened from disk; serving it live can come later without changing
  the page.
- **A small command line** stays, for SSH and alerts: `status` (exits 1 while a pipeline
  fails) and `run ID`. The ASCII pages, and the Streamlit app that served them, are gone.
- Every page is checked in a headless browser by the tests, where one is installed.


## 1. What the data is

Nine frames, all append-only events from runs (`core/monitoring/records.py`):

| Frame | One row per | Says |
| --- | --- | --- |
| runs | run event (started, succeeded, failed) | outcome, duration, rows, peak memory, limit, the error in full |
| steps | step attempt | extract / normalize / load time, CPU, retries |
| loads | resource load (part) | rows, the window or cursor range it covered |
| tables | destination table load | rows, load time |
| samples | 1-second sample | memory and CPU through the run |
| resources | resource, per run | write mode, cursor, declared backfill history |
| pipelines | pipeline, per run | source, destination, workers |
| reconciliations | `pipeline reconcile` count | rows here against the source, distinct keys |
| profiles | profiled function | where a `--profile` run spent its time |

Two processes write them, and they are different things:

- **Runs** (`mode = run`, dlt pipeline `NAME`): the recurring production job. Its questions are
  about health over time: is it running, on time, getting slower, using more memory?
- **Backfills** (`mode = backfill`, dlt pipeline `NAME__backfill`): ad hoc jobs with a goal, to
  load a declared stretch of history window by window. Their questions are about progress:
  which windows are loaded, which are missing, and the command for each one missing.

What real data (copy_trades, 31 runs) looks like, and the design has to hold for:

- **Sparse.** Older records have no steps, tables, profiles, or reconciliations; host memory and
  the memory limit are null. Panels that need these must step aside, not show empty boxes.
- **Bursty.** Runs by hand: 13 runs of one pipeline inside an hour, then nothing for days. A
  fixed time axis squeezes them into three cells; a schedule derived from them says "expected
  every 39s" and calls the pipeline late.
- **Streaks with mixed causes.** sec_form4 broke four times: killed, config missing twice,
  then SIGTERM. Saying "failed 4 in a row: SignalReceivedException" is wrong.
- **No backfills yet**, no backfill declarations: that page must say how to start, not "NO SIGNAL".

## 2. What went wrong in the last two versions

1. **Organised by metric, not by question.** Topic pages (Speed, Capacity, Completeness) split
   one pipeline's story across many places, and a killed run on the overview led nowhere.
2. **Runs and backfills mixed** in one chart, one success rate, one list.
3. **Pipeline and run as separate pages**, when a run only makes sense inside its pipeline.
4. **No visible selection.** The pipeline page silently picked "the worst"; no way to switch.
5. **Gaps without detail.** "2 gaps" with no dates, so nothing to act on.
6. **Failures as a list of runs, not problems**: no open or resolved, no history, so it only
   ever fills up.
7. **Advice as one long procedure**: 28 branches and hard-coded thresholds, and already a
   special case so a failure isn't reported twice. Every new rule makes it worse, and it
   gives advice its evidence does not support (more memory, with no limit recorded).
8. **The demo shaped the design.** A generated week with every pattern planted hid all of the
   above. The demo generator ships inside the package.

## 3. Principles

1. **One question per page; the first line answers it.**
2. **Two lenses, never mixed:** runs (production health) and backfills (history coverage).
3. **Master and detail on one page.** A list of things, and the chosen one in full beneath it;
   the choice is always visible and changeable: a dropdown for pipelines (it scales to hundreds,
   where tabs do not), the pipeline named at the top, and a marked row for the run.
4. **Incidents are the unit of trouble.** An incident is a streak of failed or killed runs of
   one pipeline, from the first failure to the next success. It is open until a success
   follows, then resolved, so the open list clears itself and the resolved list is the history.
   Its causes are all listed, counted.
5. **Every claim shows its evidence and sample** ("p99 3.7G of 4.0G, 56 runs"). A check whose
   data is missing does not run; the page lists what was not checked and why.
6. **Checks are data, not code paths.** Each check is one small object in a registry: what it
   needs, its thresholds (defaults, overridable in config), and what it finds. Adding one is
   one class; the pages never branch on which check it is.
7. **Every actionable item carries the action**: the exact command, window, file and line.
8. **Axes fit the data**: a timeline spans the runs in range, and few runs get a cell each.
9. **Filters are global and shown**: the days (the last 14 unless a range is picked), pipeline,
   and mode, on a filter line at the top of every page, the same in a terminal and on the web.
10. **Say what each thing is.** A pipeline's page starts with what it is: its source and
   destination, and how each resource reads (its cursor, or none: all of it each run). A value
   that was not recorded says so, never a placeholder that reads as data.

## 4. Pages

Five pages. `Run` and `Pipeline` merge into **Runs**; **Backfills** is new; **Incidents**
replaces Failures; Speed, Capacity, and Profile become one **Tuning** page.

### Overview: is production OK?

Runs only (`mode = run`). One row per pipeline, worst first, and a line for backfills.

```
▓▒░ DLT//OPS ░▒▓  O V E R V I E W                        range all · runs · 6 pipelines
✖ 1 INCIDENT OPEN   ⚠ 1 AT RISK   █ 4 HEALTHY

PIPELINES          LAST RUN        LAST SUCCESS   RUNS     NOW
✖ sec_form4        ✖ 2d ago 10m    never          0/4      incident since 10-25 18:37 · killed, config ×2, SIGTERM
⚠ sec_insider      █ 2d ago 1m08s  2d ago         8/13     3 runs killed at 3.1–5.2G (no limit recorded)
█ house_ptr        █ 2d ago 0.3s   2d ago         5/5      ok
█ house_members    █ 2d ago 5.3s   2d ago         4/5      ok · resolved 10-25 18:33 after 24s
...
BACKFILLS  none run · no resource declares one                              → Backfills
```

### Runs: what is this pipeline doing, and what happened in this run?

```
PIPELINE  [sec_form4] sec_insider house_ptr house_members house_filings sec_insider_downloads
✖ incident open since 10-25 18:37 (4 runs) · last success never · schedule not stated

RUNS  newest first                                          duration ▁▁▁█  rows ▁▁▁▁
▶ 10-25 19:07  ✖ failed   10m34s     0 rows  331M  SIGTERM (signal 15)
  10-25 18:44  ✖ failed     0.2s     0 rows  266M  ConfigFieldMissingException in extract · form4
  10-25 18:44  ✖ failed     0.2s     0 rows  266M  ConfigFieldMissingException in extract · form4
  10-25 18:37  ☠ killed    5m00s     -       278M  no end reported; last seen in extract

RUN 10-25 19:07 ✖ FAILED  Received signal_code=15 (SIGTERM): stopped from outside
→ $ pipeline run sec_form4
 steps · memory · resources (rows, cursor from → to) · tables · error chain and traceback
 not recorded for this run: steps, tables (older monitoring records)
```

The run chosen by default is the latest one of the open incident, else the latest run; the
marker shows which. `show runs sec_form4 --run 46c7` picks another; on the web, a click.

### Backfills: what history is missing, and how do I fill it?

Per resource that declares a backfill: its declared range, a coverage ribbon, and a row per
gap with its dates, why it is missing, and the command. Then the backfill jobs.

```
orders_api.orders   2026-09-01 → 2026-10-28 by day   ▕████████░░██████████░░███▏ 91%
  GAP  2026-10-04 → 2026-10-05   1 window   failed in job 7c1e (HTTP 503)
       $ pipeline backfill orders_api --resource orders --from 2026-10-04 --to 2026-10-05
  GAP  2026-10-09 → 2026-10-10   1 window   loaded empty between full windows
       $ pipeline backfill orders_api --resource orders --from 2026-10-09 --to 2026-10-10
  HOLE 2026-10-25 04:00 → 06:00  increment cursor skipped ahead at 10:22
       $ pipeline backfill orders_api --resource orders --from 2026-10-25T04:00 --to ...T06:00
  OFF  2 windows differ from the source (reconcile 10-26): 10-12, 10-19
JOBS  started       windows    rows    outcome
      10-26 09:00   23 of 23   1.2M    █ done
      10-24 09:00   11 of 23   604K    ✖ failed at [10-04, 10-05) → resume command
```

With nothing declared, one panel explains how to declare a backfill.

### Incidents: what broke, is it still broken, and how often?

```
OPEN
✖ sec_form4    since 10-25 18:37  2d12h  4 runs  killed ×1, ConfigFieldMissing ×2, SIGTERM ×1
               latest: extract · form4 · core/config.py:88 → $ pipeline run sec_form4
RESOLVED (in range)
  sec_insider  10-25 19:41 → 20:22  41m  5 runs  killed ×3, OSError ×1, InvalidInput ×1
  house_members 10-25 18:32 → 18:33  24s  1 run  UnboundColumnException in normalize
CAUSES BY DAY  cause × day matrix: one recurring bug, or many?
```

Filtered by range, pipeline, mode, and open, resolved, or all.

### Tuning: what to change to make it faster, cheaper, complete?

The findings of the checks, by area, each with evidence, sample, and action; then the evidence
panels (memory sizing, steps busy or waiting, slow tables, releases, what runs at once,
profiles), each shown only when its data exists. Ends with **NOT CHECKED**: the checks that
did not run, and the data they need.

## 5. Checks, without a chain of ifs

```python
class Check(Protocol):
    key: str                      # "memory.tight"
    area: str                     # what it is about: memory, speed, completeness, ...
    needs: tuple[str, ...]        # frames and columns: ("runs.peak_rss_mb", "runs.memory_limit_mb")
    level: str                    # "warning" or "info"

    def find(self, view: View, settings: CheckSettings) -> pl.DataFrame: ...
    # one row per finding: pipeline, subject, evidence columns, sample size, action
```

- The registry runs the checks whose `needs` are present; the rest are listed as not checked.
- Thresholds live in `CheckSettings` with defaults, overridable per environment in
  `[dashboard.checks]` (and per pipeline), as `[health]` is. No numbers in page code.
- A finding's identity is `(check, pipeline, subject)`. Incidents and findings are separate
  things, so nothing has to be de-duplicated.
- A check states only what its evidence supports: a killed run is "probably memory" only
  when its peak was near a known limit; otherwise it is "killed, cause unknown".

## 6. The demo

The generator (`dashboard/demo.py`, the `--demo` flag) moves out of the package into the
tests, as small scenarios built for one behaviour each (a mixed-cause streak, a hand-run
burst, records with no steps, a backfill with a failed window), alongside the
real-run frames the tests already make. Pages are checked against the sparse, bursty shape of
real data, not a week designed to light up every panel.

## 7. Framework changes it implies

- **Health: lateness only against a real schedule**: a stated `expect_every_s`, or derived
  gaps that are regular, not a burst of hand-runs.
- Nothing else: incidents, checks, and coverage are all computed from the frames as they are.
