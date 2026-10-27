# Dashboard plan: mission control for the pipelines

**Status (2026-10-27): built**, in `dashboard/`, as below, except:

- The demo frames are generated from the framework's own records (`dashboard/demo.py`), not by
  running test pipelines: a week of runs with every pattern planted takes a second, is the same
  every time, and can include what a real run cannot be made to do on demand (an OOM kill at a
  chosen load, a release that slows one pipeline). Real runs' frames are tested too.
- `rich` is not used: the ANSI encoder is twenty lines of stdlib, so the `dashboard` extra is
  Streamlit alone.
- Added: `pipeline-dashboard export` writes every page as static, linked HTML for any web host.
- Found while building: normalize's worker processes were not counted in a step's CPU time, so a
  busy normalize read as waiting. `cpu_used` now includes finished child processes.
- The Streamlit frontend (`dashboard/app/web.py`) is written but untested until the `dashboard`
  extra is installed; its smoke test skips until then.

A terminal-styled, 90s-hacker dashboard over the monitoring frames: black, electric colours,
ASCII art. It answers three questions, in the order an engineer asks them:

1. **Is everything running?** (every morning, 30 seconds)
2. **What broke, and why?** (when something is red, 5 minutes)
3. **How do we make it faster, cheaper, and complete?** (weekly, or before scaling, 30 minutes)

It reads the monitoring frames and nothing else: no pipeline code, no destination, no dlt
state. So it runs anywhere the frames can be read (a laptop, a container, Streamlit hosting,
a bucket of Parquet files) and works for any pipeline and source type the framework runs.

## 1. How it will be used

| When | Who and why | Where they go | What they leave with |
| --- | --- | --- | --- |
| Every morning | "Did last night run?" | **Overview** | Green, or the one red pipeline to open |
| On an alert | `run_failed`, `run_lost`, `run_stale` | **Failures**, then **Run** | The resource, the input, the line raised at, the traceback, and the command to rerun or backfill |
| After a release | "Did it get slower?" | **Performance** (by `code_version`) | The step and table that changed |
| Weekly tuning | "Where is time and memory going?" | **Performance**, **Capacity**, **Advisor** | Concrete changes: workers, container memory, chunk size, a missing index |
| Before reporting on the data | "Is it all there, once?" | **Completeness** | Coverage, gaps with the backfill command, duplicate keys, cursor holes, source against destination |
| A slow job you cannot explain | Run once with `--profile` | **Profile** | The functions the time went to |

The rule for depth: the Overview never needs scrolling, and every number on it links to the
page that explains it. Detail (per step, per table, per function) appears only after a click.

## 2. Pages

All pages share a top bar (the time range, pipelines, mode, outcome, resource search, and a
UTC clock) and a bottom ticker of the latest events. Filters apply to every page and stay in
the URL, so a view can be shared.

### 0. Overview: mission control

```
 ███╗   ███╗██╗███████╗███████╗██╗ ██████╗ ███╗   ██╗   ┌─ RANGE 24h ─ PIPELINES all ─ MODE run ─┐
 ████╗ ████║██║██╔════╝██╔════╝██║██╔═══██╗████╗  ██║   │ 2026-10-27 06:00:13Z    ● LIVE  ▮      │
 ██╔████╔██║██║███████╗███████╗██║██║   ██║██╔██╗ ██║   └────────────────────────────────────────┘
┌─ RUNS ──────┐┌─ SUCCESS ───┐┌─ FAILED ────┐┌─ LOST ──────┐┌─ ROWS ──────┐┌─ OLDEST DATA ───┐
│    142      ││   97.2 %    ││     3  ▲2   ││     1       ││  12.4 M     ││  sec_form4 26h  │
│ ▁▂▃▅▇█▇▅▃▂▁ ││ ████████▌░  ││ ✖ sec_form4 ││ ☠ sec_ins.. ││ ▂▃▃▅▅▆█▇▆▅▄ ││  ⚠ late (2×)    │
└─────────────┘└─────────────┘└─────────────┘└─────────────┘└─────────────┘└─────────────────┘
┌─ RUN MATRIX ─ one cell an hour ─────────────────────────────────────────────────────────────┐
│ house_filings  █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █                                  │
│ sec_form4      █ █ █ █ █ ✖ ✖ ✖ · · · · · · · · · · · · · · · ·   ← late since 05:00            │
│ sec_insider    █ ▓ █ ☠ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █                                  │
│ LEGEND █ ok  ▓ ok after retries  ✖ failed  ☠ lost (killed)  ◌ running  · nothing ran        │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
┌─ HEALTH ─ pipeline health, now ───────────────┐┌─ PIPELINES ─────────────────────────────────┐
│ ✖ ERR  sec_form4    run_failed  3 in a row    ││ NAME           LAST  DUR ▁▃▅  ROWS ▁▃▅  MEM  │
│ ⚠ WRN  sec_insider  memory_high  peak 86%     ││ house_filings  ✔ 2m  ▂▂▃▂▂▂   ▅▅▅▅▆▅  31%  │
│ ⚠ WRN  sec_insider  coverage_gap 2 gaps       ││ sec_form4      ✖ 1h  ▂▃▂▂██   ▃▃▃▂··  12%  │
└───────────────────────────────────────────────┘└─────────────────────────────────────────────┘
 >> 06:00:13 sec_insider succeeded 4.4M rows (run 3f9f…) >> 05:58:40 sec_form4 failed in extract…
```

- **KPI cards**: runs, success rate, failed, lost, rows loaded, and the oldest data
  (worst freshness), each with a sparkline over the range and the change from the previous one.
- **Run matrix**: pipelines by time bucket (the bucket follows the range: 5 min to a day);
  each cell's glyph and colour is the worst outcome in it. Hover shows the runs; a click opens
  them.
- **Health**: the same signals as `pipeline health`, sorted error first.
- **Pipelines table**: last outcome and age, sparklines of duration and rows, peak memory share;
  sortable, and a click opens the pipeline on Performance.

### 1. Runs: what ran, when, and alongside what

- **Timeline (Gantt)**: one lane per pipeline, a bar per run from start to end, coloured by
  outcome; overlapping bars show what ran at the same time.
- **Concurrency strip** under it: how many runs were active, and their summed peak memory,
  against the host. It shows when runs pile up and where there is room to schedule more.
- **Runs table**: run id, pipeline, mode, start, duration, rows, outcome, exit code, code
  version, attempts; filterable, with a click through to the run.

### 2. Run: one run in full

```
┌─ RUN 3f9fe3f6… ── sec_insider ── run ── ✔ succeeded ── 6m12s ── 4.4M rows ── code 1.4.2 ────┐
│ STEP WATERFALL                         wall   cpu/wall                                       │
│ sync       ▕█▏                         0.8s   ░░░░░░░░░░  4%  waiting                         │
│ extract    ▕██████████████████▏        212s   ███▌░░░░░░ 35%  mostly waiting on upstream      │
│   ↳ try 1  ▕██▏ ✖ ConnectionError      14s                                                    │
│ normalize  ▕      ███████▏             81s    █████████▉ 99%  busy                            │
│ load       ▕             █████████▏    76s    █▏░░░░░░░░ 12%  waiting on the destination      │
├─ MEMORY ─ RSS against 5.0 GB limit ─ bands are steps ────────────────────────────────────────┤
│ 5.0G ┤······································································ limit      │
│ 3.5G ┤                     ⣀⣤⣶⣿⣿⣿⣶⣤⡀              ⢀⣤⣶⣿⣿⣷⣄                                   │
│ 0.5G ┤⣀⣀⣀⣠⣤⣤⣶⣶⣿⣿⣿⣿⣿⣿⣿⣿⡿⠟⠋      ⠙⠿⣿⣿⣿⣿⣿⣶⣤⣤⣤⣀⣀⠴⠋        ⠻⣿⣦⣀                               │
│      └ extract ────────────────┴ normalize ─┴ load ──────┘                                    │
├─ RESOURCES ─ own extract span inside the unit ───┬─ TABLES ─ load seconds ───────────────────┤
│ nonderiv_trans ▕████████████████████▏  7.3M      │ insider_nonderiv_trans   51s ████████████ │
│ submission     ▕██████▏                4.5M      │ insider_submission       29s ███████      │
│ footnotes      ▕▏                      0         │ insider_reportingowner   22s █████        │
└──────────────────────────────────────────────────┴───────────────────────────────────────────┘
```

- On failure the top shows the **error panel**: type, step, the resource that broke, the window,
  the input (notes), where raised (ours and origin), the cause chain, and a fold-out traceback,
  plus the command to run next (`pipeline run X`, `pipeline backfill X --missing`).
- A `lost` run shows its last samples and the step it died in.

### 3. Performance: where time goes

- **Duration trend** per pipeline (p50 and p95 bands), with `code_version` changes marked.
- **Step mix**: stacked step seconds per run; which step grew.
- **Busy or waiting**: each step as a dot, duration against CPU share. Bottom right is slow and
  waiting (upstream or destination); top right is slow and computing (our code, or dlt).
- **Throughput**: rows/s and MB/s per resource over time; a drop at constant volume means
  upstream or the destination got slower.
- **Critical path**: within each load, resources' own extract spans side by side; the one that
  ends last holds up the unit.
- **Slow tables**: `load_s` against rows per table; the outliers (a merge without an index, a
  huge staging table) are labelled.
- **Retries**: attempts per step over time, by cause.

### 4. Capacity: memory, CPU, and concurrency

- **Peak memory per run** against the container limit (or the host), with p95 and p99 lines.
- **Memory by step**: which step sets the peak.
- **Headroom heatmap**: pipelines by day, coloured by peak share.
- **CPU use** against cores (`cpu_count`): spare cores mean room for more workers.
- **Sizing card**: a recommended container memory (p99 peak × 1.25, rounded up), and how much
  is idle when p99 is far below the limit.

### 5. Completeness: is all the data there, exactly once

- **Backfill coverage ribbon** per resource across its declared history:
  `▕████████░░██████████▏ 92%  2 gaps`, each gap with its `pipeline backfill … --from --to`.
- **Cursor progress**: each increment's `[cursor_start_value, cursor_last_value]` on a line.
  A gap between one load's end and the next one's start is a **hole** (state jumped forward); a
  start before the previous end is a **re-read** (lag, or a reset, so duplicates under `Append`);
  a cursor that went backwards is a **reset**.
- **Reconciliation**: destination against source per window (`=` match, `▼` missing, `▲` extra),
  and duplicate keys per resource from `distinct_keys`.
- **Double loads**: the same backfill window loaded more than once (wasted work, and duplicates
  under `Append`).
- **Rows per window heatmap**: a window with 0 rows among full ones is suspect.
- **Empty and suspicious loads**: 0-row increments where the resource usually loads some.

### 6. Failures: patterns, not just the last one

- **Failures by cause** (`error_cause_type`) and by place (`error_where`) over time: one
  recurring bug or many problems?
- **Time to recover**: from first failure to the next success, per pipeline.
- **Lost runs** with their last memory and step.
- **Failure table** with the full error panel on click.

### 7. Profile: where a profiled run spent its time

- **Top functions per step** as bars of total and own share, our code highlighted, from
  `profiles`. Empty unless a run used `--profile`, and the page says how to make one.

### 8. Advisor: what to change, and why

Rules that turn the frames into recommendations, each with its evidence and how sure it is:

| Finding | Recommendation |
| --- | --- |
| Extract waiting (CPU share < 20%) on most runs, cores idle | More extract workers, or split the pipeline's resources across pipelines |
| Normalize busy at about `normalize workers × 100%` CPU, cores idle | More normalize workers |
| Load waiting, one table's `load_s` dominating | Look at that table's merge: its key and index, or `delete-insert` against `upsert` |
| One resource on the critical path of most loads | Give it its own pipeline, or `chunk` it |
| p99 peak memory above 70% of the limit | Raise the limit to the sizing card's figure, or `chunk` the largest resource |
| p99 peak below 30% of the limit | Lower the limit (cost) |
| Peak memory grows with increment rows | `chunk=N`, sized from rows per GB |
| Retries in most runs, by cause | The upstream or destination that keeps failing |
| Gaps or double loads | The exact `backfill` command |

## 3. Design

### Look

- **Black** (`#05070a`) with a faint grid and an optional CRT scanline overlay.
- **Electric palette** as tokens: phosphor green `#39ff14` (ok, primary), cyan `#00f0ff`
  (info), magenta `#ff2bd6` (accent, selection), amber `#ffb000` (warning), red `#ff3355`
  (error), dim `#1f6f3f` (grid, legends).
- **Monospace everywhere**, with box-drawing panels, block sparklines `▁▂▃▄▅▆▇█`, braille line
  charts `⣀⣤⣶⣿` (2×4 dots a character, for smooth curves), shade heatmaps `░▒▓█`, gauges
  `[████████░░]`, and a figlet banner per page.
- **Never colour alone**: every state has a glyph (`✔ ✖ ☠ ◌ ▓ ·`), so it reads in a plain
  terminal and to colour-blind eyes. Every chart has a legend and axis labels.

### Shape: three layers, one direction

```
dashboard/
  model/        frames → tidy facts and findings (pure Polars, no UI): runs, steps, loads,
                concurrency, capacity, completeness, failures, advisor, filters
  render/       ASCII primitives (panel, card, sparkline, braille chart, gantt, matrix,
                heatmap, gauge, table, legend) → styled text; encoders: ANSI and HTML
  pages/        each page composes model facts into render primitives; one module a page
  app/          frontends: streamlit.py (web, interactive), terminal.py (a page as ANSI text)
  theme.py      the palette and glyphs, in one place
```

- **Model** knows nothing about rendering; **render** knows nothing about the frames; **pages**
  join them; **app** only lays pages out and wires filters. An import-linter contract keeps it
  that way, and `dlt_framework` never imports `dashboard`.
- **One renderer, every surface**: a primitive produces lines of `(text, style)` segments. The
  HTML encoder makes styled `<span>`s for the web, the ANSI encoder colours a terminal. The same
  page renders in Streamlit, in a terminal (`pipeline-dashboard show overview --since 24h`, for
  SSH, cron, or pasting into an incident), or as static HTML.
- **Frontends are thin.** Streamlit is the first because it gives filters, page navigation, and
  hosting for free. Another frontend (Textual, a static export, a web framework) is one module
  in `app/`, reusing the model and the renderer.
- **Data comes through the framework's sinks** (`Sink.resolve(uri).read(Record)`), so it reads
  Parquet folders, buckets, or a database, whichever the monitoring URI names.

### Dirty data, handled once, in the model

- **Older record shapes**: missing columns read as null, and every fact works without them.
- **Duplicate records** (from an interrupted compaction): de-duplicated on each frame's key.
- **Open runs**: a `started` with no end is *running* while younger than the pipeline's p99
  duration, then *lost*, so a run killed after the last check still shows.
- **Clock and order**: every time is UTC; facts are ordered by time, never by file order.
- **Empty frames and filters**: a panel with nothing to show says `NO SIGNAL` and why, never
  a blank or a crash.
- **Sampling holes**: a gap in samples longer than three intervals is drawn as a break (the
  process stalled, or died), not interpolated.

### Framework changes first (small, in `core/monitoring`)

The dashboard needs the frames to describe themselves:

1. **`resources` records each declared history**: `backfill_start`, `backfill_end`,
   `backfill_every`, so coverage and gaps are computed without pipeline code.
2. **`pipelines` records how the run was configured to run in parallel**: extract, normalize,
   and load workers, and the DuckDB memory limit and threads. The advisor's concurrency advice
   needs them.
3. **`health()` and coverage work from the frames** (the `resources` dimension) instead of a
   `Pipeline` definition, so `pipeline health` and the dashboard share one implementation.

### Quality

- **Model**: tests on frames whose answers are known, as `test_health.py` does: holes,
  re-reads, resets, double loads, duplicates, open runs, older shapes, empty frames.
- **Render**: snapshot tests of primitives (the exact text), in both encoders.
- **Pages**: every page renders on empty, on a single run, and on the real frames from the
  copy-trades project.
- **App**: Streamlit's `AppTest` smoke test per page, with filters applied.
- The same gates as the framework: ruff, mypy, import-linter, coverage.

## 4. Order of work

1. Framework changes (§3): self-describing frames, and health from frames. Tested, on its own.
2. `model/` with its tests: every fact and finding above.
3. `render/` and `theme.py` with snapshot tests.
4. Overview, Run, and Failures pages, in Streamlit and the terminal. Daily use works here.
5. Performance, Capacity, Completeness, Profile, Advisor.
6. A demo data set, generated by running the framework's test pipelines (with a lost run, a
   failure, retries, a backfill with a gap, and duplicates), so the dashboard can be seen and
   tested without production data.

## 5. Decisions and downloads

- **Downloads (need your yes)**: `streamlit` (pip, into this repo's venv as a `dashboard` extra)
  and `rich` declared directly (already installed through dlt). Optional later: `textual` for an
  interactive terminal app. Fonts (JetBrains Mono, VT323) load from Google Fonts in the browser
  when the page is viewed.
- **Where it lives**: a top-level `dashboard/` package in this repo, installed with
  `pip install dlt-framework[dashboard]` and run with `pipeline-dashboard`.
- **Live refresh**: Streamlit re-reads the frames every 60 s on the Overview (configurable);
  other pages read on demand.
