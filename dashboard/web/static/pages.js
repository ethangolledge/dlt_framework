/* dlt//ops, part 3 of 3: the pages, by the question each answers, and the loop that draws them.

     overview   is ingestion working?          the verdict, health by day, what needs attention
     pipelines  which pipelines are there?     every pipeline: what it is, how it is doing
     pipeline   what is this pipeline doing?   what it reads and writes, its trend, its runs
     runs       which runs happened?           every run in range, filtered
     run        what happened in this run?     the error, steps, memory, and what it loaded
     backfills  what history is missing?       each missing range with its command
     incidents  what broke, and is it fixed?   open and resolved incidents, causes by day
     tuning     what should change?            the checks' findings and the evidence behind them

   A page is a function of the view (read from the URL) returning its sections; PAGES says which
   filters it takes and which tab it sits under. */

"use strict";

const details = {}; // run id -> its record in full, once loaded
const requested = new Set();

// ---- pieces more than one page shows ----------------------------------------------------------------

function runTable(list, { named = true, limit = 500 } = {}) {
  const columns = [
    { title: "started (UTC)", value: (r) => when(r.started), cls: () => "nowrap" },
    named && { title: "pipeline", value: (r) => r.pipeline, cls: () => "bright nowrap" },
    { title: "kind", value: (r) => r.mode, cls: (r) => (r.mode === "backfill" ? "info" : "muted") },
    { title: "outcome", value: outcomeBadge, cls: () => "nowrap" },
    { title: "took", value: (r) => took(r.seconds), num: true },
    { title: "rows", value: (r) => human(r.rows), num: true },
    { title: "peak memory", value: (r) => mb(r.peak_mb), num: true },
    { title: "cause", value: (r) => el("span", { tip: r.detail }, r.detail || ""), cls: () => "muted clip" },
  ].filter(Boolean);
  return [
    table(columns, list.slice(0, limit), { onRow: (r) => go("run", r.id), empty: "no runs in this range" }),
    list.length > limit ? note(`showing ${limit} of ${list.length}: narrow the days or pick a pipeline`) : null,
  ];
}

/** Runs as dots over time: how long each took, coloured by how it ended; a click opens one. */
function runsPlot(list, measure = (r) => r.seconds, format = took, extra = null) {
  const times = list.map((r) => Date.parse(r.started));
  const values = list.map(measure).filter((v) => v !== null && v !== undefined);
  const ySteps = format === took ? TIME_STEPS : format === mb ? MB_STEPS : null;
  return plot({ x: [Math.min(...times), Math.max(...times)], y: [0, Math.max(0, ...values)], yFormat: format, ySteps }, (svg, X, Y) => {
    if (extra) extra(svg, X, Y);
    dots(
      svg,
      X,
      Y,
      list
        .filter((r) => measure(r) !== null && measure(r) !== undefined)
        .map((r) => ({
          x: Date.parse(r.started),
          y: measure(r),
          token: LOOK.outcomes[look(r)][1],
          tip: `${when(r.started)} · ${LOOK.outcomes[look(r)][2]}\n${took(r.seconds)} · ${human(r.rows)} rows · ${mb(r.peak_mb)}${r.detail ? "\n" + r.detail : ""}`,
          onclick: () => go("run", r.id),
        })),
    );
  });
}

function incidentItem(incident) {
  const latest = runById[incident.latest];
  const causes = incident.causes.map(([name, count]) => (count > 1 ? `${name} ×${count}` : name)).join(", ");
  const span = incident.open
    ? `last ${plural(incident.runs.length, "run")} broke · none ok since ${when(incident.started)} · latest ${ago(latest && latest.started)}`
    : `${when(incident.started)} → ${when(incident.resolved)} · lasted ${took((Date.parse(incident.resolved) - Date.parse(incident.started)) / 1000)} · ${plural(incident.runs.length, "run")}`;
  return item(
    [
      el("span", { class: incident.open ? "error" : "ok" }, incident.open ? "✖ OPEN" : "✔ RESOLVED"),
      el("a", { href: link("pipeline", incident.pipeline) }, incident.pipeline),
      incident.mode === "backfill" ? el("span", { class: "info" }, "backfill") : null,
      el("span", { class: "muted" }, span),
    ],
    el("span", {}, causes),
    latest ? el("span", { class: "muted" }, "latest: ", el("a", { href: link("run", latest.id) }, when(latest.started)), " · ", latest.detail || "") : null,
    incident.open ? command(incident.rerun) : null,
  );
}

const KINDS = {
  gap: ["GAP", "error", "never loaded"],
  failed: ["FAILED", "error", "a backfill failed there"],
  hole: ["HOLE", "error", "an increment skipped it"],
  empty: ["EMPTY", "warn", "no rows between windows that had some"],
  off: ["OFF", "warn", "differs from the source"],
  reset: ["RESET", "warn", "the cursor went back"],
  twice: ["TWICE", "muted", "loaded more than once"],
};

function missingItem(m) {
  const [label, token, meaning] = KINDS[m.kind] || [m.kind, "muted", ""];
  return item(
    [el("span", { class: token, tip: meaning }, label), el("b", { class: "bright" }, `${edge(m.start)} → ${m.end ? edge(m.end) : "open"}`), el("span", { class: "muted" }, m.resource)],
    el("span", { class: "muted" }, m.why),
    m.command ? command(m.command) : null,
  );
}

function outcomeStrip(list) {
  const svg = sv("svg", { width: list.length * 7, height: 14 });
  list.forEach((run, index) =>
    svg.append(sv("rect", { x: index * 7, y: 1, width: 5, height: 12, fill: colour(LOOK.outcomes[look(run)][1]), tip: `${when(run.started)} ${LOOK.outcomes[look(run)][2]} ${took(run.seconds)}` })),
  );
  return svg;
}

// ---- overview ---------------------------------------------------------------------------------------

function overview(view) {
  const shown = runs.filter((run) => inRange(run, view));
  const counts = Object.fromEntries(STATE_ORDER.map((level) => [level, pipelines.filter((p) => p.state === level).length]));
  const risks = pipelines.flatMap((p) => p.concerns.filter((c) => c.level === "warning").map((c) => [p, c]));
  const open = D.incidents.filter((incident) => incident.open);
  return [
    el(
      "section",
      { class: "verdict" },
      el("span", { class: "subtle" }, "NOW"),
      ...STATE_ORDER.filter((level) => counts[level]).map((level) => {
        const [glyph, token, word] = LOOK.states[level];
        return el("span", { class: `big ${token}` }, `${glyph} ${counts[level]} ${word}`);
      }),
    ),
    note(
      D.lateness
        ? `judged over the last ${D.days} days · late means past a stated or regular schedule; pipelines run by hand are never late`
        : "lateness not judged: this environment states no [health] settings, so a pipeline that stopped running still reads as its last run",
    ),
    facts(
      el("span", { class: "subtle" }, "IN RANGE"),
      [shown.length, "runs"],
      [human(sum(shown, (r) => r.rows)), "rows processed"],
      [shown.filter((r) => r.outcome === "failed").length, "failed", "error"],
      [shown.filter((r) => r.outcome === "lost").length, "killed", "accent"],
      [shown.filter((r) => r.empty).length, "ok but loaded nothing", "muted"],
      [shown.filter((r) => r.mode === "backfill").length, "backfill runs"],
    ),
    panel(
      "Health by day",
      note("each day: its runs' outcomes, stacked, worst on top · brighter: more runs · ◤ a backfill ran · click a day for its runs"),
      healthMatrix(view, shown),
      outcomeLegend(),
    ),
    open.length ? panel("Open incidents", ...open.map(incidentItem)) : null,
    risks.length
      ? panel("At risk", ...risks.map(([p, c]) => item([el("a", { href: link("pipeline", p.id) }, p.id), el("span", { class: "warn" }, c.headline)], el("span", { class: "muted" }, c.detail))))
      : null,
  ];
}

const STACK = ["succeeded", "empty", "running", "retried", "failed", "lost"]; // bottom to top

function healthMatrix(view, shown) {
  const columns = dayColumns(view);
  const cells = {};
  for (const run of shown) {
    const index = columnOf(columns, run.started);
    if (index >= 0) (cells[`${run.pipeline}|${index}`] = cells[`${run.pipeline}|${index}`] || []).push(run);
  }
  const most = Math.max(1, ...Object.values(cells).map((list) => list.length));
  const rows = pipelines
    .filter((p) => matches(p.id, view.q))
    .map((p) => {
      const [glyph, token] = LOOK.states[p.state];
      return {
        label: p.id,
        glyph,
        token,
        tip: `${p.id}\n${p.headline}`,
        onclick: () => go("pipeline", p.id),
        right: p.last_success ? `ok ${ago(p.last_success)}` : "never ok",
        rightTip: "last successful run",
        cells: columns.map(([low, high], index) => {
          const list = cells[`${p.id}|${index}`];
          if (!list) return null;
          const count = {};
          for (const run of list) count[look(run)] = (count[look(run)] || 0) + 1;
          const backfills = list.filter((r) => r.mode === "backfill").length;
          return {
            stack: STACK.filter((key) => count[key]).map((key) => [LOOK.outcomes[key][1], count[key]]),
            strength: 0.45 + 0.55 * (Math.log(1 + list.length) / Math.log(1 + most)),
            tip: `${p.id} · ${low === high ? low : low + " → " + high}\n${plural(list.length, "run")}: ${STACK.filter((key) => count[key]).map((key) => `${count[key]} ${LOOK.outcomes[key][2]}`).join(", ")}${backfills ? `\n${backfills} backfill` : ""}`,
            onclick: () => go("runs", null, { pipeline: p.id, from: low, to: high }),
            mark: backfills > 0,
          };
        }),
      };
    });
  return matrix(columns, rows);
}

// ---- pipelines --------------------------------------------------------------------------------------

function reads(p) {
  const cursors = [...new Set(p.resources.map((r) => r.cursor).filter(Boolean))];
  return cursors.length ? `cursor ${cursors.join(", ")}` : "no cursor: all of it each run";
}

function pipelinesPage(view) {
  const shown = pipelines.filter((p) => matches(p.id, view.q));
  return [
    heading("Pipelines", el("span", { class: "muted" }, `${shown.length} of ${pipelines.length} · worst first`)),
    panel(
      null,
      table(
        [
          { title: "state", value: (p) => stateBadge(p.state), cls: () => "nowrap" },
          { title: "pipeline", value: (p) => p.id, cls: () => "bright" },
          { title: "from → to", value: (p) => `${p.source_type || "?"} → ${p.destination || "?"}${p.dataset ? " · " + p.dataset : ""}`, cls: () => "muted" },
          { title: "reads", value: (p) => `${plural(p.resources.length, "resource")} · ${reads(p)}`, cls: () => "muted" },
          { title: "last ok", value: (p) => ago(p.last_success), cls: () => "nowrap" },
          { title: "ok in range", value: (p) => okShare(p, view), num: true },
          { title: "latest 40 runs", value: (p) => outcomeStrip((runsOf[p.id] || []).slice(0, 40).reverse()) },
          { title: "now", value: (p) => (p.state === "ok" ? "" : p.headline), cls: (p) => LOOK.states[p.state][1] },
        ],
        shown,
        { onRow: (p) => go("pipeline", p.id), empty: "no pipeline matches" },
      ),
    ),
  ];
}

function okShare(p, view) {
  const mine = (runsOf[p.id] || []).filter((run) => inRange(run, view));
  return `${mine.filter((r) => r.outcome === "succeeded" || r.outcome === "retried").length}/${mine.length}`;
}

// ---- one pipeline -----------------------------------------------------------------------------------

function pipelinePage(view) {
  const p = pipelineById[view.id];
  if (!p) return [panel("Pipeline", note(`no pipeline "${view.id}"`, "empty"))];
  const mine = (runsOf[p.id] || []).filter((run) => inRange(run, view));
  const incident = D.incidents.find((i) => i.open && i.pipeline === p.id);
  const missing = D.missing.filter((m) => m.pipeline === p.id && m.fixable);
  const concerns = p.concerns.filter((c) => !(incident && c.level === "error" && c.headline.startsWith("failing")));
  return [
    heading(p.id, stateBadge(p.state), select(pipelines.map((other) => [other.id, other.id]), p.id, (id) => go("pipeline", id))),
    facts(
      [`${p.source_type || "?"} ${p.source || ""}`, `→ ${p.destination || "?"}${p.dataset ? " · " + p.dataset : ""}`],
      [ago(p.last_success), "last success"],
      [mine.length, "runs in range"],
      [human(sum(mine, (r) => r.rows)), "rows processed in range"],
    ),
    concerns.length || incident || missing.length
      ? panel(
          "Now",
          ...concerns.map((c) => item([el("span", { class: c.level === "error" ? "error" : "warn" }, c.headline)], el("span", { class: "muted" }, c.detail))),
          incident ? incidentItem(incident) : null,
          missing.length ? el("a", { href: link("backfills", null, { q: p.id }) }, `${missing.length} missing range(s): see Backfills`) : null,
        )
      : null,
    panel(
      "What it reads and writes",
      resourcesTable(p),
      p.tables.length
        ? table(
            [
              { title: "destination table", value: (t) => `${p.dataset || ""}.${t.table}` },
              { title: "rows processed", value: (t) => human(t.rows), num: true },
              { title: "last load", value: (t) => ago(t.last), cls: () => "muted" },
            ],
            p.tables,
          )
        : note("destination tables: none recorded (older monitoring records)"),
    ),
    trendPanel(mine, view),
    panel("Runs", runTable(mine, { named: false, limit: 200 })),
  ];
}

/** How far behind the source a time cursor was when its load ran, in seconds. */
function lag([at, value]) {
  const behind = (Date.parse(at) - Date.parse(value)) / 1000;
  return Number.isNaN(behind) ? null : behind;
}

function resourcesTable(p) {
  return table(
    [
      { title: "resource", value: (r) => r.name, cls: () => "bright" },
      { title: "write", value: (r) => r.write },
      { title: "reads", value: (r) => (r.cursor ? `${r.cursor} (${(r.cursor_kind || "").replace("Cursor", "").toLowerCase()})${r.cursor_start ? " from " + edge(r.cursor_start) : ""}` : "no cursor: all of it, each run"), cls: () => "muted" },
      { title: "cursor now", value: (r) => (r.cursor_path.length ? edge(r.cursor_path[r.cursor_path.length - 1][1]) : r.cursor ? "not recorded" : "") },
      { title: "lag at last load", value: (r) => (r.cursor_kind === "TimeCursor" && r.cursor_path.length ? took(lag(r.cursor_path[r.cursor_path.length - 1])) : ""), num: true },
      { title: "cursor over loads", value: cursorTrend },
      { title: "history (backfill)", value: (r) => (r.history ? `${edge(r.history[0])} → ${r.history[1] ? edge(r.history[1]) : "open"}` : "none declared"), cls: () => "muted" },
      { title: "if empty", value: (r) => r.empty },
      { title: "rows processed", value: (r) => human(r.rows), num: true },
      { title: "last load", value: (r) => ago(r.last_load), cls: () => "muted" },
    ],
    p.resources,
  );
}

/** A time cursor's lag over its loads (it should stay flat), or a value cursor's value (it should
    climb); only with three loads or more, as fewer show no trend. */
function cursorTrend(r) {
  if (r.cursor_path.length < 3) return r.cursor ? el("span", { class: "dim" }, `${r.cursor_path.length} load(s)`) : "";
  const time = r.cursor_kind === "TimeCursor";
  const values = r.cursor_path.map(time ? lag : ([, value]) => Number(value)).filter((v) => v !== null && !Number.isNaN(v));
  if (values.length < 3) return "";
  return el("span", { tip: time ? "lag behind the source at each load: flat is steady, rising is falling behind" : "cursor value at each load: it should climb" }, sparkline(values));
}

/** Duration, rows, and peak memory of each run, one chart each on the same time axis. */
function trendPanel(mine, view) {
  const kind = view.mode === "backfill" ? "backfill" : "run";
  const list = mine.filter((r) => r.mode === kind);
  if (!list.length) return panel("Trend", note(`no ${kind === "run" ? "incremental runs" : "backfills"} in this range`, "empty"));
  const limits = list.map((r) => r.limit && r.available_mb).filter(Boolean);
  const memoryLimit = limits.length ? Math.max(...limits) : null;
  const chart = (title, measure, format, extra) => el("div", {}, el("span", { class: "subtle" }, title), runsPlot(list, measure, format, extra));
  return panel(
    `Trend · ${kind === "run" ? "incremental runs" : "backfills"}`,
    note("each dot a run, coloured by how it ended; the same runs on the same time axis in each chart · click one to open it"),
    chart("how long it took", (r) => r.seconds, took),
    chart("rows processed", (r) => r.rows, human),
    chart(
      `peak memory${memoryLimit ? "" : " · no container limit recorded"}`,
      (r) => r.peak_mb,
      mb,
      memoryLimit ? (svg, X, Y) => rule(svg, X, Y, memoryLimit, `limit ${mb(memoryLimit)}`) : null,
    ),
    outcomeLegend(),
  );
}

// ---- runs ---------------------------------------------------------------------------------------

function runsPage(view) {
  const shown = runs.filter((run) => inRange(run, view) && (!view.pipeline || run.pipeline === view.pipeline) && (!view.outcome || look(run) === view.outcome));
  return [
    heading("Runs", el("span", { class: "muted" }, `${plural(shown.length, "run")} in range, newest first · open one to see it in full`)),
    shown.length ? panel("When they ran · how long each took", runsPlot(shown), outcomeLegend()) : null,
    panel(null, runTable(shown)),
  ];
}

// ---- one run ------------------------------------------------------------------------------------

window.DASH_RUN = (detail) => {
  details[detail.run] = detail;
  render();
};

function runPage(view) {
  const run = runById[view.id];
  if (!run) return [panel("Run", note(`no run "${view.id}"`, "empty"))];
  const detail = details[run.id];
  if (!detail && !requested.has(run.id)) {
    requested.add(run.id);
    const script = el("script", { src: `runs/${run.id.replace(/[^A-Za-z0-9_-]/g, "_")}.js` });
    script.addEventListener("error", () => window.DASH_RUN({ run: run.id, missing: true }));
    document.body.append(script);
  }
  const siblings = runsOf[run.pipeline] || [];
  const index = siblings.indexOf(run);
  const [glyph, token, word] = LOOK.outcomes[look(run)];
  return [
    heading(
      `${when(run.started)} UTC`,
      el("a", { href: link("pipeline", run.pipeline) }, run.pipeline),
      el("span", { class: run.mode === "backfill" ? "info" : "muted" }, run.mode === "backfill" ? "backfill" : "incremental run"),
      siblings[index - 1] ? el("a", { href: link("run", siblings[index - 1].id) }, "← newer") : null,
      siblings[index + 1] ? el("a", { href: link("run", siblings[index + 1].id) }, "older →") : null,
    ),
    el("section", { class: "verdict" }, el("span", { class: `big ${token}` }, `${glyph} ${word.toUpperCase()}`), run.detail ? el("span", { class: token }, run.detail) : null),
    broken(run) ? el("section", {}, el("span", { class: "muted" }, "fix the cause, then"), command(run.rerun)) : null,
    facts(
      [took(run.seconds), "took"],
      [human(run.rows), "rows"],
      [took(run.cpu_s), "cpu"],
      [mb(run.peak_mb), `peak memory${run.available_mb ? ` of ${mb(run.available_mb)} ${run.limit ? "limit" : "host"}` : ""}`],
      [run.version || "-", "code"],
      [run.host || "-", "host"],
      el("span", { class: "dim" }, run.id),
    ),
    detail ? runDetail(run, detail) : panel(null, note("loading this run…", "muted")),
  ];
}

function runDetail(run, detail) {
  if (detail.missing) return panel(null, note(`runs/${run.id}.js is missing: rebuild the dashboard`, "empty"));
  const lists = { steps: detail.steps, "memory samples": detail.samples, loads: detail.loads, tables: detail.tables };
  const absent = Object.keys(lists).filter((name) => !lists[name].length);
  return [
    errorPanel(run, detail.error),
    detail.steps.length || detail.samples.length > 1
      ? panel("Timeline", detail.steps.length ? stepsChart(run, detail.steps) : null, detail.samples.length > 1 ? memoryChart(run, detail) : null)
      : null,
    detail.loads.length ? panel("What it loaded", loadsTable(run, detail.loads)) : null,
    detail.tables.length
      ? panel(
          "Destination tables",
          table(
            [
              { title: "table", value: (t) => t.table },
              { title: "rows", value: (t) => human(t.rows), num: true },
              { title: "load time", value: (t) => took(t.seconds), num: true },
            ],
            detail.tables,
          ),
        )
      : null,
    absent.length ? note(`not recorded for this run: ${absent.join(", ")}. It stopped before them, or its monitoring predates them.`) : null,
  ];
}

function errorPanel(run, error) {
  if (run.outcome === "lost")
    return panel(
      "Killed",
      note(
        `No end was reported: the process was stopped from outside (out of memory, a stopped container, a killed shell). Its peak memory was ${mb(run.peak_mb)}${run.available_mb ? ` of ${mb(run.available_mb)}.` : ", with no limit recorded to compare."}`,
        "",
      ),
    );
  if (run.outcome !== "failed") return null;
  const rows = [
    ["cause", `${error.cause_type || ""}: ${error.message || ""}`],
    ["step", [error.step, error.resource].filter(Boolean).join(" · ")],
    ["window", error.window],
    ["input", error.notes],
    ["our code", error.where],
    ["raised in", error.origin],
    ["raised as", `${error.type || "-"}, exit code ${error.exit_code ?? "-"}`],
  ].filter(([, value]) => value);
  return panel(
    "Error",
    el("dl", {}, ...rows.flatMap(([name, value]) => [el("dt", {}, name), el("dd", {}, value)])),
    error.chain ? el("details", { open: "" }, el("summary", {}, "exception chain"), el("pre", {}, error.chain)) : null,
    error.traceback ? el("details", {}, el("summary", {}, "traceback"), el("pre", {}, error.traceback)) : null,
  );
}

function runSpan(run) {
  const start = Date.parse(run.started);
  return [start, Math.max(start + 1000, Date.parse(run.ended || run.started))];
}

const since = (start) => (ms) => `+${took((ms - start) / 1000)}`;

/** Each step attempt as a bar from its start to its end, coloured by whether it computed. */
function stepsChart(run, steps) {
  const [start, end] = runSpan(run);
  const lane = 20;
  const nature = (s) => {
    const busy = s.seconds ? s.cpu / s.seconds : 0;
    return s.outcome === "failed" ? "error" : busy >= 0.6 ? "accent" : busy <= 0.2 ? "info" : "warn";
  };
  const svg = plot(
    { x: [start, end], y: [0, steps.length], height: steps.length * lane + 28, left: 130, yTicks: 0, xFormat: since(start), xTicks: 5 },
    (svg, X, Y) =>
      steps.forEach((s, row) => {
        const y = Y(steps.length - row);
        svg.append(
          sv("text", { x: 0, y: y + 14 }, `${s.step} #${s.attempt}`),
          sv("rect", {
            x: X(Date.parse(s.started)),
            y: y + 3,
            width: Math.max(2, X(Date.parse(s.ended)) - X(Date.parse(s.started))),
            height: lane - 6,
            fill: colour(nature(s)),
            class: "bar",
            tip: `${s.step} attempt ${s.attempt}: ${took(s.seconds)}, ${pct(s.seconds ? s.cpu / s.seconds : 0)} CPU${s.cause ? "\n" + s.cause : ""}`,
          }),
        );
      }),
  );
  return el("div", {}, note("steps"), svg, legend([["accent", "computing"], ["info", "waiting on upstream or the destination"], ["warn", "mixed"], ["error", "failed"]]));
}

function memoryChart(run, detail) {
  const [start, end] = runSpan(run);
  const limit = detail.samples.map((s) => s[3]).find((v) => v) || null;
  const points = detail.samples.map(([at, rss, step]) => ({ x: Date.parse(at), y: rss, token: "ok", tip: `${at.slice(11, 19)} · ${mb(rss)}${step ? " · " + step : ""}` }));
  const svg = plot({ x: [start, end], y: [0, Math.max(limit || 0, ...points.map((p) => p.y)) * 1.05], left: 130, yFormat: mb, ySteps: MB_STEPS, xFormat: since(start), xTicks: 5 }, (svg, X, Y) => {
    line(svg, X, Y, points);
    dots(svg, X, Y, points, 0);
    if (limit) rule(svg, X, Y, limit, `limit ${mb(limit)}`);
  });
  return el("div", {}, note(`memory (rss), sampled · peak ${mb(run.peak_mb)}${limit ? "" : " · no container limit recorded"}`), svg);
}

function loadsTable(run, loads) {
  const declared = Object.fromEntries(((pipelineById[run.pipeline] || {}).resources || []).map((r) => [r.name, r]));
  const edges = (load) => (load.kind === "increment" ? load.cursor : load.window);
  const unrecorded = (load) => el("span", { class: "dim" }, load.kind !== "increment" || (declared[load.resource] || {}).cursor ? "not recorded" : "-");
  return table(
    [
      { title: "resource", value: (l) => l.resource, cls: () => "bright" },
      { title: "rows", value: (l) => (l.rows || 0).toLocaleString("en"), num: true },
      { title: "reads", value: (l) => (l.kind !== "increment" ? `${l.kind} window` : (declared[l.resource] || {}).cursor ? `cursor ${declared[l.resource].cursor}` : "no cursor: all of it"), cls: () => "muted" },
      { title: "from", value: (l) => (edges(l)[0] ? edge(edges(l)[0]) : unrecorded(l)) },
      { title: "to", value: (l) => (edges(l)[1] ? edge(edges(l)[1]) : unrecorded(l)) },
      { title: "extracted (UTC)", value: (l) => (l.extracted[0] ? `${l.extracted[0].slice(11, 19)} → ${(l.extracted[1] || "").slice(11, 19)}` : "-"), cls: () => "muted" },
    ],
    loads,
  );
}

// ---- backfills ----------------------------------------------------------------------------------

function backfillsPage(view) {
  const histories = D.histories.filter((h) => matches(h.resource, view.q));
  const declared = new Set(histories.map((h) => h.resource));
  const loose = D.missing.filter((m) => !declared.has(m.resource) && matches(m.resource, view.q));
  const jobs = runs.filter((run) => run.mode === "backfill" && inRange(run, { ...view, mode: "all" }));
  const missing = D.missing.filter((m) => m.fixable && matches(m.resource, view.q));
  if (!D.histories.length && !D.missing.length && !D.duplicates.length && !jobs.length)
    return [
      heading("Backfills"),
      panel(
        null,
        note("No resource declares a history, and no backfill has run in range.", ""),
        note("A resource declares one with backfill=TimeBackfill(start=..., every=...) or ValueBackfill(...). Then `pipeline backfill NAME` loads it window by window, and this page shows what is loaded and what is missing.", "muted"),
      ),
    ];
  const extra = Object.fromEntries(D.jobs.map((j) => [j.run, j]));
  return [
    heading("Backfills", el("span", { class: missing.length ? "error" : "ok" }, missing.length ? `✖ ${missing.length} range(s) missing` : "✔ nothing missing")),
    ...histories.map(historyPanel),
    loose.length ? panel("Incremental cursors", ...loose.map(missingItem)) : null,
    D.duplicates.length
      ? panel(
          "Duplicate keys",
          note("from each resource's latest `pipeline reconcile`: rows here whose key is not unique"),
          table(
            [
              { title: "resource", value: (d) => d.resource_id, cls: () => "bright" },
              { title: "duplicates", value: (d) => human(d.duplicates), num: true, cls: () => "warn" },
              { title: "of rows", value: (d) => human(d.destination_rows), num: true },
              { title: "reconciled", value: (d) => when(d.at), cls: () => "muted" },
            ],
            D.duplicates.filter((d) => matches(d.resource_id, view.q)),
          ),
        )
      : null,
    panel(
      "Backfill jobs in range",
      table(
        [
          { title: "started (UTC)", value: (r) => when(r.started) },
          { title: "pipeline", value: (r) => r.pipeline, cls: () => "bright" },
          { title: "outcome", value: outcomeBadge },
          { title: "windows", value: (r) => (extra[r.id] || {}).windows, num: true },
          { title: "rows", value: (r) => human(r.rows), num: true },
          { title: "took", value: (r) => took(r.seconds), num: true },
          { title: "resume with", value: (r) => ((extra[r.id] || {}).resume ? el("code", {}, extra[r.id].resume) : "") },
        ],
        jobs,
        { onRow: (r) => go("run", r.id), empty: "no backfill ran in this range" },
      ),
    ),
  ];
}

function historyPanel(h) {
  const w = width() - 40;
  const cell = w / h.cells.length;
  const ribbon = sv("svg", { width: w, height: 20 });
  h.cells.forEach((loaded, index) => ribbon.append(sv("rect", { x: index * cell, y: 2, width: Math.max(1, cell - 0.5), height: 16, fill: colour(loaded ? "ok" : "error"), "fill-opacity": loaded ? 0.8 : 0.6 })));
  const items = D.missing.filter((m) => m.resource === h.resource);
  return panel(
    `${h.resource} · ${h.window}`,
    facts(
      [`${edge(h.start)} → ${h.end ? edge(h.end) : "open"}`, "declared"],
      [h.fraction === null ? "open-ended" : pct(h.fraction), "loaded", items.some((m) => m.fixable) ? "warn" : "ok"],
    ),
    ribbon,
    legend([["ok", "loaded"], ["error", "missing"]]),
    items.length ? items.map(missingItem) : note("✔ nothing missing", "ok"),
  );
}

// ---- incidents ----------------------------------------------------------------------------------

function incidentsPage(view) {
  const overlaps = (i) => (i.resolved || D.generated).slice(0, 10) >= view.from && i.started.slice(0, 10) <= view.to;
  const kept = D.incidents.filter((i) => overlaps(i) && matches(i.pipeline, view.q) && (view.mode === "all" || i.mode === view.mode));
  const open = kept.filter((i) => i.open);
  const resolved = kept.filter((i) => !i.open);
  return [
    heading("Incidents", el("span", { class: open.length ? "error" : "ok" }, open.length ? `✖ ${open.length} open` : "✔ nothing open"), el("span", { class: "muted" }, `${resolved.length} resolved in range`)),
    note("An incident is a streak of failed or killed runs of one pipeline, from the first to the next success. It stays open until that success, then moves to resolved."),
    panel("Open", open.length ? open.map(incidentItem) : note("✔ no open incidents", "ok")),
    panel("Resolved", resolved.length ? resolved.map(incidentItem) : note("none in this range", "empty")),
    causesPanel(kept, view),
  ];
}

function causesPanel(kept, view) {
  const columns = dayColumns(view);
  const counts = {};
  for (const incident of kept)
    for (const id of incident.runs) {
      const run = runById[id];
      const index = run ? columnOf(columns, run.started) : -1;
      if (index < 0) continue;
      const cause = run.cause || "error";
      (counts[cause] = counts[cause] || columns.map(() => 0))[index] += 1;
    }
  const causes = Object.entries(counts).sort((a, b) => sum(b[1], Number) - sum(a[1], Number));
  if (!causes.length) return null;
  const most = Math.max(...causes.flatMap(([, list]) => list));
  const rows = causes.map(([cause, list]) => ({
    label: cause,
    right: String(sum(list, Number)),
    cells: list.map((count, index) => (count ? { stack: [["error", count]], strength: 0.35 + 0.65 * (count / most), tip: `${cause} · ${columns[index][0]}\n${plural(count, "run")}` } : null)),
  }));
  return panel("Causes by day", note("one cause on many days is one recurring bug; many causes once each are many problems"), matrix(columns, rows, { label: 260, right: 50 }));
}

// ---- tuning -------------------------------------------------------------------------------------

function tuningPage() {
  const share = (value) => meter(value, value >= 0.9 ? "error" : value >= 0.7 ? "warn" : "ok", pct(value));
  return [
    heading("Tuning", el("span", { class: "muted" }, `judged over the ${D.days} days to ${today}, incremental runs only`)),
    note("high: the p99 (memory) or p95 (time) once 20 runs or more are behind it; below that, the highest seen, as a percentile of a few runs is only the maximum"),
    panel(
      "Findings",
      D.findings.length
        ? D.findings.map((f) =>
            item(
              [el("span", { class: f.level === "warning" ? "warn" : "info" }, f.level === "warning" ? "⚠" : "●"), el("a", { href: link("pipeline", f.pipeline.split(", ")[0]) }, f.pipeline), el("b", { class: f.level === "warning" ? "warn" : "info" }, f.title)],
              el("span", { class: "muted" }, f.evidence),
              el("span", {}, "→ ", f.action, f.also ? el("span", { class: "muted" }, ` (${f.also})`) : null),
            ),
          )
        : note("✔ every check that could run found nothing to change", "ok"),
    ),
    D.memory.length
      ? panel(
          "Memory per pipeline",
          table(
            [
              { title: "pipeline", value: (r) => el("a", { href: link("pipeline", r.pipeline_id) }, r.pipeline_id) },
              { title: "runs", value: (r) => r.runs, num: true },
              { title: "p50", value: (r) => mb(r.p50_mb), num: true },
              { title: "high", value: (r) => `${mb(r.top_mb)} ${r.top}`, num: true },
              { title: "of", value: (r) => (r.available_mb ? `${mb(r.available_mb)} ${r.limit_mb ? "limit" : "host"}` : "no limit recorded"), cls: () => "muted" },
              { title: "share", value: (r) => (r.share === null ? "" : share(r.share)) },
              { title: "size it at", value: (r) => mb(r.recommended_mb), num: true },
            ],
            D.memory,
          ),
        )
      : null,
    D.steps.length
      ? panel(
          "Steps",
          table(
            [
              { title: "pipeline", value: (s) => s.pipeline_id },
              { title: "step", value: (s) => s.step },
              { title: "runs", value: (s) => s.runs, num: true },
              { title: "p50", value: (s) => took(s.p50_s), num: true },
              { title: "high", value: (s) => `${took(s.top_s)} ${s.top}`, num: true },
              { title: "share of run", value: (s) => meter(s.share, "info", pct(s.share)) },
              { title: "computing", value: (s) => pct(s.cpu_share), num: true },
            ],
            D.steps,
          ),
        )
      : null,
    D.slow_tables.length
      ? panel(
          "Slowest destination tables",
          table(
            [
              { title: "pipeline", value: (t) => t.pipeline_id },
              { title: "table", value: (t) => t.table, cls: () => "bright" },
              { title: "loads", value: (t) => t.loads, num: true },
              { title: "p50", value: (t) => took(t.p50_s), num: true },
              { title: "high", value: (t) => `${took(t.top_s)} ${t.top}`, num: true },
              { title: "rows a second", value: (t) => human(t.rows_per_s), num: true },
            ],
            D.slow_tables,
          ),
        )
      : null,
    D.unchecked.length
      ? panel(
          "Not checked",
          note("these checks need records this data does not hold (older monitoring records lack them; runs since record them)"),
          table(
            [
              { title: "check", value: (u) => u.title },
              { title: "needs", value: (u) => u.needs.join(", "), cls: () => "muted" },
            ],
            D.unchecked,
          ),
        )
      : null,
  ];
}

// ---- drawing ------------------------------------------------------------------------------------

const PAGES = {
  overview: { title: "Overview", draw: overview, filters: ["q"] },
  pipelines: { title: "Pipelines", draw: pipelinesPage, filters: ["q"] },
  pipeline: { under: "pipelines", draw: pipelinePage, filters: [] },
  runs: { title: "Runs", draw: runsPage, filters: ["pipeline", "outcome"] },
  run: { under: "runs", draw: runPage, filters: [] },
  backfills: { title: "Backfills", draw: backfillsPage, filters: ["q"] },
  incidents: { title: "Incidents", draw: incidentsPage, filters: ["q"] },
  tuning: { title: "Tuning", draw: tuningPage, filters: [] },
};

function render() {
  const view = read();
  const page = PAGES[view.route] || PAGES.overview;
  const tab = page.under || (PAGES[view.route] ? view.route : "overview");
  document.getElementById("nav").replaceChildren(
    ...Object.entries(PAGES)
      .filter(([, p]) => p.title)
      .map(([key, p]) => el("a", { href: link(key), class: key === tab ? "on" : "" }, p.title)),
  );
  document.getElementById("controls").replaceChildren(...controls(view, page.filters));
  document.getElementById("page").replaceChildren(...[page.draw(view)].flat(Infinity).filter(Boolean));
  document.title = `dlt//ops · ${view.route}${view.id ? " · " + view.id : ""}`;
}

let resizing;
window.addEventListener("hashchange", render);
window.addEventListener("resize", () => {
  clearTimeout(resizing);
  resizing = setTimeout(render, 150);
});
render();
