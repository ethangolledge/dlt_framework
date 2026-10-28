/* dlt//ops: the monitoring dashboard, in the browser.

   Pages, by the question each answers:
     overview   is ingestion healthy?           a heatmap of pipelines by day, runs and rows per day
     pipelines  which pipelines are there?      every pipeline: what it is, and how it is doing
     pipeline   what is this pipeline?          what it reads and writes, its runs, cursors, memory
     runs       which runs happened?            every run in range, filtered
     run        what happened in this run?      the error, steps, memory, and what it loaded
     backfills  what history is missing?        each missing range with its command
     incidents  what broke, and is it fixed?    open and resolved incidents, causes by day
     tuning     what should change?             the checks' findings, and what could not be checked

   The range of days, the mode (runs, backfills, or both), and what is selected live in the URL
   after the #, so any view can be bookmarked or shared. Everything is UTC.
   Data comes from data.js (window.DASH_DATA), and each run in full from runs/<id>.js. */

(function () {
  "use strict";

  const D = window.DASH_DATA;
  const NS = "http://www.w3.org/2000/svg";
  const DAY = 86400000;
  const RANK = { lost: 0, failed: 1, retried: 2, running: 3, succeeded: 4 };
  const STATE_RANK = { error: 0, warning: 1, ok: 2, idle: 3 };
  const STATES = {
    error: ["✖", "error", "FAILING"],
    warning: ["⚠", "warn", "AT RISK"],
    ok: ["█", "ok", "HEALTHY"],
    idle: ["·", "muted", "NO RECENT RUNS"],
  };
  const OUTCOMES = {
    succeeded: ["█", "ok", "ok"],
    retried: ["▓", "warn", "ok after retries"],
    failed: ["✖", "error", "failed"],
    lost: ["☠", "accent", "killed"],
    running: ["◌", "info", "running"],
  };
  const SIGNALS = {
    2: ["SIGINT", "interrupted, as by Ctrl-C"],
    9: ["SIGKILL", "killed outright, often by the out-of-memory killer"],
    15: ["SIGTERM", "told to stop from outside: a timeout, a deploy, or kill"],
  };
  const PAGES = [
    ["overview", "Overview"],
    ["pipelines", "Pipelines"],
    ["runs", "Runs"],
    ["backfills", "Backfills"],
    ["incidents", "Incidents"],
    ["tuning", "Tuning"],
  ];
  const details = {}; // run id -> its record in full, once loaded
  const requested = new Set(); // runs whose record has been asked for

  // ---- the data, indexed ------------------------------------------------------------------

  const pipelines = D.pipelines.slice().sort(byState);
  const pipelineById = Object.fromEntries(pipelines.map((p) => [p.id, p]));
  const runs = D.runs.slice().sort((a, b) => (a.started < b.started ? 1 : -1)); // newest first
  const runById = Object.fromEntries(runs.map((r) => [r.id, r]));
  const runsOf = {};
  for (const run of runs) (runsOf[run.pipeline] = runsOf[run.pipeline] || []).push(run);
  const today = D.generated.slice(0, 10);
  const firstDay = runs.length ? runs[runs.length - 1].started.slice(0, 10) : today;

  function byState(a, b) {
    return STATE_RANK[a.state] - STATE_RANK[b.state] || (a.id < b.id ? -1 : 1);
  }

  // ---- the URL: route, selection, and filters ---------------------------------------------

  function read() {
    const [path, query] = location.hash.replace(/^#\/?/, "").split("?");
    const [route, ...rest] = (path || "overview").split("/");
    const params = new URLSearchParams(query || "");
    return {
      route: route || "overview",
      id: rest.length ? decodeURIComponent(rest.join("/")) : null,
      from: params.get("from") || shift(today, -(D.days - 1)),
      to: params.get("to") || today,
      mode: params.get("mode") || "all",
      q: params.get("q") || "",
      pipeline: params.get("pipeline") || "",
      outcome: params.get("outcome") || "",
    };
  }

  function link(route, id, changes) {
    const now = read();
    const keep = { from: now.from, to: now.to, mode: now.mode, ...(changes || {}) };
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(keep)) if (value) params.set(key, value);
    const defaults = { from: shift(today, -(D.days - 1)), to: today, mode: "all" };
    for (const [key, value] of Object.entries(defaults)) if (params.get(key) === value) params.delete(key);
    const text = params.toString();
    return `#/${route}${id ? "/" + encodeURIComponent(id) : ""}${text ? "?" + text : ""}`;
  }

  function go(route, id, changes) {
    location.hash = link(route, id, changes);
  }

  // ---- building blocks --------------------------------------------------------------------

  function el(tag, props, ...kids) {
    const node = document.createElement(tag);
    apply(node, props);
    append(node, kids);
    return node;
  }

  function sv(tag, props, ...kids) {
    const node = document.createElementNS(NS, tag);
    apply(node, props);
    append(node, kids);
    return node;
  }

  function apply(node, props) {
    for (const [key, value] of Object.entries(props || {})) {
      if (value === null || value === undefined || value === false) continue;
      if (key === "class") node.setAttribute("class", value);
      else if (key === "tip") tip(node, value);
      else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
      else node.setAttribute(key, value);
    }
  }

  function append(node, kids) {
    for (const kid of kids.flat(Infinity)) {
      if (kid === null || kid === undefined || kid === false) continue;
      node.append(kid instanceof Node ? kid : String(kid));
    }
  }

  function panel(title, ...kids) {
    return el("section", { class: "panel" }, title ? el("h2", {}, title) : null, ...kids);
  }

  function tip(node, text) {
    const box = document.getElementById("tip");
    node.addEventListener("mousemove", (event) => {
      box.textContent = typeof text === "function" ? text() : text;
      box.hidden = false;
      const x = Math.min(event.clientX + 14, window.innerWidth - box.offsetWidth - 8);
      const y = Math.min(event.clientY + 14, window.innerHeight - box.offsetHeight - 8);
      box.style.left = `${x}px`;
      box.style.top = `${y}px`;
    });
    node.addEventListener("mouseleave", () => (box.hidden = true));
  }

  function command(text) {
    const button = el("button", { class: "copy", type: "button" }, "copy");
    button.addEventListener("click", () => {
      const done = () => (button.textContent = "copied");
      if (navigator.clipboard) navigator.clipboard.writeText(text).then(done, () => (button.textContent = "select it"));
    });
    return el("div", { class: "cmdline" }, el("code", {}, "$ " + text), button);
  }

  function colour(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(`--${name}`).trim();
  }

  function state(level) {
    const [glyph, style, word] = STATES[level] || STATES.idle;
    return el("span", { class: `state ${style}` }, `${glyph} ${word}`);
  }

  function outcome(name) {
    const [glyph, style, word] = OUTCOMES[name] || ["·", "muted", name];
    return el("span", { class: style }, `${glyph} ${word}`);
  }

  function width() {
    // the inside of a panel: the page, less its padding and the panel's
    return Math.max(480, document.getElementById("page").clientWidth - 64);
  }

  // ---- numbers and times ------------------------------------------------------------------

  function human(value) {
    if (value === null || value === undefined) return "-";
    for (const [size, unit] of [[1e9, "B"], [1e6, "M"], [1e3, "K"]])
      if (Math.abs(value) >= size) return (value / size).toFixed(1) + unit;
    return String(Math.round(value));
  }

  function took(seconds) {
    if (seconds === null || seconds === undefined) return "-";
    const whole = Math.floor(seconds);
    if (whole >= 86400) return `${Math.floor(whole / 86400)}d ${Math.floor((whole % 86400) / 3600)}h`;
    if (whole >= 3600) return `${Math.floor(whole / 3600)}h${String(Math.floor((whole % 3600) / 60)).padStart(2, "0")}m`;
    if (whole >= 60) return `${Math.floor(whole / 60)}m${String(whole % 60).padStart(2, "0")}s`;
    return seconds < 10 ? seconds.toFixed(1) + "s" : whole + "s";
  }

  function mb(value) {
    if (value === null || value === undefined) return "-";
    if (value < 1024) return Math.round(value) + "M";
    return (value / 1024).toFixed(2).replace(/0+$/, "").replace(/\.$/, "") + "G";
  }

  function when(iso) {
    return iso ? iso.slice(0, 16).replace("T", " ") : "-";
  }

  function ago(iso) {
    if (!iso) return "never";
    const seconds = (Date.parse(D.generated) - Date.parse(iso)) / 1000;
    for (const [size, unit] of [[86400, "d"], [3600, "h"], [60, "m"]])
      if (seconds >= size) return `${Math.floor(seconds / size)}${unit} ago`;
    return "just now";
  }

  function shift(day, days) {
    return new Date(Date.parse(day + "T00:00:00Z") + days * DAY).toISOString().slice(0, 10);
  }

  function days(from, to) {
    const out = [];
    for (let day = from; day <= to && out.length < 4000; day = shift(day, 1)) out.push(day);
    return out;
  }

  function edge(text) {
    if (!text) return "-";
    const moment = Date.parse(text);
    return Number.isNaN(moment) || !/\d{4}-\d{2}-\d{2}/.test(text) ? text : when(new Date(moment).toISOString());
  }

  // ---- what is in range -------------------------------------------------------------------

  function inRange(run, view) {
    const day = run.started.slice(0, 10);
    if (day < view.from || day > view.to) return false;
    if (view.mode === "run" && run.mode !== "run") return false;
    if (view.mode === "backfill" && run.mode !== "backfill") return false;
    return true;
  }

  function matches(pipeline, q) {
    return !q || pipeline.id.toLowerCase().includes(q.toLowerCase());
  }

  // ---- the frame: navigation and the controls every page shares ----------------------------

  function frame(view) {
    const nav = document.getElementById("nav");
    nav.replaceChildren(
      ...PAGES.map(([key, title]) =>
        el("a", { href: link(key), class: view.route === key || (key === "pipelines" && view.route === "pipeline") || (key === "runs" && view.route === "run") ? "on" : "" }, title),
      ),
    );
    const span = days(view.from, view.to).length;
    const preset = (label, from) =>
      el(
        "button",
        { type: "button", class: from === view.from && view.to === today ? "on" : "", onclick: () => go(view.route, view.id, { from, to: today }) },
        label,
      );
    const date = (name, value) =>
      el("input", {
        type: "date",
        value,
        min: firstDay,
        max: today,
        onchange: (event) => event.target.value && go(view.route, view.id, { [name]: event.target.value }),
      });
    document.getElementById("controls").replaceChildren(
      el("label", {}, "days", date("from", view.from), "→", date("to", view.to)),
      preset("7d", shift(today, -6)),
      preset("14d", shift(today, -13)),
      preset("30d", shift(today, -29)),
      preset("all", firstDay < shift(today, -29) ? firstDay : shift(today, -89)),
      el(
        "label",
        {},
        "showing",
        el(
          "select",
          { onchange: (event) => go(view.route, view.id, { mode: event.target.value }) },
          ...[["all", "runs and backfills"], ["run", "runs only"], ["backfill", "backfills only"]].map(([value, text]) =>
            el("option", { value, ...(value === view.mode ? { selected: "" } : {}) }, text),
          ),
        ),
      ),
      el("span", { class: "range" }, `${view.from} → ${view.to} UTC · ${span} day${span === 1 ? "" : "s"}`),
    );
  }

  // ---- overview -----------------------------------------------------------------------------

  function overview(view) {
    const counts = { error: 0, warning: 0, ok: 0, idle: 0 };
    for (const p of pipelines) counts[p.state] += 1;
    const shown = runs.filter((run) => inRange(run, view));
    const broken = shown.filter((run) => run.outcome === "failed").length;
    const killed = shown.filter((run) => run.outcome === "lost").length;
    const backfills = shown.filter((run) => run.mode === "backfill").length;
    const rows = shown.reduce((sum, run) => sum + (run.rows || 0), 0);
    const open = D.incidents.filter((item) => item.open);
    const risks = pipelines.flatMap((p) => p.concerns.filter((c) => c.level === "warning").map((c) => [p, c]));
    const search = el("input", {
      type: "search",
      placeholder: "filter pipelines",
      value: view.q,
      onchange: (event) => go("overview", null, { q: event.target.value }),
    });
    return [
      el(
        "section",
        { class: "verdict" },
        el("span", { class: "subtle" }, "NOW"),
        ...Object.entries(counts)
          .filter(([, count]) => count)
          .map(([level, count]) => el("span", { class: `big ${STATES[level][1]}` }, `${STATES[level][0]} ${count} ${STATES[level][2]}`)),
      ),
      el(
        "section",
        { class: "facts" },
        el("span", { class: "subtle" }, "IN RANGE"),
        el("span", {}, el("b", {}, shown.length), " runs"),
        el("span", {}, el("b", {}, human(rows)), " rows"),
        el("span", { class: broken ? "error" : "" }, el("b", {}, broken), " failed"),
        el("span", { class: killed ? "accent" : "" }, el("b", {}, killed), " killed"),
        el("span", {}, el("b", {}, backfills), " backfill runs"),
      ),
      panel(
        "Health by day",
        el("div", { class: "cmdline" }, search, el("span", { class: "subtle" }, "each day: its runs' outcomes, stacked · brighter: more runs · ◤ a backfill ran · click a day for its runs")),
        el("div", { class: "scroll" }, heatmap(view, shown)),
        legend(),
      ),
      open.length ? panel("Open incidents", ...open.map(incidentItem)) : null,
      risks.length
        ? panel(
            "At risk",
            ...risks.map(([p, c]) =>
              el("div", { class: "item" }, el("div", { class: "head" }, el("a", { href: link("pipeline", p.id) }, p.id), el("span", { class: "warn" }, c.headline)), el("span", { class: "muted" }, c.detail)),
            ),
          )
        : null,
    ];
  }

  function legend() {
    return el(
      "div",
      { class: "legend" },
      ...Object.entries(OUTCOMES).map(([, [, style, word]]) => el("span", {}, el("i", { style: `background: var(--${style})` }), word)),
      el("span", {}, el("i", { style: "background: var(--dim)" }), "no run"),
    );
  }

  function buckets(view) {
    const all = days(view.from, view.to);
    const step = Math.max(1, Math.ceil(all.length / 120)); // a column a day, up to 120 columns
    const out = [];
    for (let index = 0; index < all.length; index += step) out.push([all[index], all[Math.min(all.length - 1, index + step - 1)]]);
    return out;
  }

  function heatmap(view, shown) {
    const columns = buckets(view);
    const rows = pipelines.filter((p) => matches(p, view.q));
    const label = 250;
    const right = 90;
    const lane = 18;
    const cell = Math.max(8, Math.min(44, Math.floor((width() - label - right) / columns.length)));
    const top = 18;
    const chart = 64;
    const height = top + rows.length * lane + 10 + chart + 20 + chart + 30;
    const svg = sv("svg", { width: label + columns.length * cell + right, height });
    const at = (index) => label + index * cell;
    const cellOf = {};
    for (const run of shown) {
      const index = columns.findIndex(([low, high]) => run.started.slice(0, 10) >= low && run.started.slice(0, 10) <= high);
      if (index < 0) continue;
      const key = `${run.pipeline}|${index}`;
      (cellOf[key] = cellOf[key] || []).push(run);
    }
    const most = Math.max(1, ...Object.values(cellOf).map((list) => list.length));
    const every = Math.ceil(columns.length / Math.floor((columns.length * cell) / 52));
    columns.forEach(([low], index) => {
      if (index % every === 0) svg.append(sv("text", { x: at(index), y: 11 }, low.slice(5)));
    });
    rows.forEach((p, row) => {
      const y = top + row * lane;
      const [glyph, style] = STATES[p.state];
      svg.append(
        sv("text", { x: 0, y: y + 12, class: "label", style: `fill: var(--${style})`, onclick: () => go("pipeline", p.id) }, glyph),
        sv("text", { x: 16, y: y + 12, class: "label", onclick: () => go("pipeline", p.id), tip: `${p.id}\n${p.headline}` }, p.id.length > 32 ? p.id.slice(0, 31) + "…" : p.id),
      );
      columns.forEach(([low, high], index) => {
        const list = cellOf[`${p.id}|${index}`];
        if (!list) {
          svg.append(sv("rect", { x: at(index) + cell / 2 - 1, y: y + lane / 2 - 1, width: 2, height: 2, fill: colour("dim") }));
          return;
        }
        const strength = 0.45 + 0.55 * (Math.log(1 + list.length) / Math.log(1 + most));
        const count = list.reduce((t, r) => ((t[r.outcome] = (t[r.outcome] || 0) + 1), t), {});
        const tally = Object.entries(count)
          .map(([name, n]) => `${n} ${OUTCOMES[name][2]}`)
          .join(", ");
        const filled = list.filter((r) => r.mode === "backfill").length;
        const target = { class: "cell", tip: `${p.id} · ${low === high ? low : low + " → " + high}\n${list.length} run(s): ${tally}${filled ? `\n${filled} backfill run(s)` : ""}`, onclick: () => go("runs", null, { pipeline: p.id, from: low, to: high }) };
        // a stack of the day's outcomes, worst on top, so a day that broke and recovered
        // reads differently from a day that only broke
        let base = y + lane - 2;
        for (const name of ["succeeded", "running", "retried", "failed", "lost"]) {
          if (!count[name]) continue;
          const h = ((lane - 4) * count[name]) / list.length;
          base -= h;
          svg.append(sv("rect", { x: at(index) + 1, y: base, width: cell - 2, height: h, fill: colour(OUTCOMES[name][1]), "fill-opacity": strength.toFixed(2), "pointer-events": "none" }));
        }
        svg.append(sv("rect", { x: at(index) + 1, y: y + 2, width: cell - 2, height: lane - 4, fill: "transparent", ...target }));
        if (filled) svg.append(sv("path", { d: `M${at(index) + 1} ${y + 2} h6 l-6 6 z`, fill: colour("info"), "pointer-events": "none" }));
      });
      svg.append(sv("text", { x: at(columns.length) + 8, y: y + 12, tip: "last successful run" }, p.last_success ? ago(p.last_success) : "never ok"));
    });
    if (!rows.length) svg.append(sv("text", { x: label, y: top + 12 }, "no pipeline matches"));
    let y = top + rows.length * lane + 10;
    stacked(svg, columns, shown, y, chart, at, cell, "runs per day", (run) => 1, (run) => (run.mode === "backfill" ? "backfill" : run.outcome));
    y += chart + 20;
    stacked(svg, columns, shown, y, chart, at, cell, "rows loaded", (run) => run.rows || 0, () => "rows");
    y += chart + 14;
    columns.forEach(([low], index) => {
      if (index % every === 0) svg.append(sv("text", { x: at(index), y: y + 6 }, low.slice(5)));
    });
    return svg;
  }

  function stacked(svg, columns, shown, y, height, at, cell, title, measure, series) {
    const order = ["succeeded", "retried", "running", "failed", "lost", "backfill", "rows"];
    const tones = { succeeded: "ok", retried: "warn", running: "info", failed: "error", lost: "accent", backfill: "info", rows: "info" };
    const totals = columns.map(() => ({}));
    for (const run of shown) {
      const index = columns.findIndex(([low, high]) => run.started.slice(0, 10) >= low && run.started.slice(0, 10) <= high);
      if (index < 0) continue;
      const key = series(run);
      totals[index][key] = (totals[index][key] || 0) + measure(run);
    }
    const top = Math.max(1, ...totals.map((t) => Object.values(t).reduce((a, b) => a + b, 0)));
    svg.append(
      sv("text", { x: 16, y: y + 12 }, title),
      sv("text", { x: 16, y: y + 26, class: "dim" }, `max ${human(top)}`),
      sv("line", { x1: at(0), x2: at(columns.length), y1: y + height, y2: y + height, class: "axis" }),
    );
    totals.forEach((counts, index) => {
      let base = y + height;
      for (const key of order) {
        if (!counts[key]) continue;
        const h = Math.max(1, (counts[key] / top) * (height - 4));
        base -= h;
        svg.append(
          sv("rect", {
            x: at(index) + 2,
            y: base,
            width: Math.max(2, cell - 4),
            height: h,
            class: "bar",
            fill: colour(tones[key]),
            "fill-opacity": key === "backfill" ? "0.45" : "0.9",
            tip: `${columns[index][0]}\n${human(counts[key])} ${key === "rows" ? "rows" : key === "backfill" ? "backfill runs" : (OUTCOMES[key] || [0, 0, key])[2]}`,
          }),
        );
      }
    });
  }

  function incidentItem(item) {
    const latest = runById[item.latest];
    const causes = item.causes.map(([name, count]) => (count > 1 ? `${name} ×${count}` : name)).join(", ");
    return el(
      "div",
      { class: "item" },
      el(
        "div",
        { class: "head" },
        el("span", { class: item.open ? "error" : "ok" }, item.open ? "✖ OPEN" : "✔ RESOLVED"),
        el("a", { href: link("pipeline", item.pipeline) }, item.pipeline),
        item.mode === "backfill" ? el("span", { class: "info" }, "backfill") : null,
        el("span", { class: "muted" }, item.open ? `since ${when(item.started)} · for ${took(item.seconds)}` : `${when(item.started)} → ${when(item.resolved)} · lasted ${took(item.seconds)}`),
        el("span", { class: "muted" }, `${item.runs.length} run(s)`),
      ),
      el("span", {}, causes),
      latest ? el("span", { class: "muted" }, "latest: ", el("a", { href: link("run", latest.id) }, when(latest.started)), " · ", latest.detail || "") : null,
      item.open ? command(item.rerun) : null,
    );
  }

  // ---- pipelines ----------------------------------------------------------------------------

  function pipelinesPage(view) {
    const shown = pipelines.filter((p) => matches(p, view.q));
    const search = el("input", {
      type: "search",
      placeholder: `filter ${pipelines.length} pipelines`,
      value: view.q,
      onchange: (event) => go("pipelines", null, { q: event.target.value }),
    });
    return [
      el("section", { class: "title" }, el("h1", {}, "Pipelines"), search, el("span", { class: "muted" }, `${shown.length} of ${pipelines.length} · worst first`)),
      el("section", { class: "grid" }, ...shown.map((p) => pipelineCard(p, view))),
    ];
  }

  function pipelineCard(p, view) {
    const mine = (runsOf[p.id] || []).filter((run) => inRange(run, view));
    const good = mine.filter((run) => run.outcome === "succeeded" || run.outcome === "retried").length;
    const cursors = [...new Set(p.resources.map((r) => r.cursor).filter(Boolean))];
    return el(
      "a",
      { class: `card ${p.state}`, href: link("pipeline", p.id) },
      el("div", { class: "head title" }, el("span", { class: "name" }, p.id), state(p.state)),
      el("span", { class: "muted" }, `${p.source_type || "source ?"} → ${p.destination || "?"}${p.dataset ? " · " + p.dataset : ""}`),
      el("span", { class: "muted" }, `${p.resources.length} resource(s) · ${cursors.length ? "cursor " + cursors.join(", ") : "no cursor: all of it each run"}`),
      el("span", {}, `last ok ${ago(p.last_success)} · ${good}/${mine.length} runs ok in range`),
      strip((runsOf[p.id] || []).slice(0, 40).reverse()),
      p.state === "ok" ? null : el("span", { class: STATES[p.state][1] }, p.headline),
    );
  }

  function strip(list) {
    const svg = sv("svg", { width: 40 * 7, height: 14 });
    list.forEach((run, index) =>
      svg.append(sv("rect", { x: index * 7, y: 1, width: 5, height: 12, fill: colour(OUTCOMES[run.outcome][1]), tip: `${when(run.started)} ${OUTCOMES[run.outcome][2]} ${took(run.seconds)}` })),
    );
    return svg;
  }

  function pipelinePage(view) {
    const p = pipelineById[view.id];
    if (!p) return [panel("Pipeline", el("p", { class: "empty" }, `no pipeline "${view.id}"`))];
    const mine = (runsOf[p.id] || []).filter((run) => inRange(run, view));
    const incident = D.incidents.find((item) => item.open && item.pipeline === p.id);
    const missing = D.missing.filter((m) => m.pipeline === p.id && m.fixable);
    return [
      el(
        "section",
        { class: "title" },
        el("span", { class: "subtle" }, "PIPELINE"),
        el("h1", {}, p.id),
        state(p.state),
        el("select", { onchange: (event) => go("pipeline", event.target.value) }, ...pipelines.map((other) => el("option", { value: other.id, ...(other.id === p.id ? { selected: "" } : {}) }, other.id))),
      ),
      el(
        "section",
        { class: "facts" },
        el("span", {}, el("b", {}, `${p.source_type || "?"} ${p.source || ""}`), " → ", el("b", {}, `${p.destination || "?"}${p.dataset ? " · " + p.dataset : ""}`)),
        el("span", {}, "last success ", el("b", {}, ago(p.last_success))),
        el("span", {}, el("b", {}, mine.length), " runs in range"),
        el("span", {}, el("b", {}, human(mine.reduce((s, r) => s + (r.rows || 0), 0))), " rows in range"),
      ),
      p.concerns.length
        ? panel("Now", ...p.concerns.filter((c) => !(incident && c.headline.startsWith("failing"))).map((c) => el("div", { class: "item" }, el("span", { class: c.level === "error" ? "error" : "warn" }, c.headline), el("span", { class: "muted" }, c.detail))), incident ? incidentItem(incident) : null, missing.length ? el("a", { href: link("backfills", null, { q: p.id }) }, `${missing.length} missing range(s): see Backfills`) : null)
        : null,
      panel("What it reads and writes", resourcesTable(p), p.tables.length ? tablesTable(p) : el("p", { class: "subtle" }, "destination tables: none recorded (older monitoring records)")),
      panel("Runs in range", mine.length ? runChart(mine) : el("p", { class: "empty" }, "no runs in this range"), legend()),
      cursorPanel(p, view),
      panel("Runs", runTable(mine.slice(0, 200), false)),
    ];
  }

  function resourcesTable(p) {
    return el(
      "table",
      {},
      el("tr", {}, ...["resource", "write", "reads", "cursor now", "history (backfill)", "if empty", "rows loaded", "last load"].map((h) => el("th", { class: h === "rows loaded" ? "num" : "" }, h))),
      ...p.resources.map((r) => {
        const last = r.cursor_path.length ? r.cursor_path[r.cursor_path.length - 1][1] : null;
        return el(
          "tr",
          {},
          el("td", { class: "bright" }, r.name),
          el("td", {}, r.write),
          el("td", { class: "muted" }, r.cursor ? `${r.cursor} (${(r.cursor_kind || "").replace("Cursor", "").toLowerCase()})${r.cursor_start ? " from " + edge(r.cursor_start) : ""}` : "no cursor: all of it, each run"),
          el("td", {}, r.cursor ? (last ? edge(last) : el("span", { class: "dim" }, "not recorded")) : el("span", { class: "dim" }, "-")),
          el("td", { class: "muted" }, r.history ? `${edge(r.history[0])} → ${r.history[1] ? edge(r.history[1]) : "open"}` : "none declared"),
          el("td", {}, r.empty),
          el("td", { class: "num" }, human(r.rows)),
          el("td", { class: "muted" }, r.last_load ? ago(r.last_load) : "never"),
        );
      }),
    );
  }

  function tablesTable(p) {
    return el(
      "table",
      { style: "margin-top: 12px" },
      el("tr", {}, el("th", {}, "destination table"), el("th", { class: "num" }, "rows loaded"), el("th", {}, "last load")),
      ...p.tables.map((t) => el("tr", {}, el("td", {}, `${p.dataset || ""}.${t.table}`), el("td", { class: "num" }, human(t.rows)), el("td", { class: "muted" }, ago(t.last)))),
    );
  }

  function runChart(list) {
    // each run a bar at when it started, as tall as it took (square root, so long runs do not
    // flatten the rest), coloured by how it ended; a click opens it. The axis spans the runs,
    // not the whole range: runs by hand an hour apart would otherwise be one spike.
    const w = width();
    const h = 150;
    const left = 60;
    const first = Math.min(...list.map((r) => Date.parse(r.started)));
    const last = Math.max(...list.map((r) => Date.parse(r.ended || r.started)));
    const pad = Math.max(60000, (last - first) * 0.03);
    const start = first - pad;
    const end = last + pad;
    const longest = Math.max(1, ...list.map((r) => r.seconds || 0));
    const x = (iso) => left + ((Date.parse(iso) - start) / (end - start)) * (w - left - 10);
    const y = (seconds) => h - 20 - Math.sqrt((seconds || 0) / longest) * (h - 40);
    const svg = sv(
      "svg",
      { width: w, height: h },
      sv("line", { x1: left, x2: w - 10, y1: h - 20, y2: h - 20, class: "axis" }),
      sv("text", { x: 0, y: y(longest) + 4 }, took(longest)),
      sv("text", { x: 0, y: h - 22 }, "0"),
    );
    for (const run of list) {
      svg.append(
        sv("rect", {
          x: x(run.started) - 2,
          y: y(run.seconds),
          width: 4,
          height: Math.max(2, h - 20 - y(run.seconds)),
          class: "bar",
          fill: colour(OUTCOMES[run.outcome][1]),
          tip: `${when(run.started)} · ${OUTCOMES[run.outcome][2]} · ${took(run.seconds)} · ${human(run.rows)} rows${run.mode === "backfill" ? " · backfill" : ""}${run.detail ? "\n" + run.detail : ""}`,
          onclick: () => go("run", run.id),
          style: "cursor: pointer",
        }),
      );
    }
    axis(svg, start, end, left, w - 10, h - 4);
    return el("div", {}, el("span", { class: "subtle" }, `each bar a run: height how long it took, colour how it ended · ${when(new Date(first).toISOString())} → ${when(new Date(last).toISOString())} UTC`), svg);
  }

  function axis(svg, start, end, x0, x1, y, ticks = 6) {
    for (let i = 0; i <= ticks; i++) {
      const moment = start + ((end - start) * i) / ticks;
      const iso = new Date(moment).toISOString();
      const text = end - start > 7 * DAY ? iso.slice(5, 10) : end - start > DAY / 2 ? iso.slice(5, 16).replace("T", " ") : iso.slice(11, 16);
      svg.append(sv("text", { x: x0 + ((x1 - x0) * i) / ticks - 14, y }, text));
    }
  }

  function cursorPanel(p, view) {
    const charted = p.resources.filter((r) => r.cursor && r.cursor_path.length);
    if (!charted.length) return null;
    return panel(
      "Cursor position",
      el("p", { class: "subtle" }, "where each incremental cursor stood after each load: it should climb; a flat line is no new data, a jump is a hole, a drop is a reset"),
      el("div", { class: charted.length > 1 ? "pair" : "" }, ...charted.map((r) => el("div", {}, el("span", { class: "bright" }, `${r.name} · ${r.cursor}`), lineChart(r.cursor_path, r.cursor_kind === "TimeCursor", charted.length > 1)))),
    );
  }

  function lineChart(points, times, small) {
    const w = small ? Math.floor((width() - 16) / 2) : width();
    const h = small ? 84 : 110;
    const left = small ? 80 : 120;
    const value = (v) => (times ? Date.parse(v) : Number(v));
    const xs = points.map(([at]) => Date.parse(at));
    const ys = points.map(([, v]) => value(v)).filter((v) => !Number.isNaN(v));
    if (!ys.length) return el("p", { class: "dim" }, "values are not comparable");
    const [x0, x1] = [Math.min(...xs), Math.max(...xs) + 1];
    const [y0, y1] = [Math.min(...ys), Math.max(...ys) + 1];
    const X = (t) => left + ((t - x0) / (x1 - x0)) * (w - left - 10);
    const Y = (v) => h - 22 - ((v - y0) / (y1 - y0)) * (h - 36);
    const sameDay = times && new Date(y0).toISOString().slice(0, 10) === new Date(y1).toISOString().slice(0, 10);
    const label = (v) => (times ? new Date(v).toISOString().slice(sameDay ? 11 : 0, 16).replace("T", " ") : human(v));
    const svg = sv(
      "svg",
      { width: w, height: h },
      sv("text", { x: 0, y: Y(y1) + 10 }, label(y1)),
      sv("text", { x: 0, y: Y(y0) }, label(y0)),
      sv("polyline", { points: points.map(([at, v]) => `${X(Date.parse(at))},${Y(value(v))}`).join(" "), fill: "none", stroke: colour("ok"), "stroke-width": 1.5 }),
    );
    for (const [at, v] of points) svg.append(sv("circle", { cx: X(Date.parse(at)), cy: Y(value(v)), r: 3, fill: colour("ok"), tip: `loaded ${when(at)}\ncursor ${edge(v)}` }));
    axis(svg, x0, x1, left, w - 10, h - 4, small ? 3 : 6);
    return svg;
  }

  // ---- runs ---------------------------------------------------------------------------------

  function runsPage(view) {
    const shown = runs.filter((run) => inRange(run, view) && (!view.pipeline || run.pipeline === view.pipeline) && (!view.outcome || run.outcome === view.outcome));
    const pick = el(
      "select",
      { onchange: (event) => go("runs", null, { pipeline: event.target.value, outcome: view.outcome }) },
      el("option", { value: "" }, `all ${pipelines.length} pipelines`),
      ...pipelines.map((p) => el("option", { value: p.id, ...(p.id === view.pipeline ? { selected: "" } : {}) }, p.id)),
    );
    const ending = el(
      "select",
      { onchange: (event) => go("runs", null, { pipeline: view.pipeline, outcome: event.target.value }) },
      el("option", { value: "" }, "any outcome"),
      ...Object.entries(OUTCOMES).map(([key, [, , word]]) => el("option", { value: key, ...(key === view.outcome ? { selected: "" } : {}) }, word)),
    );
    return [
      el("section", { class: "title" }, el("h1", {}, "Runs"), pick, ending, el("span", { class: "muted" }, `${shown.length} run(s) in range, newest first · open one to see it in full`)),
      shown.length ? panel("When they ran", runChart(shown), legend()) : null,
      panel(null, runTable(shown.slice(0, 500), true), shown.length > 500 ? el("p", { class: "subtle" }, `showing 500 of ${shown.length}: narrow the days or pick a pipeline`) : null),
    ];
  }

  function runTable(list, named) {
    if (!list.length) return el("p", { class: "empty" }, "no runs in this range");
    return el(
      "table",
      {},
      el("tr", {}, ...["started (UTC)", named ? "pipeline" : null, "kind", "outcome", "took", "rows", "peak memory", "cause"].filter(Boolean).map((h) => el("th", { class: ["took", "rows", "peak memory"].includes(h) ? "num" : "" }, h))),
      ...list.map((run) =>
        el(
          "tr",
          { class: "link", onclick: () => go("run", run.id) },
          el("td", { class: "nowrap" }, when(run.started)),
          named ? el("td", { class: "bright nowrap" }, run.pipeline) : null,
          el("td", { class: run.mode === "backfill" ? "info" : "muted" }, run.mode === "backfill" ? "backfill" : "run"),
          el("td", { class: "nowrap" }, outcome(run.outcome)),
          el("td", { class: "num" }, took(run.seconds)),
          el("td", { class: "num" }, human(run.rows)),
          el("td", { class: "num" }, mb(run.peak_mb)),
          el("td", { class: "muted clip", tip: run.detail || null }, run.detail || ""),
        ),
      ),
    );
  }

  // ---- one run ------------------------------------------------------------------------------

  window.DASH_RUN = (detail) => {
    details[detail.run] = detail;
    render();
  };

  function runPage(view) {
    const run = runById[view.id];
    if (!run) return [panel("Run", el("p", { class: "empty" }, `no run "${view.id}"`))];
    const detail = details[run.id];
    if (!detail && !requested.has(run.id)) {
      requested.add(run.id);
      const script = el("script", { src: `runs/${run.id.replace(/[^A-Za-z0-9_-]/g, "_")}.js` });
      script.addEventListener("error", () => {
        details[run.id] = { run: run.id, missing: true, error: {}, steps: [], samples: [], loads: [], tables: [] };
        render();
      });
      document.body.append(script);
    }
    const siblings = runsOf[run.pipeline] || [];
    const index = siblings.indexOf(run);
    const older = siblings[index + 1];
    const newer = siblings[index - 1];
    const broken = run.outcome === "failed" || run.outcome === "lost";
    return [
      el(
        "section",
        { class: "title" },
        el("span", { class: "subtle" }, "RUN"),
        el("h1", {}, `${when(run.started)} UTC`),
        el("a", { href: link("pipeline", run.pipeline) }, run.pipeline),
        el("span", { class: run.mode === "backfill" ? "info" : "muted" }, run.mode === "backfill" ? "backfill" : "incremental run"),
        newer ? el("a", { href: link("run", newer.id) }, "← newer") : null,
        older ? el("a", { href: link("run", older.id) }, "older →") : null,
      ),
      el(
        "section",
        { class: "verdict" },
        el("span", { class: `big ${OUTCOMES[run.outcome][1]}` }, `${OUTCOMES[run.outcome][0]} ${OUTCOMES[run.outcome][2].toUpperCase()}`),
        run.detail ? el("span", { class: OUTCOMES[run.outcome][1] }, run.detail) : null,
      ),
      broken ? el("section", {}, el("span", { class: "muted" }, "fix the cause, then"), command(run.rerun)) : null,
      el(
        "section",
        { class: "facts" },
        el("span", {}, "took ", el("b", {}, took(run.seconds))),
        el("span", {}, el("b", {}, human(run.rows)), " rows"),
        el("span", {}, "cpu ", el("b", {}, took(run.cpu_s))),
        el("span", {}, "peak memory ", el("b", {}, mb(run.peak_mb)), run.available_mb ? ` of ${mb(run.available_mb)} ${run.limit ? "limit" : "host"}` : ""),
        el("span", {}, "code ", el("b", {}, run.version || "-")),
        el("span", {}, "host ", el("b", {}, run.host || "-")),
        el("span", { class: "dim" }, run.id),
      ),
      detail ? runDetail(run, detail) : panel(null, el("p", { class: "muted" }, "loading this run…")),
    ];
  }

  function runDetail(run, detail) {
    if (detail.missing) return [panel(null, el("p", { class: "empty" }, `runs/${run.id}.js is missing: rebuild the dashboard`))];
    const absent = [["steps", detail.steps], ["memory samples", detail.samples], ["loads", detail.loads], ["tables", detail.tables]].filter(([, list]) => !list.length).map(([name]) => name);
    return [
      errorPanel(run, detail.error),
      detail.steps.length || detail.samples.length ? panel("Timeline", detail.steps.length ? waterfall(run, detail.steps) : null, detail.samples.length > 1 ? memoryChart(run, detail) : null) : null,
      detail.loads.length ? panel("What it loaded", loadsTable(run, detail.loads)) : null,
      detail.tables.length
        ? panel("Destination tables", el("table", {}, el("tr", {}, el("th", {}, "table"), el("th", { class: "num" }, "rows"), el("th", { class: "num" }, "load time")), ...detail.tables.map((t) => el("tr", {}, el("td", {}, t.table), el("td", { class: "num" }, human(t.rows)), el("td", { class: "num" }, took(t.seconds))))))
        : null,
      absent.length ? el("p", { class: "subtle" }, `not recorded for this run: ${absent.join(", ")}. It stopped before them, or its monitoring predates them.`) : null,
    ];
  }

  function errorPanel(run, error) {
    if (run.outcome === "lost")
      return panel("Killed", el("p", {}, "No end was reported: the process was stopped from outside (out of memory, a stopped container, a killed shell). Its last memory sample was ", el("b", {}, mb(run.peak_mb)), run.available_mb ? ` of ${mb(run.available_mb)}.` : ", with no limit recorded to compare."));
    if (run.outcome !== "failed") return null;
    const signal = /signal_code=\D?(\d+)/.exec(error.message || "");
    const meaning = signal ? SIGNALS[Number(signal[1])] : null;
    const rows = [
      ["cause", `${error.cause_type || ""}: ${error.message || ""}`],
      ["meaning", meaning ? `signal ${signal[1]}, ${meaning[0]}: ${meaning[1]}` : null],
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

  function span(run) {
    const start = Date.parse(run.started);
    return [start, Math.max(start + 1000, Date.parse(run.ended || run.started))];
  }

  function waterfall(run, steps) {
    const w = width();
    const left = 130;
    const lane = 20;
    const [start, end] = span(run);
    const x = (iso) => left + ((Date.parse(iso) - start) / (end - start)) * (w - left - 10);
    const svg = sv("svg", { width: w, height: steps.length * lane + 22 });
    steps.forEach((step, row) => {
      const busy = step.seconds ? step.cpu / step.seconds : 0;
      const nature = step.outcome === "failed" ? "error" : busy >= 0.6 ? "accent" : busy <= 0.2 ? "info" : "warn";
      const y = row * lane;
      svg.append(
        sv("text", { x: 0, y: y + 13 }, `${step.step} #${step.attempt}`),
        sv("rect", {
          x: x(step.started),
          y: y + 3,
          width: Math.max(2, x(step.ended) - x(step.started)),
          height: lane - 6,
          fill: colour(nature),
          class: "bar",
          tip: `${step.step} attempt ${step.attempt}: ${took(step.seconds)}, ${(busy * 100).toFixed(0)}% CPU${step.cause ? "\n" + step.cause : ""}`,
        }),
      );
    });
    axisSince(svg, start, end, left, w - 10, steps.length * lane + 16);
    return el("div", {}, el("span", { class: "subtle" }, "steps · magenta computing · cyan waiting on upstream or the destination · amber mixed · red failed"), svg);
  }

  function axisSince(svg, start, end, x0, x1, y) {
    for (let i = 0; i <= 5; i++) svg.append(sv("text", { x: x0 + ((x1 - x0) * i) / 5 - 10, y }, `+${took(((end - start) * i) / 5 / 1000)}`));
  }

  function memoryChart(run, detail) {
    const w = width();
    const h = 140;
    const left = 130;
    const [start, end] = span(run);
    const limit = detail.samples.map((s) => s[3]).find((v) => v) || null;
    const top = Math.max(limit || 0, ...detail.samples.map((s) => s[1])) * 1.05;
    const X = (iso) => left + ((Date.parse(iso) - start) / (end - start)) * (w - left - 10);
    const Y = (v) => h - 22 - (v / top) * (h - 34);
    const svg = sv(
      "svg",
      { width: w, height: h },
      sv("text", { x: 0, y: Y(top) + 10 }, mb(top)),
      sv("text", { x: 0, y: Y(0) }, "0"),
      sv("line", { x1: left, x2: w - 10, y1: Y(0), y2: Y(0), class: "axis" }),
      sv("polyline", { points: detail.samples.map((s) => `${X(s[0])},${Y(s[1])}`).join(" "), fill: "none", stroke: colour("ok"), "stroke-width": 1.5 }),
    );
    if (limit)
      svg.append(sv("line", { x1: left, x2: w - 10, y1: Y(limit), y2: Y(limit), stroke: colour("error"), "stroke-dasharray": "3 3" }), sv("text", { x: w - 120, y: Y(limit) - 4, style: "fill: var(--error)" }, `limit ${mb(limit)}`));
    for (const s of detail.samples) svg.append(sv("rect", { x: X(s[0]) - 2, y: 0, width: 4, height: h - 22, fill: "transparent", tip: `${s[0].slice(11, 19)} · ${mb(s[1])}${s[2] ? " · " + s[2] : ""}` }));
    axisSince(svg, start, end, left, w - 10, h - 4);
    return el("div", {}, el("span", { class: "subtle" }, `memory (rss) · peak ${mb(run.peak_mb)}${limit ? "" : " · no container limit recorded"}`), svg);
  }

  function loadsTable(run, loads) {
    const p = pipelineById[run.pipeline];
    const declared = Object.fromEntries((p ? p.resources : []).map((r) => [r.name, r]));
    return el(
      "table",
      {},
      el("tr", {}, ...["resource", "rows", "reads", "from", "to", "extracted (UTC)"].map((h) => el("th", { class: h === "rows" ? "num" : "" }, h))),
      ...loads.map((load) => {
        const r = declared[load.resource] || {};
        const windowed = load.kind !== "increment";
        const [low, high] = windowed ? load.window : load.cursor;
        const reads = windowed ? `${load.kind} window` : r.cursor ? `cursor ${r.cursor}` : "no cursor: all of it";
        const none = el("span", { class: "dim" }, windowed || r.cursor ? "not recorded" : "-");
        return el(
          "tr",
          {},
          el("td", { class: "bright" }, load.resource),
          el("td", { class: "num" }, (load.rows || 0).toLocaleString("en")),
          el("td", { class: "muted" }, reads),
          el("td", {}, low ? edge(low) : none),
          el("td", {}, high ? edge(high) : windowed || r.cursor ? none : el("span", { class: "dim" }, "-")),
          el("td", { class: "muted" }, load.extracted[0] ? `${load.extracted[0].slice(11, 19)} → ${(load.extracted[1] || "").slice(11, 19)}` : "-"),
        );
      }),
    );
  }

  // ---- backfills ----------------------------------------------------------------------------

  const KINDS = {
    gap: ["GAP", "error", "never loaded"],
    failed: ["FAILED", "error", "a backfill failed there"],
    hole: ["HOLE", "error", "an increment skipped it"],
    empty: ["EMPTY", "warn", "no rows between windows that had some"],
    off: ["OFF", "warn", "differs from the source"],
    reset: ["RESET", "warn", "the cursor went back"],
    twice: ["TWICE", "muted", "loaded more than once"],
  };

  function backfillsPage(view) {
    const histories = D.histories.filter((h) => matches({ id: h.resource }, view.q));
    const declared = new Set(histories.map((h) => h.resource));
    const loose = D.missing.filter((m) => !declared.has(m.resource) && matches({ id: m.resource }, view.q));
    const jobs = runs.filter((run) => run.mode === "backfill" && inRange(run, { ...view, mode: "all" }));
    const missing = D.missing.filter((m) => m.fixable && matches({ id: m.resource }, view.q));
    const search = el("input", { type: "search", placeholder: "filter resources", value: view.q, onchange: (event) => go("backfills", null, { q: event.target.value }) });
    if (!D.histories.length && !D.missing.length && !jobs.length)
      return [
        el("section", { class: "title" }, el("h1", {}, "Backfills")),
        panel(null, el("p", {}, "No resource declares a history, and no backfill has run in range."), el("p", { class: "muted" }, "A resource declares one with backfill=TimeBackfill(start=..., every=...) or ValueBackfill(...). Then `pipeline backfill NAME` loads it window by window, and this page shows what is loaded and what is missing.")),
      ];
    return [
      el("section", { class: "title" }, el("h1", {}, "Backfills"), search, el("span", { class: missing.length ? "error" : "ok" }, missing.length ? `✖ ${missing.length} range(s) missing` : "✔ nothing missing")),
      ...histories.map((h) => historyPanel(h)),
      loose.length ? panel("Incremental cursors", ...loose.map(missingItem)) : null,
      panel("Backfill jobs in range", jobs.length ? jobsTable(jobs) : el("p", { class: "empty" }, "no backfill ran in this range")),
    ];
  }

  function historyPanel(h) {
    const w = width() - 40;
    const cell = w / h.cells.length;
    const ribbon = sv("svg", { width: w, height: 20 });
    h.cells.forEach((loaded, index) => ribbon.append(sv("rect", { x: index * cell, y: 2, width: Math.max(1, cell - 0.5), height: 16, fill: colour(loaded ? "ok" : "error"), "fill-opacity": loaded ? 0.8 : 0.6 })));
    const items = D.missing.filter((m) => m.resource === h.resource);
    return panel(
      `${h.resource} · ${h.window} windows`,
      el("div", { class: "facts" }, el("span", {}, "declared ", el("b", {}, `${edge(h.start)} → ${h.end ? edge(h.end) : "open"}`)), el("span", { class: items.some((m) => m.fixable) ? "warn" : "ok" }, h.fraction === null ? "open-ended" : `${Math.round(h.fraction * 100)}% loaded`)),
      ribbon,
      el("div", { class: "legend" }, el("span", {}, el("i", { style: "background: var(--ok)" }), "loaded"), el("span", {}, el("i", { style: "background: var(--error)" }), "missing")),
      items.length ? items.map(missingItem) : el("p", { class: "ok" }, "✔ nothing missing"),
    );
  }

  function missingItem(m) {
    const [label, style, meaning] = KINDS[m.kind] || [m.kind, "muted", ""];
    return el(
      "div",
      { class: "item" },
      el("div", { class: "head" }, el("span", { class: style, tip: meaning }, label), el("b", { class: "bright" }, `${edge(m.start)} → ${m.end ? edge(m.end) : "open"}`), el("span", { class: "muted" }, m.resource)),
      el("span", { class: "muted" }, m.why),
      m.command ? command(m.command) : null,
    );
  }

  function jobsTable(jobs) {
    const extra = Object.fromEntries(D.jobs.map((j) => [j.run, j]));
    return el(
      "table",
      {},
      el("tr", {}, ...["started (UTC)", "pipeline", "outcome", "windows", "rows", "took", "where it stopped"].map((h) => el("th", {}, h))),
      ...jobs.map((run) =>
        el(
          "tr",
          { class: "link", onclick: () => go("run", run.id) },
          el("td", {}, when(run.started)),
          el("td", { class: "bright" }, run.pipeline),
          el("td", {}, outcome(run.outcome)),
          el("td", { class: "num" }, (extra[run.id] || {}).windows ?? "-"),
          el("td", { class: "num" }, human(run.rows)),
          el("td", { class: "num" }, took(run.seconds)),
          el("td", {}, (extra[run.id] || {}).resume ? el("code", {}, (extra[run.id] || {}).resume) : ""),
        ),
      ),
    );
  }

  // ---- incidents ----------------------------------------------------------------------------

  function incidentsPage(view) {
    const overlaps = (item) => (item.resolved || D.generated).slice(0, 10) >= view.from && item.started.slice(0, 10) <= view.to;
    const kept = D.incidents.filter((item) => overlaps(item) && matches({ id: item.pipeline }, view.q) && (view.mode === "all" || item.mode === view.mode));
    const open = kept.filter((item) => item.open);
    const resolved = kept.filter((item) => !item.open);
    const search = el("input", { type: "search", placeholder: "filter pipelines", value: view.q, onchange: (event) => go("incidents", null, { q: event.target.value }) });
    return [
      el("section", { class: "title" }, el("h1", {}, "Incidents"), search, el("span", { class: open.length ? "error" : "ok" }, open.length ? `✖ ${open.length} open` : "✔ nothing open"), el("span", { class: "muted" }, `${resolved.length} resolved in range`)),
      el("p", { class: "subtle" }, "An incident is a streak of failed or killed runs of one pipeline, from the first to the next success. It stays open until that success, then moves to resolved."),
      panel("Open", open.length ? open.map(incidentItem) : el("p", { class: "ok" }, "✔ no open incidents")),
      panel("Resolved", resolved.length ? resolved.map(incidentItem) : el("p", { class: "empty" }, "none in this range")),
      causesPanel(kept, view),
    ];
  }

  function causesPanel(kept, view) {
    const columns = buckets(view);
    const counts = {};
    for (const item of kept)
      for (const id of item.runs) {
        const run = runById[id];
        if (!run) continue;
        const index = columns.findIndex(([low, high]) => run.started.slice(0, 10) >= low && run.started.slice(0, 10) <= high);
        if (index < 0) continue;
        const cause = run.cause || "error";
        (counts[cause] = counts[cause] || columns.map(() => 0))[index] += 1;
      }
    const causes = Object.entries(counts).sort((a, b) => b[1].reduce((x, y) => x + y) - a[1].reduce((x, y) => x + y));
    if (!causes.length) return null;
    const label = 260;
    const cell = Math.max(8, Math.min(34, Math.floor((width() - label - 60) / columns.length)));
    const svg = sv("svg", { width: label + columns.length * cell + 60, height: causes.length * 18 + 34 });
    const top = Math.max(...causes.flatMap(([, list]) => list));
    causes.forEach(([cause, list], row) => {
      svg.append(sv("text", { x: 0, y: row * 18 + 12, class: "label" }, cause.length > 36 ? cause.slice(0, 35) + "…" : cause));
      list.forEach((count, index) => {
        if (count) svg.append(sv("rect", { x: label + index * cell + 1, y: row * 18 + 2, width: cell - 2, height: 14, fill: colour("error"), "fill-opacity": (0.35 + 0.65 * (count / top)).toFixed(2), class: "cell", tip: `${cause} · ${columns[index][0]}\n${count} run(s)` }));
        else svg.append(sv("rect", { x: label + index * cell + cell / 2 - 1, y: row * 18 + 8, width: 2, height: 2, fill: colour("dim") }));
      });
      svg.append(sv("text", { x: label + columns.length * cell + 8, y: row * 18 + 12 }, list.reduce((x, y) => x + y)));
    });
    columns.forEach(([low], index) => {
      if (index % Math.max(1, Math.ceil(columns.length / 10)) === 0) svg.append(sv("text", { x: label + index * cell, y: causes.length * 18 + 28 }, low.slice(5)));
    });
    return panel("Causes by day", el("p", { class: "subtle" }, "one cause on many days is one recurring bug; many causes once each are many problems"), el("div", { class: "scroll" }, svg));
  }

  // ---- tuning -------------------------------------------------------------------------------

  function tuningPage() {
    const warnings = D.findings.filter((f) => f.level === "warning");
    const tuning = D.findings.filter((f) => f.level !== "warning");
    return [
      el("section", { class: "title" }, el("h1", {}, "Tuning"), el("span", { class: "muted" }, `judged over the ${D.days} days to ${today}, production runs only`)),
      panel("Findings", D.findings.length ? [...warnings, ...tuning].map(findingItem) : el("p", { class: "ok" }, "✔ every check that could run found nothing to change")),
      D.memory.length ? panel("Memory per pipeline", memoryBars()) : null,
      D.steps.length ? panel("Steps", stepsTable()) : null,
      D.slow_tables.length
        ? panel("Slowest destination tables", el("table", {}, el("tr", {}, ...["pipeline", "table", "loads", "p50", "p95", "rows a second"].map((h, i) => el("th", { class: i > 1 ? "num" : "" }, h))), ...D.slow_tables.map((t) => el("tr", {}, el("td", {}, t.pipeline_id), el("td", { class: "bright" }, t.table), el("td", { class: "num" }, t.loads), el("td", { class: "num" }, took(t.p50_s)), el("td", { class: "num" }, took(t.p95_s)), el("td", { class: "num" }, human(t.rows_per_s))))))
        : null,
      D.unchecked.length
        ? panel("Not checked", el("p", { class: "subtle" }, "these checks need records this data does not hold (older monitoring records lack them; runs since record them)"), el("table", {}, ...D.unchecked.map((u) => el("tr", {}, el("td", {}, u.title), el("td", { class: "muted" }, "needs " + u.needs.join(", "))))))
        : null,
    ];
  }

  function findingItem(f) {
    return el(
      "div",
      { class: "item" },
      el("div", { class: "head" }, el("span", { class: f.level === "warning" ? "warn" : "info" }, f.level === "warning" ? "⚠" : "●"), el("a", { href: link("pipeline", f.pipeline.split(", ")[0]) }, f.pipeline), el("b", { class: f.level === "warning" ? "warn" : "info" }, f.title)),
      el("span", { class: "muted" }, f.evidence),
      el("span", {}, "→ ", f.action, f.also ? el("span", { class: "muted" }, ` (${f.also})`) : null),
    );
  }

  function memoryBars() {
    const w = width();
    const left = 260;
    const rows = D.memory;
    const top = Math.max(...rows.map((r) => Math.max(r.max_mb || 0, r.available_mb || 0)));
    const X = (v) => left + (v / top) * (w - left - 80);
    const svg = sv("svg", { width: w, height: rows.length * 22 + 8 });
    rows.forEach((r, row) => {
      const y = row * 22;
      const tone = r.share === null ? "info" : r.share >= 0.9 ? "error" : r.share >= 0.7 ? "warn" : "ok";
      svg.append(
        sv("text", { x: 0, y: y + 14, class: "label", onclick: () => go("pipeline", r.pipeline_id) }, r.pipeline_id),
        sv("rect", { x: left, y: y + 4, width: Math.max(1, X(r.p99_mb) - left), height: 12, fill: colour(tone), "fill-opacity": 0.75, tip: `${r.pipeline_id}\np50 ${mb(r.p50_mb)} · p99 ${mb(r.p99_mb)} · max ${mb(r.max_mb)}\n${r.available_mb ? "of " + mb(r.available_mb) + (r.limit_mb ? " limit" : " host") : "no limit or host memory recorded"}\nsize it at ${mb(r.recommended_mb)} (p99 + 25%) · ${r.runs} runs` }),
        sv("line", { x1: X(r.max_mb), x2: X(r.max_mb), y1: y + 2, y2: y + 18, stroke: colour("bright") }),
      );
      if (r.available_mb) svg.append(sv("line", { x1: X(r.available_mb), x2: X(r.available_mb), y1: y + 1, y2: y + 19, stroke: colour("error"), "stroke-dasharray": "2 2" }));
      svg.append(sv("text", { x: w - 74, y: y + 14 }, `${mb(r.p99_mb)} p99`));
    });
    return el("div", {}, el("span", { class: "subtle" }, "bar: p99 peak · white tick: the highest peak · dashed red: the limit (or the host's memory)"), svg);
  }

  function stepsTable() {
    return el(
      "table",
      {},
      el("tr", {}, ...["pipeline", "step", "runs", "p50", "p95", "share of run", "computing"].map((h, i) => el("th", { class: i > 1 ? "num" : "" }, h))),
      ...D.steps.map((s) => el("tr", {}, el("td", {}, s.pipeline_id), el("td", {}, s.step), el("td", { class: "num" }, s.runs), el("td", { class: "num" }, took(s.p50_s)), el("td", { class: "num" }, took(s.p95_s)), el("td", { class: "num" }, `${Math.round((s.share || 0) * 100)}%`), el("td", { class: "num" }, `${Math.round((s.cpu_share || 0) * 100)}%`))),
    );
  }

  // ---- rendering ----------------------------------------------------------------------------

  const ROUTES = {
    overview,
    pipelines: pipelinesPage,
    pipeline: pipelinePage,
    runs: runsPage,
    run: runPage,
    backfills: backfillsPage,
    incidents: incidentsPage,
    tuning: tuningPage,
  };

  function render() {
    const view = read();
    frame(view);
    const draw = ROUTES[view.route] || overview;
    document.getElementById("page").replaceChildren(...[draw(view)].flat(Infinity).filter(Boolean));
    document.title = `dlt//ops · ${view.route}${view.id ? " · " + view.id : ""}`;
  }

  let resizing;
  window.addEventListener("hashchange", render);
  window.addEventListener("resize", () => {
    clearTimeout(resizing);
    resizing = setTimeout(render, 150);
  });
  render();
})();
