/* dlt//ops, part 2 of 3: the pieces every page is built from, each written once and shaped by
   its arguments. Pages never build a table, a dropdown, a filter, or a chart by hand.

     layout     heading, facts, panel, item, command, legend, badges
     controls   controls (the one filter row), select
     tables     table: columns as data, rows as data; meter for a share inside a cell
     charts     matrix (rows by day columns), plot (a time axis and a linear y axis, with dots,
                lines, spans and rules as marks), sparkline */

"use strict";

// ---- layout -----------------------------------------------------------------------------------

function heading(title, ...extras) {
  return el("section", { class: "title" }, el("h1", {}, title), ...extras);
}

/** A line of facts; each a [value, label] pair (a tone may follow), or a node. */
function facts(...parts) {
  return el(
    "section",
    { class: "facts" },
    ...parts.filter(Boolean).map((part) =>
      part instanceof Node ? part : el("span", { class: part[2] || "" }, el("b", {}, part[0]), " ", part[1]),
    ),
  );
}

function panel(title, ...kids) {
  return el("section", { class: "panel" }, title ? el("h2", {}, title) : null, ...kids);
}

/** One entry of a list: a head line of parts, then lines beneath. */
function item(head, ...lines) {
  return el("div", { class: "item" }, el("div", { class: "head" }, ...head), ...lines);
}

function note(text, cls = "subtle") {
  return el("p", { class: cls }, text);
}

function command(text) {
  const button = el("button", { class: "copy", type: "button" }, "copy");
  button.addEventListener("click", () => {
    if (navigator.clipboard)
      navigator.clipboard.writeText(text).then(
        () => (button.textContent = "copied"),
        () => (button.textContent = "select it"),
      );
  });
  return el("div", { class: "cmdline" }, el("code", {}, "$ " + text), button);
}

/** A legend: [colour token, label] pairs. */
function legend(entries) {
  return el(
    "div",
    { class: "legend" },
    ...entries.map(([token, label]) => el("span", {}, el("i", { style: `background: var(--${token})` }), label)),
  );
}

function outcomeLegend() {
  return legend([...Object.values(LOOK.outcomes).map(([, token, word]) => [token, word]), ["dim", "no run"]]);
}

/** A state or outcome as glyph and word, in its colour: never colour alone. */
function badge([glyph, token, word], upper = false) {
  return el("span", { class: `state ${token}` }, `${glyph} ${upper ? word.toUpperCase() : word}`);
}

function stateBadge(level) {
  return badge(LOOK.states[level] || LOOK.states.idle);
}

function outcomeBadge(run) {
  return badge(LOOK.outcomes[look(run)]);
}

// ---- controls ---------------------------------------------------------------------------------

function select(options, current, onchange) {
  return el(
    "select",
    { onchange: (event) => onchange(event.target.value) },
    ...options.map(([value, text]) => el("option", { value, ...(value === current ? { selected: "" } : {}) }, text)),
  );
}

/** The one filter row: the days and mode every page shares, then the filters ``page`` asks for. */
function controls(view, filters) {
  const preset = (label, from) =>
    el(
      "button",
      { type: "button", class: from === view.from && view.to === today ? "on" : "", onclick: () => go(view.route, view.id, { from, to: today }) },
      label,
    );
  const date = (key, value) =>
    el("input", {
      type: "date",
      value,
      min: firstDay,
      max: today,
      onchange: (event) => event.target.value && go(view.route, view.id, { [key]: event.target.value }),
    });
  const own = {
    q: () =>
      el("input", {
        type: "search",
        placeholder: "filter by name",
        value: view.q,
        onchange: (event) => refilter({ q: event.target.value }),
      }),
    pipeline: () =>
      select([["", `all ${pipelines.length} pipelines`], ...pipelines.map((p) => [p.id, p.id])], view.pipeline, (value) =>
        refilter({ pipeline: value }),
      ),
    outcome: () =>
      select([["", "any outcome"], ...Object.entries(LOOK.outcomes).map(([key, [, , word]]) => [key, word])], view.outcome, (value) =>
        refilter({ outcome: value }),
      ),
  };
  const count = daysBetween(view.from, view.to).length;
  return [
    el("label", {}, "days", date("from", view.from), "→", date("to", view.to)),
    preset("7d", shift(today, -6)),
    preset("14d", shift(today, -13)),
    preset("30d", shift(today, -29)),
    preset("all", firstDay < shift(today, -29) ? firstDay : shift(today, -89)),
    select(
      [["all", "runs and backfills"], ["run", "runs only"], ["backfill", "backfills only"]],
      view.mode,
      (mode) => go(view.route, view.id, { mode }),
    ),
    ...filters.map((key) => own[key]()),
    el("span", { class: "range" }, `${view.from} → ${view.to} UTC · ${plural(count, "day")}`),
  ];
}

// ---- tables -----------------------------------------------------------------------------------

/** A table from column specs: ``{title, value(row), num, cls(row)}``; ``value`` returns text or a
    node. ``onRow`` makes rows links. */
function table(columns, rows, { onRow, empty = "nothing to show" } = {}) {
  if (!rows.length) return note(empty, "empty");
  return el(
    "table",
    {},
    el("tr", {}, ...columns.map((c) => el("th", { class: c.num ? "num" : "" }, c.title))),
    ...rows.map((row) =>
      el(
        "tr",
        { class: onRow ? "link" : "", onclick: onRow && (() => onRow(row)) },
        ...columns.map((c) => {
          const value = c.value(row);
          return el("td", { class: [c.num ? "num" : "", c.cls ? c.cls(row) : ""].join(" ").trim() }, value ?? "-");
        }),
      ),
    ),
  );
}

/** A share of something as a short bar, for a table cell. */
function meter(share, token = "ok", tipText = null) {
  const svg = sv("svg", { width: 80, height: 10, tip: tipText });
  svg.append(sv("rect", { x: 0, y: 2, width: 80, height: 6, fill: colour("dim"), "fill-opacity": 0.4 }));
  if (share !== null && share !== undefined)
    svg.append(sv("rect", { x: 0, y: 2, width: Math.max(1, Math.min(1, share) * 80), height: 6, fill: colour(token) }));
  return svg;
}

// ---- charts -----------------------------------------------------------------------------------

/** Day columns for the range: a column a day, merged into wider ones past 120 days. */
function dayColumns(view) {
  const all = daysBetween(view.from, view.to);
  const step = Math.max(1, Math.ceil(all.length / 120));
  const out = [];
  for (let index = 0; index < all.length; index += step) out.push([all[index], all[Math.min(all.length - 1, index + step - 1)]]);
  return out;
}

function columnOf(columns, iso) {
  const day = iso.slice(0, 10);
  return columns.findIndex(([low, high]) => day >= low && day <= high);
}

/** Rows by day columns. Each row: ``{label, glyph, token, tip, onclick, right, cells}``, where
    ``cells[index]`` is null (nothing that day) or ``{stack: [[token, count]], strength, tip,
    onclick, mark}``: a stacked cell, worst on top. */
function matrix(columns, rows, { label = 250, right = 90, lane = 18 } = {}) {
  const cell = Math.max(8, Math.min(44, Math.floor((width() - label - right) / Math.max(1, columns.length))));
  const top = 18;
  const svg = sv("svg", { width: label + columns.length * cell + right, height: top + rows.length * lane + 6 });
  const at = (index) => label + index * cell;
  const every = Math.max(1, Math.ceil(columns.length / Math.max(1, Math.floor((columns.length * cell) / 52))));
  columns.forEach(([low], index) => index % every === 0 && svg.append(sv("text", { x: at(index), y: 11 }, low.slice(5))));
  rows.forEach((row, index) => {
    const y = top + index * lane;
    const name = row.label.length > 32 ? row.label.slice(0, 31) + "…" : row.label;
    if (row.glyph) svg.append(sv("text", { x: 0, y: y + 12, style: `fill: var(--${row.token})` }, row.glyph));
    svg.append(sv("text", { x: row.glyph ? 16 : 0, y: y + 12, class: row.onclick ? "label" : "", onclick: row.onclick, tip: row.tip }, name));
    row.cells.forEach((cellData, column) => {
      if (!cellData) {
        svg.append(sv("rect", { x: at(column) + cell / 2 - 1, y: y + lane / 2 - 1, width: 2, height: 2, fill: colour("dim") }));
        return;
      }
      const total = sum(cellData.stack, ([, count]) => count);
      let base = y + lane - 2;
      for (const [token, count] of cellData.stack) {
        const h = ((lane - 4) * count) / total;
        base -= h;
        svg.append(sv("rect", { x: at(column) + 1, y: base, width: cell - 2, height: h, fill: colour(token), "fill-opacity": (cellData.strength ?? 1).toFixed(2), "pointer-events": "none" }));
      }
      svg.append(sv("rect", { x: at(column) + 1, y: y + 2, width: cell - 2, height: lane - 4, fill: "transparent", class: "cell", tip: cellData.tip, onclick: cellData.onclick }));
      if (cellData.mark) svg.append(sv("path", { d: `M${at(column) + 1} ${y + 2} h6 l-6 6 z`, fill: colour("info"), "pointer-events": "none" }));
    });
    if (row.right) svg.append(sv("text", { x: at(columns.length) + 8, y: y + 12, tip: row.rightTip }, row.right));
  });
  if (!rows.length) svg.append(sv("text", { x: label, y: top + 12 }, "nothing matches"));
  return el("div", { class: "scroll" }, svg);
}

// Round steps a reader counts in: seconds by the clock, memory by powers of two.
const TIME_STEPS = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 21600, 43200, 86400];
const MB_STEPS = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536];

/** About ``count`` round tick values from ``low``, the last at or above ``high``; ``steps`` are
    the round step sizes to choose from (decimal 1, 2, 5 by default). */
function ticks(low, high, count, steps = null) {
  const raw = (high - low) / count || 1;
  const power = 10 ** Math.floor(Math.log10(raw));
  const step = (steps || [1, 2, 5, 10].map((m) => m * power)).find((s) => s >= raw) || raw;
  const out = [];
  for (let value = Math.ceil(low / step) * step; out.length < 2 || out[out.length - 1] < high; value += step) out.push(value);
  return out;
}

function timeLabel(start, end) {
  const span = end - start;
  return (ms) => {
    const iso = new Date(ms).toISOString();
    return span > 7 * DAY_MS ? iso.slice(5, 10) : span > DAY_MS / 2 ? iso.slice(5, 16).replace("T", " ") : iso.slice(11, 16);
  };
}

/** A plot: x a time span (ms), y linear from ``y[0]``, a hairline grid at about ``yTicks`` round
    values (none at 0), stepping by ``ySteps`` where given. ``marks`` are drawn with the scales: ``(svg, X, Y) => ...``; ``X.range`` is
    the plot's left and right edge. Returns the svg. */
function plot({ x, y, height = 130, left = 64, yFormat = String, yTicks = 3, ySteps = null, xFormat, xTicks = 6, w = width() }, marks) {
  const bottom = 20;
  const [x0, x1] = x[0] === x[1] ? [x[0] - 60000, x[1] + 60000] : x;
  const high = y[1] > y[0] ? y[1] : y[0] + 1;
  const grid = yTicks ? ticks(y[0], high, yTicks, ySteps) : [];
  const y1 = grid.length ? grid[grid.length - 1] : high;
  const X = (ms) => left + ((ms - x0) / (x1 - x0)) * (w - left - 10);
  const Y = (value) => height - bottom - ((value - y[0]) / (y1 - y[0])) * (height - bottom - 8);
  X.range = [left, w - 10];
  const svg = sv("svg", { width: w, height });
  for (const value of grid) {
    svg.append(sv("line", { x1: left, x2: w - 10, y1: Y(value), y2: Y(value), class: "gridline" }));
    svg.append(sv("text", { x: left - 6, y: Y(value) + 4, "text-anchor": "end" }, yFormat(value)));
  }
  svg.append(sv("line", { x1: left, x2: w - 10, y1: Y(y[0]), y2: Y(y[0]), class: "axis" }));
  const label = xFormat || timeLabel(x0, x1);
  for (let i = 0; i <= xTicks; i++) {
    const ms = x0 + ((x1 - x0) * i) / xTicks;
    svg.append(sv("text", { x: X(ms), y: height - 4, "text-anchor": i === 0 ? "start" : i === xTicks ? "end" : "middle" }, label(ms)));
  }
  marks(svg, X, Y);
  return svg;
}

/** Dots ``{x, y, token, tip, onclick}``, each with a hit area wider than the mark; with ``r`` 0,
    only the hit areas, to hover a line. */
function dots(svg, X, Y, points, r = 4) {
  for (const p of points) {
    if (r) svg.append(sv("circle", { cx: X(p.x), cy: Y(p.y), r, fill: colour(p.token), "pointer-events": "none" }));
    svg.append(sv("circle", { cx: X(p.x), cy: Y(p.y), r: 12, fill: "transparent", class: p.onclick ? "cell" : "", tip: p.tip, onclick: p.onclick }));
  }
}

function line(svg, X, Y, points, token = "ok") {
  svg.append(sv("polyline", { points: points.map((p) => `${X(p.x)},${Y(p.y)}`).join(" "), fill: "none", stroke: colour(token), "stroke-width": 2 }));
}

/** A labelled horizontal rule, e.g. a memory limit. */
function rule(svg, X, Y, value, text, token = "error") {
  const [from, to] = X.range;
  svg.append(sv("line", { x1: from, x2: to, y1: Y(value), y2: Y(value), stroke: colour(token), "stroke-dasharray": "3 3" }));
  svg.append(sv("text", { x: to, y: Y(value) - 4, "text-anchor": "end", style: `fill: var(--${token})` }, text));
}

/** A tiny line of ``values`` for a table cell. */
function sparkline(values, token = "ok") {
  const w = 90;
  const h = 16;
  const low = Math.min(...values);
  const high = Math.max(...values);
  const y = (v) => (high === low ? h / 2 : h - 2 - ((v - low) / (high - low)) * (h - 4));
  return sv(
    "svg",
    { width: w, height: h },
    sv("polyline", { points: values.map((v, i) => `${(i / (values.length - 1)) * (w - 2) + 1},${y(v)}`).join(" "), fill: "none", stroke: colour(token), "stroke-width": 1.5 }),
  );
}
