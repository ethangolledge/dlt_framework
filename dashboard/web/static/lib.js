/* dlt//ops, part 1 of 3: the ground the rest stands on. Loaded first, as a classic script (a page
   opened from disk may not load modules), so its top-level names are shared by components.js and
   pages.js.

     the data     window.DASH_DATA (data.js) indexed; window.DASH_LOOK (theme.js): states and outcomes
     the URL      the route, what is selected, and every filter live after the #, so a view can be
                  bookmarked or shared
     elements     el() and sv() build HTML and SVG from props and children
     numbers      human(), took(), mb(), when(), ago(), edge(): one way to write each kind of value

   Everything is UTC. */

"use strict";

const D = window.DASH_DATA;
const LOOK = window.DASH_LOOK;
const SVG_NS = "http://www.w3.org/2000/svg";
const DAY_MS = 86400000;

// ---- the data, indexed ----------------------------------------------------------------------

const STATE_ORDER = Object.keys(LOOK.states); // worst first
const pipelines = D.pipelines
  .slice()
  .sort((a, b) => STATE_ORDER.indexOf(a.state) - STATE_ORDER.indexOf(b.state) || (a.id < b.id ? -1 : 1));
const pipelineById = Object.fromEntries(pipelines.map((p) => [p.id, p]));
const runs = D.runs.slice().sort((a, b) => (a.started < b.started ? 1 : -1)); // newest first
const runById = Object.fromEntries(runs.map((r) => [r.id, r]));
const runsOf = {};
for (const run of runs) (runsOf[run.pipeline] = runsOf[run.pipeline] || []).push(run);
const today = D.generated.slice(0, 10);
const firstDay = runs.length ? runs[runs.length - 1].started.slice(0, 10) : today;

/** How a run reads: its outcome, or "empty" for a success that loaded no rows. */
function look(run) {
  return run.empty ? "empty" : run.outcome;
}

function broken(run) {
  return run.outcome === "failed" || run.outcome === "lost";
}

function sum(list, value) {
  return list.reduce((total, item) => total + (value(item) || 0), 0);
}

// ---- the URL --------------------------------------------------------------------------------

const DEFAULTS = { from: shift(today, -(D.days - 1)), to: today, mode: "all", q: "", pipeline: "", outcome: "" };

function read() {
  const [path, query] = location.hash.replace(/^#\/?/, "").split("?");
  const [route, ...rest] = (path || "overview").split("/");
  const params = new URLSearchParams(query || "");
  const view = { route: route || "overview", id: rest.length ? decodeURIComponent(rest.join("/")) : null };
  for (const [key, value] of Object.entries(DEFAULTS)) view[key] = params.get(key) || value;
  return view;
}

/** A link to ``route``: the days and mode carry over, page filters do not unless changed. */
function link(route, id, changes) {
  const now = read();
  const keep = { from: now.from, to: now.to, mode: now.mode, ...(changes || {}) };
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(keep)) if (value && value !== DEFAULTS[key]) params.set(key, value);
  const text = params.toString();
  return `#/${route}${id ? "/" + encodeURIComponent(id) : ""}${text ? "?" + text : ""}`;
}

function go(route, id, changes) {
  location.hash = link(route, id, changes);
}

/** The current page again, with ``changes`` to its filters. */
function refilter(changes) {
  const view = read();
  go(view.route, view.id, { q: view.q, pipeline: view.pipeline, outcome: view.outcome, ...changes });
}

function inRange(run, view) {
  const day = run.started.slice(0, 10);
  return day >= view.from && day <= view.to && (view.mode === "all" || run.mode === view.mode);
}

function matches(text, q) {
  return !q || text.toLowerCase().includes(q.toLowerCase());
}

// ---- elements -------------------------------------------------------------------------------

function el(tag, props, ...kids) {
  return build(document.createElement(tag), props, kids);
}

function sv(tag, props, ...kids) {
  return build(document.createElementNS(SVG_NS, tag), props, kids);
}

function build(node, props, kids) {
  for (const [key, value] of Object.entries(props || {})) {
    if (value === null || value === undefined || value === false) continue;
    if (key === "tip") tip(node, value);
    else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value);
  }
  for (const kid of kids.flat(Infinity)) {
    if (kid === null || kid === undefined || kid === false) continue;
    node.append(kid instanceof Node ? kid : String(kid));
  }
  return node;
}

function tip(node, text) {
  const box = document.getElementById("tip");
  node.addEventListener("mousemove", (event) => {
    box.textContent = text;
    box.hidden = false;
    box.style.left = `${Math.min(event.clientX + 14, window.innerWidth - box.offsetWidth - 8)}px`;
    box.style.top = `${Math.min(event.clientY + 14, window.innerHeight - box.offsetHeight - 8)}px`;
  });
  node.addEventListener("mouseleave", () => (box.hidden = true));
}

function colour(token) {
  return getComputedStyle(document.documentElement).getPropertyValue(`--${token}`).trim();
}

/** The inside width of a panel: the page, less its padding and the panel's. */
function width() {
  return Math.max(480, document.getElementById("page").clientWidth - 64);
}

// ---- numbers and times ----------------------------------------------------------------------

function human(value) {
  if (value === null || value === undefined) return "-";
  for (const [size, unit] of [[1e9, "B"], [1e6, "M"], [1e3, "K"]])
    if (Math.abs(value) >= size) return (value / size).toFixed(1) + unit;
  return String(Math.round(value));
}

function took(seconds) {
  if (seconds === null || seconds === undefined) return "-";
  if (seconds === 0) return "0s";
  const whole = Math.floor(seconds);
  const two = (n) => String(n).padStart(2, "0");
  if (whole >= 86400) return `${Math.floor(whole / 86400)}d ${Math.floor((whole % 86400) / 3600)}h`;
  if (whole >= 3600) return `${Math.floor(whole / 3600)}h${two(Math.floor((whole % 3600) / 60))}m`;
  if (whole >= 60) return `${Math.floor(whole / 60)}m${two(whole % 60)}s`;
  return seconds < 10 ? seconds.toFixed(1) + "s" : whole + "s";
}

function mb(value) {
  if (value === null || value === undefined) return "-";
  if (value < 1024) return Math.round(value) + "M";
  return (value / 1024).toFixed(2).replace(/0+$/, "").replace(/\.$/, "") + "G";
}

function pct(share) {
  return share === null || share === undefined ? "-" : `${Math.round(share * 100)}%`;
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
  return new Date(Date.parse(day + "T00:00:00Z") + days * DAY_MS).toISOString().slice(0, 10);
}

function daysBetween(from, to) {
  const out = [];
  for (let day = from; day <= to && out.length < 4000; day = shift(day, 1)) out.push(day);
  return out;
}

/** A window edge or cursor value, short: an ISO instant as ``2026-09-30 00:00``, else as is. */
function edge(text) {
  if (!text) return "-";
  const moment = Date.parse(text);
  return Number.isNaN(moment) || !/\d{4}-\d{2}-\d{2}/.test(text) ? text : when(new Date(moment).toISOString());
}

function plural(count, word) {
  return `${count} ${word}${count === 1 ? "" : "s"}`;
}
