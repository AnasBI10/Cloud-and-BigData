import { api, showApiStatus, timeNYC } from "./api.js";

const $ = (id) => document.getElementById(id);

const POLL_MS = 20000;
const MIN_SCORE = 2.0;

const MAP = { w: 1000, h: 700, pad: 24 };
const CHART = { w: 1000, h: 320, l: 46, r: 12, t: 26, b: 26 };
const WINDOW_MS = 5 * 60 * 1000;

let segments = [];
let segmentById = new Map();
let paths = new Map();
let selected = null;
let project = null;

async function init() {
  showApiStatus($("api-status"));
  setInterval(() => showApiStatus($("api-status")), 30000);

  $("borough-filter").addEventListener("change", refreshRanking);
  $("chart-hours").addEventListener("change", () => selected && selectSegment(selected));

  await loadSegments();
  await refreshRanking();

  setInterval(async () => {
    await loadSegments({ rebuild: false });
    await refreshRanking();
    if (selected) await selectSegment(selected, { keepScroll: true });
  }, POLL_MS);
}

async function loadSegments({ rebuild = true } = {}) {
  let data;
  try {
    data = await api.segments();
  } catch (err) {
    $("map-counts").textContent = "Segmente nicht ladbar: " + err.message;
    return;
  }
  segments = data.items;
  segmentById = new Map(segments.map((s) => [s.link_id, s]));

  if (rebuild) {
    buildBoroughFilter();
    buildMap();
  }
  paintMap();
  updateCounts(data.generated_at);
}

function buildBoroughFilter() {
  const select = $("borough-filter");
  const boroughs = [...new Set(segments.map((s) => s.borough).filter(Boolean))].sort();
  for (const b of boroughs) {
    const option = document.createElement("option");
    option.value = b;
    option.textContent = b;
    select.appendChild(option);
  }
}

// Web-Mercator, skaliert auf die Bounding Box der tatsaechlichen Segmente.
function makeProjection(points) {
  const merc = (lat) =>
    (180 / Math.PI) * Math.log(Math.tan(Math.PI / 4 + (lat * Math.PI) / 180 / 2));
  const lons = points.map((p) => p[1]);
  const ys = points.map((p) => merc(p[0]));
  const [lon0, lon1] = [Math.min(...lons), Math.max(...lons)];
  const [y0, y1] = [Math.min(...ys), Math.max(...ys)];

  const inner = { w: MAP.w - 2 * MAP.pad, h: MAP.h - 2 * MAP.pad };
  const scale = Math.min(inner.w / (lon1 - lon0), inner.h / (y1 - y0));
  const offX = MAP.pad + (inner.w - (lon1 - lon0) * scale) / 2;
  const offY = MAP.pad + (inner.h - (y1 - y0) * scale) / 2;

  return (lat, lon) => [
    offX + (lon - lon0) * scale,
    offY + (y1 - merc(lat)) * scale,
  ];
}

function parsePoints(raw) {
  const out = [];
  for (const pair of (raw || "").split(/\s+/)) {
    const [lat, lon] = pair.split(",").map(Number);
    if (Number.isFinite(lat) && Number.isFinite(lon)) out.push([lat, lon]);
  }
  return out;
}

function buildMap() {
  const svg = $("map");
  svg.innerHTML = "";
  paths = new Map();

  const geo = segments.map((s) => ({ s, pts: parsePoints(s.link_points) }));
  const usable = geo.filter((g) => g.pts.length >= 2);
  if (!usable.length) {
    svg.innerHTML =
      '<text x="500" y="340" text-anchor="middle" class="map-empty">' +
      "Keine Segmentgeometrie vorhanden (link_points leer)." +
      "</text>";
    return;
  }

  project = makeProjection(usable.flatMap((g) => g.pts));

  for (const { s, pts } of usable) {
    const d = pts
      .map((p, i) => (i ? "L" : "M") + project(p[0], p[1]).map((n) => n.toFixed(1)).join(" "))
      .join(" ");
    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path.setAttribute("d", d);
    path.setAttribute("class", "seg");
    path.dataset.linkId = s.link_id;
    path.addEventListener("click", () => selectSegment(s.link_id));
    path.addEventListener("mousemove", (e) => showTip(e, s.link_id));
    path.addEventListener("mouseleave", hideTip);
    svg.appendChild(path);
    paths.set(s.link_id, path);
  }
}

function stateOf(segment) {
  if (!segment.has_baseline) return "nobase";
  if (segment.last_seen === null || segment.last_score === null) return "stale";
  const score = segment.last_score;
  if (score >= 3) return "s3";
  if (score >= 2) return "s2";
  if (score >= 1) return "s1";
  if (score >= 0) return "s0";
  return "sneg";
}

function paintMap() {
  for (const [linkId, path] of paths) {
    const segment = segmentById.get(linkId);
    if (!segment) continue;
    path.setAttribute(
      "class",
      "seg st-" + stateOf(segment) + (linkId === selected ? " is-selected" : "")
    );
  }
}

function updateCounts(generatedAt) {
  const total = segments.length;
  const nobase = segments.filter((s) => !s.has_baseline).length;
  const measured = segments.filter((s) => s.last_seen !== null).length;
  $("map-counts").textContent =
    `${total} Segmente · ${measured} mit aktueller Messung · ${nobase} unbewertbar`;
  $("freshness").textContent =
    `Stand ${timeNYC(generatedAt)} · Aktualisierung alle ${POLL_MS / 1000} s`;
}

function showTip(event, linkId) {
  const s = segmentById.get(linkId);
  const tip = $("map-tip");
  const score = s.last_score === null ? null : s.last_score.toFixed(2);
  tip.innerHTML =
    `<strong>${escapeHtml(s.link_name || s.link_id)}</strong>` +
    `<span>${escapeHtml(s.borough || "—")}</span>` +
    (s.has_baseline
      ? s.last_seen
        ? `<span>${s.last_speed} mph · Score ${score}</span>`
        : "<span>keine aktuelle Messung</span>"
      : "<span>unbewertbar — keine Baseline</span>");
  const box = $("map-wrap").getBoundingClientRect();
  tip.style.left = Math.min(event.clientX - box.left + 12, box.width - 200) + "px";
  tip.style.top = event.clientY - box.top + 12 + "px";
  tip.hidden = false;
}

function hideTip() {
  $("map-tip").hidden = true;
}

async function refreshRanking() {
  const borough = $("borough-filter").value;
  const params = { limit: 10, min_score: MIN_SCORE };
  if (borough) params.borough = borough;

  let data;
  try {
    data = await api.anomalies(params);
  } catch (err) {
    $("rank-meta").textContent = "Rangliste nicht ladbar: " + err.message;
    return;
  }

  $("rank-meta").textContent =
    `Fenster ${timeNYC(data.latest_window)} · ${data.total_segments} Segmente gemessen, ` +
    `davon ${data.segments_with_baseline} bewertbar und ` +
    `${data.segments_without_baseline} ohne Baseline · Schwelle ${MIN_SCORE} σ`;

  const list = $("rank-list");
  list.innerHTML = "";
  if (!data.items.length) {
    const li = document.createElement("li");
    li.className = "rank-empty";
    li.textContent =
      data.segments_with_baseline === 0
        ? "Kein Segment ist derzeit bewertbar — es fehlt die Baseline."
        : `Kein Segment über ${MIN_SCORE} σ. Das ist ein Befund, kein Fehler.`;
    list.appendChild(li);
    return;
  }

  for (const item of data.items) {
    const li = document.createElement("li");
    li.className = "rank-item" + (item.link_id === selected ? " is-selected" : "");
    li.innerHTML =
      `<span class="score st-${stateOf({ has_baseline: true, last_seen: 1, last_score: item.congestion_score })}">` +
      `${item.congestion_score.toFixed(1)}<small>σ</small></span>` +
      `<span class="rank-body"><strong>${escapeHtml(
        segmentById.get(item.link_id)?.link_name || item.link_id
      )}</strong>` +
      `<small>${escapeHtml(item.borough || "—")} · ${item.speed_avg} mph statt ` +
      `${item.baseline_speed} mph erwartet · ${item.sample_count} Messungen</small></span>`;
    li.addEventListener("click", () => selectSegment(item.link_id));
    list.appendChild(li);
  }
}

async function selectSegment(linkId, { keepScroll = false } = {}) {
  selected = linkId;
  paintMap();
  for (const li of document.querySelectorAll(".rank-item")) li.classList.remove("is-selected");

  const segment = segmentById.get(linkId);
  $("chart-title").textContent = segment?.link_name || linkId;
  const hours = Number($("chart-hours").value);

  let data;
  try {
    data = await api.timeseries(linkId, hours);
  } catch (err) {
    $("chart-meta").textContent = "Zeitreihe nicht ladbar: " + err.message;
    $("chart").innerHTML = "";
    return;
  }

  if (!data.has_baseline) {
    $("chart-meta").textContent =
      `${data.borough || "—"} · ohne Baseline: die gemessene Geschwindigkeit ` +
      "steht da, ein Vergleichswert existiert nicht. Deshalb kein Score.";
  } else {
    $("chart-meta").textContent =
      `${data.borough || "—"} · ${data.points.length} Fenster in ${hours} h · ` +
      "Lücken sind fehlende Messungen, nicht interpoliert.";
  }
  renderChart(data.points);
  if (!keepScroll) $("chart-wrap").scrollIntoView({ behavior: "smooth", block: "nearest" });
}

function renderChart(points) {
  const svg = $("chart");
  svg.innerHTML = "";
  if (!points.length) {
    svg.innerHTML =
      '<text x="500" y="160" text-anchor="middle" class="map-empty">' +
      "Keine Fenster im gewählten Zeitraum." +
      "</text>";
    return;
  }

  const rows = points.map((p) => ({
    t: new Date(p.window_start).getTime(),
    speed: p.speed_avg,
    base: p.baseline_speed,
    sd: p.baseline_stddev,
  }));

  const t0 = rows[0].t;
  const t1 = rows[rows.length - 1].t;
  const values = rows.flatMap((r) =>
    [r.speed, r.base, r.base !== null && r.sd !== null ? r.base + r.sd : null].filter(
      (v) => v !== null && v !== undefined
    )
  );
  const vMax = Math.max(...values) * 1.1;
  const vMin = 0;

  const x = (t) => CHART.l + ((t - t0) / Math.max(1, t1 - t0)) * (CHART.w - CHART.l - CHART.r);
  const y = (v) =>
    CHART.t + (1 - (v - vMin) / (vMax - vMin)) * (CHART.h - CHART.t - CHART.b);

  const ns = "http://www.w3.org/2000/svg";
  const add = (tag, attrs, cls) => {
    const el = document.createElementNS(ns, tag);
    for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
    if (cls) el.setAttribute("class", cls);
    svg.appendChild(el);
    return el;
  };

  for (let i = 0; i <= 4; i++) {
    const v = vMin + ((vMax - vMin) * i) / 4;
    add("line", { x1: CHART.l, x2: CHART.w - CHART.r, y1: y(v), y2: y(v) }, "grid");
    add("text", { x: CHART.l - 6, y: y(v) + 4, "text-anchor": "end" }, "axis").textContent =
      v.toFixed(0);
  }
  const span = t1 - t0;
  const withDate = span > 12 * 3600 * 1000;
  const label = (ms) =>
    new Intl.DateTimeFormat("de-DE", {
      timeZone: "America/New_York",
      ...(withDate ? { day: "2-digit", month: "2-digit" } : {}),
      hour: "2-digit",
      minute: "2-digit",
    }).format(new Date(ms));

  const TICKS = 5;
  for (let i = 0; i < TICKS; i++) {
    const t = t0 + (span * i) / (TICKS - 1);
    const anchor = i === 0 ? "start" : i === TICKS - 1 ? "end" : "middle";
    add("line", { x1: x(t), x2: x(t), y1: CHART.t, y2: y(vMin) }, "grid");
    add("text", { x: x(t), y: CHART.h - 6, "text-anchor": anchor }, "axis").textContent =
      label(t);
  }
  add("text", { x: 4, y: 12 }, "axis").textContent = "mph";

  // Luecken > 1.5 Fenster werden nicht ueberbrueckt.
  const runs = [];
  let run = [];
  for (const r of rows) {
    if (run.length && r.t - run[run.length - 1].t > WINDOW_MS * 1.5) {
      runs.push(run);
      run = [];
    }
    run.push(r);
  }
  if (run.length) runs.push(run);

  for (const seg of runs) {
    const band = seg.filter((r) => r.base !== null && r.sd !== null);
    if (band.length >= 2) {
      const top = band.map((r) => `${x(r.t).toFixed(1)} ${y(r.base + r.sd).toFixed(1)}`);
      const bottom = band
        .slice()
        .reverse()
        .map((r) => `${x(r.t).toFixed(1)} ${y(Math.max(0, r.base - r.sd)).toFixed(1)}`);
      add("path", { d: `M${top.join("L")}L${bottom.join("L")}Z` }, "band");
    }
    const base = seg.filter((r) => r.base !== null);
    if (base.length >= 2) {
      add(
        "path",
        { d: "M" + base.map((r) => `${x(r.t).toFixed(1)} ${y(r.base).toFixed(1)}`).join("L") },
        "line-base"
      );
    }
    const speed = seg.filter((r) => r.speed !== null);
    if (speed.length >= 2) {
      add(
        "path",
        { d: "M" + speed.map((r) => `${x(r.t).toFixed(1)} ${y(r.speed).toFixed(1)}`).join("L") },
        "line-speed"
      );
    } else if (speed.length === 1) {
      add("circle", { cx: x(speed[0].t), cy: y(speed[0].speed), r: 2.5 }, "dot-speed");
    }
  }
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  })[c]);
}

init();
