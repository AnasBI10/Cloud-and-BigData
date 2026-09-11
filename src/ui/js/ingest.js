import { api, ApiError, fillSegmentSelect, showApiStatus, timeNYC } from "./api.js";

const $ = (id) => document.getElementById(id);

let segments = [];
let segmentById = new Map();

async function init() {
  showApiStatus($("api-status"));
  setInterval(() => showApiStatus($("api-status")), 30000);

  await Promise.all([loadSegments(), loadScenarioCatalog()]);
  wireEventForm();
  wireScenarioForm();
}

async function loadSegments() {
  try {
    const data = await api.segments();
    segments = data.items;
    segmentById = new Map(segments.map((s) => [s.link_id, s]));
    fillSegmentSelect($("ev-link"), segments);
    fillSegmentSelect($("sc-link"), segments);
    updateSegmentContext();
  } catch (err) {
    for (const id of ["ev-link", "sc-link"]) {
      $(id).innerHTML = '<option value="">— Segmente nicht ladbar —</option>';
      $(id).disabled = true;
    }
    fail($("ev-result"), err);
  }
}

async function loadScenarioCatalog() {
  try {
    const data = await api.scenarios();
    const select = $("sc-kind");
    select.innerHTML = "";
    for (const option of data.available) {
      const el = document.createElement("option");
      el.value = option.name;
      el.textContent = option.name;
      el.dataset.beschreibung = option.beschreibung;
      select.appendChild(el);
    }
    updateScenarioDescription();
  } catch (err) {
    fail($("sc-result"), err);
  }
}

function updateSegmentContext() {
  const segment = segmentById.get($("ev-link").value);
  const el = $("ev-context");
  if (!segment) {
    el.textContent = " ";
    return;
  }
  const parts = [`ID ${segment.link_id}`];
  if (segment.last_speed !== null && segment.last_speed !== undefined) {
    parts.push(`zuletzt ${segment.last_speed} mph (${timeNYC(segment.last_seen)})`);
  } else {
    parts.push("keine aktuelle Messung");
  }
  parts.push(segment.has_baseline ? "Baseline vorhanden" : "ohne Baseline — unbewertbar");
  el.textContent = parts.join(" · ");
}

function wireEventForm() {
  $("ev-link").addEventListener("change", updateSegmentContext);

  const range = $("ev-speed-range");
  const number = $("ev-speed");
  range.addEventListener("input", () => (number.value = range.value));
  number.addEventListener("input", () => (range.value = number.value));

  for (const radio of document.querySelectorAll('input[name="status"]')) {
    radio.addEventListener("change", () => {
      const sentinel = radio.value === "-101" && radio.checked;
      $("ev-speed-row").hidden = sentinel;
      $("ev-speed").required = !sentinel;
    });
  }

  $("event-form").addEventListener("submit", submitEvent);
}

async function submitEvent(event) {
  event.preventDefault();
  const button = event.target.querySelector("button");
  const status = Number(document.querySelector('input[name="status"]:checked').value);
  const body = {
    link_id: $("ev-link").value,
    status,
    allow_late: $("ev-late").checked,
  };
  if (status === 0) body.speed_mph = Number($("ev-speed").value);
  if ($("ev-travel").value) body.travel_time_s = Number($("ev-travel").value);
  if ($("ev-time").value) {
    body.data_as_of = new Date($("ev-time").value).toISOString();
  }

  button.disabled = true;
  try {
    const ack = await api.publishEvent(body);
    const target = ack.late ? "traffic.speeds.dlq" : ack.topic;
    ok(
      $("ev-result"),
      `<strong>Event zugestellt</strong> → <code>${escapeHtml(target)}</code>` +
        (ack.ingest === "dryrun" ? ' <span class="tag warn">dry-run, nichts gesendet</span>' : "") +
        `<dl>
           <div><dt>event_key</dt><dd><code>${escapeHtml(ack.event_key)}</code></dd></div>
           <div><dt>status</dt><dd>${ack.status}</dd></div>
           <div><dt>speed_mph</dt><dd>${ack.speed_mph === null ? "—" : ack.speed_mph}</dd></div>
           <div><dt>Messzeit</dt><dd>${timeNYC(ack.data_as_of)}</dd></div>
         </dl>
         <p class="note">${escapeHtml(ack.note)}</p>`
    );
  } catch (err) {
    fail($("ev-result"), err);
  } finally {
    button.disabled = false;
  }
}

function updateScenarioDescription() {
  const option = $("sc-kind").selectedOptions[0];
  $("sc-desc").textContent = option ? option.dataset.beschreibung : " ";
}

function updatePlan() {
  const minutes = Number($("sc-minutes").value || 0);
  const rate = Number($("sc-rate").value || 0);
  $("sc-plan").textContent =
    `${minutes * rate} Events über ${minutes} Minuten. Sichtbar frühestens nach ` +
    "etwa eineinhalb Minuten.";
}

function wireScenarioForm() {
  $("sc-kind").addEventListener("change", updateScenarioDescription);
  $("sc-minutes").addEventListener("input", updatePlan);
  $("sc-rate").addEventListener("input", updatePlan);
  updatePlan();
  $("scenario-form").addEventListener("submit", startScenario);
}

async function startScenario(event) {
  event.preventDefault();
  const button = event.target.querySelector("button");
  button.disabled = true;
  try {
    const run = await api.startScenario({
      link_id: $("sc-link").value,
      scenario: $("sc-kind").value,
      duration_minutes: Number($("sc-minutes").value),
      events_per_minute: Number($("sc-rate").value),
    });
    renderRun(run);
    followRun(run, button);
  } catch (err) {
    fail($("sc-result"), err);
    button.disabled = false;
  }
}

function renderRun(run, note = "") {
  const done = run.published_events;
  const percent = Math.round((done / run.planned_events) * 100);
  const label =
    { running: "läuft", done: "abgeschlossen", failed: "fehlgeschlagen", cancelled: "abgebrochen" }[
      run.state
    ] || run.state;
  ok(
    $("sc-result"),
    `<strong>${escapeHtml(run.scenario)}</strong> auf ${escapeHtml(
      segmentById.get(run.link_id)?.link_name || run.link_id
    )} — ${label}` +
      (run.ingest === "dryrun" ? ' <span class="tag warn">dry-run, nichts gesendet</span>' : "") +
      `<div class="bar"><span style="width:${percent}%"></span></div>
       <p class="note">${done} von ${run.planned_events} Events · Ausgangswert
         ${run.reference_speed} mph · sichtbar ab ${timeNYC(run.expected_effect_at)}</p>` +
      (note ? `<p class="note">${escapeHtml(note)}</p>` : "") +
      (run.detail ? `<p class="note">${escapeHtml(run.detail)}</p>` : "")
  );
}

// Fortschritt kann bei mehreren Repliken auf einem anderen Pod landen (404).
function followRun(run, button) {
  const timer = setInterval(async () => {
    try {
      const current = await api.scenario(run.scenario_id);
      renderRun(current);
      if (current.state !== "running") {
        clearInterval(timer);
        button.disabled = false;
      }
    } catch (err) {
      clearInterval(timer);
      button.disabled = false;
      renderRun(
        run,
        err.status === 404
          ? "Fortschritt nicht abfragbar: der Lauf läuft auf einem anderen Pod. " +
              "Er läuft trotzdem weiter — im Dashboard nachsehen."
          : err.message
      );
    }
  }, 2000);
}

function ok(el, html) {
  el.className = "result ok";
  el.innerHTML = html;
}

function fail(el, err) {
  el.className = "result err";
  const status = err instanceof ApiError && err.status ? ` (HTTP ${err.status})` : "";
  el.innerHTML = `<strong>Nicht gesendet${status}</strong><p class="note">${escapeHtml(
    err.message
  )}</p>`;
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  })[c]);
}

init();
