const base = (window.APP_CONFIG && window.APP_CONFIG.apiBase) || "";

export const apiBase = base;

export class ApiError extends Error {
  constructor(message, status, payload) {
    super(message);
    this.status = status;
    this.payload = payload;
  }
}

async function request(path, options = {}) {
  let response;
  try {
    response = await fetch(base + path, options);
  } catch (cause) {
    throw new ApiError("Serving-API nicht erreichbar (" + (base || "gleiche Herkunft") + ")", 0, null);
  }

  const text = await response.text();
  let payload = null;
  try {
    payload = text ? JSON.parse(text) : null;
  } catch {
    payload = text;
  }

  if (!response.ok) {
    throw new ApiError(detailOf(payload) || response.statusText, response.status, payload);
  }
  return payload;
}

// FastAPI liefert Fehler als {detail: ...}; bei Validierungsfehlern ist detail eine Liste.
function detailOf(payload) {
  if (!payload) return null;
  const detail = payload.detail !== undefined ? payload.detail : payload;
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    return detail
      .map((e) => (e.loc ? e.loc.slice(1).join(".") + ": " : "") + (e.msg || JSON.stringify(e)))
      .join("; ");
  }
  if (detail && detail.detail) return detail.detail;
  return JSON.stringify(detail);
}

const json = { "Content-Type": "application/json" };

export const api = {
  health: () => request("/health"),
  segments: () => request("/api/segments"),
  anomalies: (params = {}) =>
    request("/api/anomalies?" + new URLSearchParams(params).toString()),
  timeseries: (linkId, hours) =>
    request(`/api/segments/${encodeURIComponent(linkId)}/timeseries?hours=${hours}`),
  publishEvent: (body) =>
    request("/api/events", { method: "POST", headers: json, body: JSON.stringify(body) }),
  startScenario: (body) =>
    request("/api/scenarios", { method: "POST", headers: json, body: JSON.stringify(body) }),
  scenarios: () => request("/api/scenarios"),
  scenario: (id) => request(`/api/scenarios/${encodeURIComponent(id)}`),
};

export const nyc = new Intl.DateTimeFormat("de-DE", {
  timeZone: "America/New_York",
  hour: "2-digit",
  minute: "2-digit",
});

export function timeNYC(iso) {
  return iso ? nyc.format(new Date(iso)) + " NYC" : "—";
}

export function fillSegmentSelect(select, segments) {
  select.innerHTML = "";
  const byBorough = new Map();
  for (const s of segments) {
    const key = s.borough || "ohne Borough";
    if (!byBorough.has(key)) byBorough.set(key, []);
    byBorough.get(key).push(s);
  }
  for (const borough of [...byBorough.keys()].sort()) {
    const group = document.createElement("optgroup");
    group.label = borough;
    for (const s of byBorough.get(borough).sort((a, b) => (a.link_name || "").localeCompare(b.link_name || ""))) {
      const option = document.createElement("option");
      option.value = s.link_id;
      option.textContent = s.link_name || s.link_id;
      group.appendChild(option);
    }
    select.appendChild(group);
  }
}

export async function showApiStatus(el) {
  try {
    const h = await api.health();
    const dry = h.ingest === "dryrun";
    el.textContent = `API ok · ${h.reader} · ${h.ingest}`;
    el.className = "status " + (dry ? "warn" : "ok");
    el.title = dry
      ? "INGEST_MODE=dryrun — Events werden NICHT nach Kafka geschrieben"
      : "Lesequelle und Einspeisemodus der Serving-API";
  } catch (err) {
    el.textContent = "API nicht erreichbar";
    el.className = "status err";
    el.title = err.message;
  }
}
