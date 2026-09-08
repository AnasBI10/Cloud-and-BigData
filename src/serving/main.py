"""Serving-API der NYC Congestion Watch (SCRUM-79).

Drei Lese-Endpunkte fuer das Dashboard und zwei Probes fuer Kubernetes.

Getrennte Liveness und Readiness, weil sie verschiedene Fragen beantworten:
/health sagt "der Prozess laeuft" (ein Neustart wuerde helfen), /ready sagt
"die Gold-Schicht ist lesbar" (ein Neustart wuerde nichts helfen, der
Spark-Job hat nur noch nichts geschrieben). Waere beides derselbe Endpunkt,
wuerde Kubernetes die API in einer Neustartschleife halten, solange SCRUM-86
noch nicht liefert.

Die Einspeisung von UI-Events (POST /api/events) gehoert zu SCRUM-89 und ist
hier bewusst noch nicht enthalten.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from config import Settings, setup_logging
from models import (
    AnomalyResponse,
    Health,
    SegmentsResponse,
    SegmentWindow,
    TimeseriesResponse,
)
from readers import ReaderError, TTLCache, build_reader, load_seed, segments_from

log = logging.getLogger("serving.api")

state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    settings = Settings.from_env()
    state["settings"] = settings
    state["reader"] = build_reader(settings)
    state["seed"] = load_seed(settings)
    state["cache"] = TTLCache(settings.cache_ttl_s)

    ok, detail = state["reader"].probe()
    log.info(
        "Start abgeschlossen. Gold-Schicht: %s (%s)",
        "ok" if ok else "nicht bereit",
        detail,
    )
    yield


app = FastAPI(
    title="NYC Congestion Watch — Serving API",
    version="0.1.0",
    description=(
        "Liest die Gold-Schicht des Delta Lakehouse und liefert Stau-Anomalien "
        "an das Dashboard."
    ),
    lifespan=lifespan,
)


@app.middleware("http")
async def add_attribution(request, call_next):
    """Open-Meteo steht unter CC BY 4.0, die Attribution ist Lizenzbedingung.
    Sie steht im Dashboard-Footer und zusaetzlich in jeder API-Antwort — damit
    sie auch bei direkter Nutzung der API nicht verloren geht."""
    response = await call_next(request)
    response.headers["X-Data-Attribution"] = (
        "NYC Open Data (NYC DOT Traffic Speeds, i4gi-tjb9); "
        "Weather data by Open-Meteo.com (CC BY 4.0)"
    )
    return response


app.add_middleware(
    CORSMiddleware,
    allow_origins=Settings.from_env().cors_origins,
    allow_methods=["GET"],
    allow_headers=["*"],
)


def _latest() -> list[SegmentWindow]:
    return state["cache"].get(state["reader"].latest_windows)


# ---------------------------------------------------------------------------
# Probes
# ---------------------------------------------------------------------------


@app.get("/health", response_model=Health, tags=["ops"])
def health() -> Health:
    """Liveness. Prueft absichtlich NICHT die Gold-Schicht."""
    return Health(status="ok", reader=state["reader"].name)


@app.get("/ready", response_model=Health, tags=["ops"])
def ready() -> Health:
    """Readiness. Prueft die Gold-Schicht und nimmt den Pod bei Bedarf aus
    dem Service-Endpoint, ohne ihn neu zu starten."""
    ok, detail = state["reader"].probe()
    latest = None
    if ok:
        try:
            latest = max((w.window_start for w in _latest()), default=None)
        except ReaderError as exc:
            ok, detail = False, str(exc)

    payload = Health(
        status="ready" if ok else "not-ready",
        reader=state["reader"].name,
        detail=detail,
        latest_window=latest,
    )
    if not ok:
        raise HTTPException(status_code=503, detail=payload.model_dump(mode="json"))
    return payload


# ---------------------------------------------------------------------------
# Query-Endpunkte
# ---------------------------------------------------------------------------


@app.get("/api/anomalies", response_model=AnomalyResponse, tags=["gold"])
def anomalies(
    limit: int | None = Query(None, ge=1),
    min_score: float = Query(2.0, description="Schwelle in Standardabweichungen"),
    borough: str | None = Query(None),
) -> AnomalyResponse:
    """Top-N auffaellige Segmente des juengsten Fensters.

    Segmente ohne Baseline erscheinen NICHT in der Rangliste — sie sind
    unbewertbar, nicht unauffaellig. Ihre Zahl steht stattdessen im Umschlag,
    damit das Dashboard die Luecke sichtbar machen kann statt sie zu
    verschweigen.
    """
    settings: Settings = state["settings"]
    limit = min(limit or settings.default_limit, settings.max_limit)

    try:
        windows = _latest()
    except ReaderError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if borough:
        windows = [w for w in windows if (w.borough or "").lower() == borough.lower()]

    with_baseline = [
        w for w in windows if w.has_baseline and w.congestion_score is not None
    ]
    ranked = sorted(
        (w for w in with_baseline if w.congestion_score >= min_score),
        key=lambda w: w.congestion_score,
        reverse=True,
    )

    return AnomalyResponse(
        generated_at=datetime.now(timezone.utc),
        reader=state["reader"].name,
        latest_window=max((w.window_start for w in windows), default=None),
        total_segments=len(windows),
        segments_with_baseline=len(with_baseline),
        segments_without_baseline=len(windows) - len(with_baseline),
        items=ranked[:limit],
    )


@app.get(
    "/api/segments/{link_id}/timeseries",
    response_model=TimeseriesResponse,
    tags=["gold"],
)
def timeseries(link_id: str, hours: int = Query(24, ge=1, le=168)) -> TimeseriesResponse:
    """Zeitreihe eines Segments fuer den Chart im Dashboard."""
    seed = {s["link_id"]: s for s in state["seed"]}
    if link_id not in seed:
        raise HTTPException(status_code=404, detail=f"Unbekannte link_id {link_id}")

    try:
        points = state["reader"].timeseries(link_id, hours)
    except ReaderError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return TimeseriesResponse(
        link_id=link_id,
        link_name=seed[link_id].get("link_name"),
        borough=seed[link_id].get("borough"),
        has_baseline=bool(points and points[-1].has_baseline),
        hours=hours,
        points=points,
    )


@app.get("/api/segments", response_model=SegmentsResponse, tags=["gold"])
def segments() -> SegmentsResponse:
    """Kartengrundlage: alle Segmente des Seeds mit letztem bekannten Zustand."""
    try:
        windows = _latest()
    except ReaderError:
        # Die Karte soll auch ohne Gold-Daten zeichnen koennen — dann eben
        # ohne Farbe. Ein leeres Dashboard ist schlechter als ein graues.
        windows = []
    items = segments_from(state["seed"], windows)
    return SegmentsResponse(
        generated_at=datetime.now(timezone.utc), count=len(items), items=items
    )
