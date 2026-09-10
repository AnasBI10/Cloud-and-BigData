"""Serving-API der NYC Congestion Watch (SCRUM-79).

Drei Lese-Endpunkte fuer das Dashboard und zwei Probes fuer Kubernetes.

Getrennte Liveness und Readiness, weil sie verschiedene Fragen beantworten:
/health sagt "der Prozess laeuft" (ein Neustart wuerde helfen), /ready sagt
"die Gold-Schicht ist lesbar" (ein Neustart wuerde nichts helfen, der
Spark-Job hat nur noch nichts geschrieben). Waere beides derselbe Endpunkt,
wuerde Kubernetes die API in einer Neustartschleife halten, solange SCRUM-86
noch nicht liefert.

Dazu kommen zwei schreibende Endpunkte fuer die Rolle des Datenlieferanten
(SCRUM-89): POST /api/events und POST /api/scenarios. Beide schreiben NICHT in
die Gold-Schicht, sondern nach Kafka — ein hier erzeugtes Event nimmt denselben
Weg wie eine echte DOT-Messung. Warum das so sein muss, steht in publisher.py.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Query, Response, status as http_status
from fastapi.middleware.cors import CORSMiddleware

from config import Settings, setup_logging
from models import (
    AnomalyResponse,
    EventAck,
    EventRequest,
    Health,
    ScenarioListResponse,
    ScenarioRequest,
    ScenarioStatus,
    SegmentsResponse,
    SegmentWindow,
    TimeseriesResponse,
)
from publisher import (
    DEFAULT_REFERENCE_SPEED_MPH,
    IngestError,
    ScenarioRun,
    ScenarioRunner,
    build_ingest,
    load_build_event,
    scenario_catalog,
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
    state["by_id"] = {seg["link_id"]: seg for seg in state["seed"]}

    # Einspeisung (SCRUM-89). Der Producer verbindet sich erst beim ersten
    # POST — sonst haengt der Start der Lese-API an der Verfuegbarkeit von
    # Kafka, und das Dashboard bliebe dunkel, weil das Formular nicht kann.
    state["ingest"] = build_ingest(settings.ingest_mode)
    state["scenarios"] = ScenarioRunner(state["ingest"], load_build_event())

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
    # POST fuer die Einspeisung (SCRUM-89). Weiterhin kein PUT/DELETE: die
    # API kennt keine Ressourcen, die sich aendern oder loeschen liessen.
    allow_methods=["GET", "POST"],
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
    return Health(
        status="ok", reader=state["reader"].name, ingest=state["ingest"].name
    )


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
        ingest=state["ingest"].name,
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


# ---------------------------------------------------------------------------
# Einspeisung — Rolle Datenlieferant (SCRUM-89)
# ---------------------------------------------------------------------------


def _segment_or_404(link_id: str) -> dict:
    segment = state["by_id"].get(link_id)
    if segment is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Unbekannte link_id {link_id}. Erlaubt sind die "
                f"{len(state['seed'])} Segmente aus data/dot_links_seed.json — "
                "erfundene IDs wuerden zwar durch Kafka laufen, aber nie in der "
                "Karte auftauchen."
            ),
        )
    return segment


@app.post(
    "/api/events",
    response_model=EventAck,
    status_code=http_status.HTTP_201_CREATED,
    tags=["ingest"],
)
def publish_event(request: EventRequest) -> EventAck:
    """Ein einzelnes Messereignis nach Kafka.

    Der Weg ist derselbe wie beim Live-Poller: Avro gegen die Schema-Registry,
    Topic ``traffic.speeds.raw``, dann Spark. Die Antwort kommt erst, wenn der
    Broker die Zustellung bestaetigt hat — eine Quittung, die nur besagt, dass
    etwas in einen Puffer gelegt wurde, waere wertlos.

    Sichtbar wird das Event dadurch noch nicht: ein einzelner Wert verschiebt
    einen Fenstermittelwert kaum. Dafuer gibt es POST /api/scenarios.
    """
    settings: Settings = state["settings"]
    segment = _segment_or_404(request.link_id)
    now = datetime.now(timezone.utc)
    data_as_of = request.data_as_of or now
    if data_as_of.tzinfo is None:
        data_as_of = data_as_of.replace(tzinfo=timezone.utc)

    if data_as_of > now + timedelta(seconds=60):
        raise HTTPException(
            status_code=422,
            detail=(
                "data_as_of liegt in der Zukunft. Der Spark-Job wuerde das "
                "Event in ein Fenster einsortieren, das noch nicht begonnen "
                "hat, und die Watermark aller anderen Segmente mitziehen."
            ),
        )

    age_s = (now - data_as_of).total_seconds()
    late = age_s > settings.watermark_delay_s
    if late and not request.allow_late:
        raise HTTPException(
            status_code=422,
            detail=(
                f"data_as_of ist {int(age_s)} s alt, die Watermark des "
                f"Spark-Jobs liegt bei {settings.watermark_delay_s} s. Das "
                "Event wuerde aus der Aggregation ausgeschlossen und in die "
                "DLQ geschrieben, im Dashboard passierte nichts. Mit "
                "allow_late=true ist genau das gewollt und erlaubt."
            ),
        )

    status_code = request.status
    speed = request.speed_mph
    travel_time = request.travel_time_s
    note = "Event im Topic. Sichtbar, sobald der Spark-Job das Fenster schliesst."

    if status_code == -101:
        # Das Sentinel-Triplett des echten Feeds: status=-101 tritt praktisch
        # immer mit speed=0 und travel_time=0 auf (DATA_SOURCES.md). Ein
        # Sentinel mit 45 mph gaebe es im Feed nicht, und der Statusfilter
        # liesse sich damit nicht ehrlich vorfuehren.
        speed, travel_time = 0.0, 0
        note = (
            "Sentinel-Event. Der Statusfilter des Spark-Jobs verwirft es in "
            "Silver — es erreicht die Gold-Schicht bewusst nie."
        )
    elif speed is None:
        raise HTTPException(
            status_code=422,
            detail="Bei status=0 ist speed_mph erforderlich — eine gueltige "
            "Messung ohne Messwert gibt es nicht.",
        )

    if late:
        note = (
            "Verspaetetes Event. Geht in die DLQ (traffic.speeds.dlq), nicht "
            "in die Aggregation — der Weg aus SCRUM-85."
        )

    event = load_build_event()(
        link_id=request.link_id,
        data_as_of=data_as_of,
        status=status_code,
        speed_mph=speed,
        travel_time_s=travel_time,
        borough=segment.get("borough"),
        link_name=segment.get("link_name"),
        link_points=None,
        # Das Avro-Enum kennt DOT_LIVE, SYNTHETIC und REPLAY. Ein eigenes
        # Symbol "UI" waere eine nicht abwaertskompatible Schema-Aenderung
        # (bestehende Leser kennen es nicht) — dafuer ist der Nutzen zu klein.
        # SYNTHETIC trifft es: von Hand erzeugt, nicht aus dem Feed.
        source="SYNTHETIC",
    )

    ingest = state["ingest"]
    try:
        ingest.publish(event)
        ingest.flush()
    except IngestError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return EventAck(
        published=True,
        ingest=ingest.name,
        topic=ingest.topic,
        link_id=request.link_id,
        event_key=event["event_key"],
        data_as_of=data_as_of,
        status=status_code,
        speed_mph=speed,
        late=late,
        note=note,
    )


@app.post(
    "/api/scenarios",
    response_model=ScenarioStatus,
    status_code=http_status.HTTP_202_ACCEPTED,
    tags=["ingest"],
)
async def start_scenario(request: ScenarioRequest, response: Response) -> ScenarioStatus:
    """Eine Folge von Events ueber mehrere Minuten, im Hintergrund.

    202 statt 201: der Lauf beginnt, ist aber noch nicht fertig. Er laeuft in
    Echtzeit und nicht als Stapel mit rueckdatierten Zeitstempeln — der
    Spark-Job wuerde rueckdatierte Events als verspaetet aussortieren, und im
    Dashboard bliebe alles beim Alten.
    """
    settings: Settings = state["settings"]
    segment = _segment_or_404(request.link_id)

    if request.duration_minutes > settings.max_scenario_minutes:
        raise HTTPException(
            status_code=422,
            detail=f"duration_minutes ueber dem Limit von "
            f"{settings.max_scenario_minutes} Minuten.",
        )
    if request.events_per_minute > settings.max_events_per_minute:
        raise HTTPException(
            status_code=422,
            detail=(
                f"events_per_minute ueber dem Limit von "
                f"{settings.max_events_per_minute}. Fuer Lasttests ist der "
                "synthetische Producer zustaendig (SCRUM-93), nicht die UI."
            ),
        )

    reference = request.reference_speed_mph
    if reference is None:
        # Ohne Baseline kein segmentspezifischer Ausgangswert: dann lieber ein
        # offen benannter Ersatzwert als ein aus der letzten Messung geratener.
        reference = state["reader"].reference_speed(
            request.link_id, datetime.now(timezone.utc)
        ) or DEFAULT_REFERENCE_SPEED_MPH

    ingest = state["ingest"]
    ok, detail = ingest.probe()
    if not ok:
        raise HTTPException(status_code=503, detail=detail)

    run = ScenarioRun(
        scenario=request.scenario,
        link_id=request.link_id,
        duration_minutes=request.duration_minutes,
        events_per_minute=request.events_per_minute,
        reference_speed=reference,
    )
    state["scenarios"].start(run, segment)
    log.info(
        "Szenario %s gestartet: %s auf %s, %d Events ueber %d Minuten",
        run.id, run.scenario, run.link_id, run.planned_events, run.duration_minutes,
    )
    response.headers["Location"] = f"/api/scenarios/{run.id}"
    return ScenarioStatus(**run.as_dict(), ingest=ingest.name)


@app.get("/api/scenarios", response_model=ScenarioListResponse, tags=["ingest"])
def list_scenarios() -> ScenarioListResponse:
    """Katalog der Szenarien und die Laeufe dieses Pods.

    Die Laufliste ist absichtlich nicht geteilt: sie ist die Quittung eines
    Knopfdrucks, kein Betriebszustand, und waere geteilt nur um den Preis
    eines weiteren zustandsbehafteten Dienstes zu haben.
    """
    runner: ScenarioRunner = state["scenarios"]
    runs = sorted(runner.runs.values(), key=lambda r: r.started_at, reverse=True)
    return ScenarioListResponse(
        available=scenario_catalog(),
        runs=[ScenarioStatus(**r.as_dict(), ingest=state["ingest"].name) for r in runs],
    )


@app.get("/api/scenarios/{scenario_id}", response_model=ScenarioStatus, tags=["ingest"])
def scenario_status(scenario_id: str) -> ScenarioStatus:
    runner: ScenarioRunner = state["scenarios"]
    run = runner.runs.get(scenario_id)
    if run is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Kein Lauf {scenario_id} auf diesem Pod. Bei mehreren "
                "Repliken kennt ihn nur der Pod, der ihn faehrt."
            ),
        )
    return ScenarioStatus(**run.as_dict(), ingest=state["ingest"].name)
