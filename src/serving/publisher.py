"""Einspeisung von UI-Events in die Ingestion (SCRUM-89).

Ein hier erzeugtes Event geht durch denselben Pfad wie eine echte
DOT-Messung: Avro gegen die Schema-Registry, nach traffic.speeds.raw, von
dort in den Spark-Job und ueber Bronze/Silver/Gold zurueck ins Dashboard.

Importiert src/ingestion/common.py statt es nachzubauen — Serialisierung,
Registry-Anbindung und der Bau des event_key bleiben an einer Stelle.

Zwei Modi: INGEST_MODE=kafka (Abgabestand) und dryrun (Entwicklung ohne
Kafka, zaehlt nur mit, stellt nicht zu).
"""

from __future__ import annotations

import asyncio
import logging
import pathlib
import sys
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

log = logging.getLogger("serving.publisher")

# common.py liegt im Repo unter src/ingestion, im Image daneben unter /app.
# Muss vor dem Import unten stehen, damit common.py gefunden wird.
_CANDIDATES = [
    pathlib.Path(__file__).resolve().parent,
    pathlib.Path(__file__).resolve().parents[1] / "ingestion",
]
for _d in _CANDIDATES:
    if (_d / "common.py").exists() and str(_d) not in sys.path:
        sys.path.insert(0, str(_d))
        break

from common import EventPublisher, Settings as IngestSettings, build_event  # noqa: E402


class IngestError(RuntimeError):
    """Einspeisung nicht moeglich. Fuehrt zu 503."""


# Ein einzelnes Event verschiebt einen 5-Minuten-Fenstermittelwert kaum
# sichtbar — Szenarien erzeugen eine Folge von Events ueber mehrere Minuten.
SCENARIOS: dict[str, dict[str, Any]] = {
    "congestion": {
        "beschreibung": (
            "Geschwindigkeit fällt linear auf 30 % des Ausgangswerts. "
            "congestion_score steigt, das Segment wandert in die Rangliste."
        ),
        "status": 0,
        "speed_factor": lambda p: 1.0 - 0.7 * p,
    },
    "recovery": {
        "beschreibung": (
            "Gegenstück zu congestion: von 30 % zurück auf den Ausgangswert."
        ),
        "status": 0,
        "speed_factor": lambda p: 0.3 + 0.7 * p,
    },
    "sensor_outage": {
        "beschreibung": (
            "Serie mit status=-101 und speed=0 — das Sentinel-Triplett des "
            "echten Feeds. Der Statusfilter verwirft sie in Silver."
        ),
        "status": -101,
        "speed_factor": lambda p: 0.0,
    },
}

DEFAULT_REFERENCE_SPEED_MPH = 30.0


def scenario_catalog() -> list[dict[str, str]]:
    return [
        {"name": name, "beschreibung": spec["beschreibung"]}
        for name, spec in SCENARIOS.items()
    ]


class KafkaIngest:
    name = "kafka"

    def __init__(self) -> None:
        self._publisher = None
        self._settings = None
        self._lock = threading.Lock()

    def _ensure(self):
        with self._lock:
            if self._publisher is not None:
                return self._publisher
            try:
                self._settings = IngestSettings.from_env()
                self._publisher = EventPublisher(self._settings)
                log.info(
                    "Producer verbunden: %s, Topic %s, Registry %s",
                    self._settings.bootstrap,
                    self._settings.topic,
                    self._settings.schema_registry,
                )
            except Exception as exc:
                raise IngestError(f"Producer nicht verfuegbar: {exc}") from exc
            return self._publisher

    @property
    def topic(self) -> str:
        try:
            return self._ensure().settings.topic
        except IngestError:
            return "traffic.speeds.raw"

    def publish(self, event: dict) -> None:
        publisher = self._ensure()
        try:
            publisher.publish(event)
        except Exception as exc:
            raise IngestError(f"Zustellung fehlgeschlagen: {exc}") from exc

    def flush(self) -> tuple[int, int]:
        publisher = self._ensure()
        publisher.flush()
        return publisher.stats

    def probe(self) -> tuple[bool, str]:
        try:
            self._ensure()
            return True, f"Kafka {self._settings.bootstrap}, Topic {self._settings.topic}"
        except IngestError as exc:
            return False, str(exc)


class DryRunIngest:
    """Entwicklungsmodus ohne Kafka: zaehlt und protokolliert, stellt nicht zu."""

    name = "dryrun"
    topic = "(dry-run, kein Topic)"

    def __init__(self) -> None:
        self.count = 0

    def publish(self, event: dict) -> None:
        self.count += 1
        log.info(
            "DRY-RUN, nicht zugestellt: link_id=%s status=%s speed=%s data_as_of=%s",
            event["link_id"], event["status"], event["speed_mph"], event["data_as_of"],
        )

    def flush(self) -> tuple[int, int]:
        return self.count, 0

    def probe(self) -> tuple[bool, str]:
        return True, "Dry-Run: Events werden NICHT nach Kafka geschrieben"


def build_ingest(mode: str):
    if mode == "kafka":
        return KafkaIngest()
    log.warning("INGEST_MODE=%s — Events werden NICHT nach Kafka geschrieben.", mode)
    return DryRunIngest()


class ScenarioRun:
    """Zustand eines Szenarios. Lebt im Prozess, wird nicht geteilt — bei
    mehreren Repliken kennt ihn nur der Pod, der ihn faehrt."""

    def __init__(
        self,
        *,
        scenario: str,
        link_id: str,
        duration_minutes: int,
        events_per_minute: int,
        reference_speed: float,
    ):
        self.id = uuid.uuid4().hex[:12]
        self.scenario = scenario
        self.link_id = link_id
        self.duration_minutes = duration_minutes
        self.events_per_minute = events_per_minute
        self.reference_speed = reference_speed
        self.planned_events = max(1, duration_minutes * events_per_minute)
        self.published_events = 0
        self.state = "running"
        self.detail: str | None = None
        self.started_at = datetime.now(timezone.utc)
        self.finished_at: datetime | None = None

    @property
    def expected_effect_at(self) -> datetime:
        return self.started_at + timedelta(minutes=1, seconds=30)

    def as_dict(self) -> dict:
        return {
            "scenario_id": self.id,
            "scenario": self.scenario,
            "link_id": self.link_id,
            "state": self.state,
            "detail": self.detail,
            "planned_events": self.planned_events,
            "published_events": self.published_events,
            "duration_minutes": self.duration_minutes,
            "events_per_minute": self.events_per_minute,
            "reference_speed": round(self.reference_speed, 2),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "expected_effect_at": self.expected_effect_at,
        }


class ScenarioRunner:
    """Faehrt Szenarien im Hintergrund, in Echtzeit — rueckdatierte Events
    wuerden vom Spark-Job als verspaetet ausgefiltert."""

    MAX_KEPT = 20

    def __init__(self, ingest, build_event_fn: Callable[..., dict] = build_event):
        self.ingest = ingest
        self._build_event = build_event_fn
        self.runs: dict[str, ScenarioRun] = {}
        self._tasks: set[asyncio.Task] = set()

    def start(self, run: ScenarioRun, segment: dict) -> ScenarioRun:
        self._prune()
        self.runs[run.id] = run
        task = asyncio.create_task(self._drive(run, segment))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return run

    def _prune(self) -> None:
        if len(self.runs) <= self.MAX_KEPT:
            return
        for key in sorted(self.runs, key=lambda k: self.runs[k].started_at)[: -self.MAX_KEPT]:
            if self.runs[key].state != "running":
                del self.runs[key]

    async def _drive(self, run: ScenarioRun, segment: dict) -> None:
        spec = SCENARIOS[run.scenario]
        interval = 60.0 / run.events_per_minute
        try:
            for i in range(run.planned_events):
                progress = i / max(1, run.planned_events - 1)
                factor = spec["speed_factor"](progress)
                status = spec["status"]
                speed = round(run.reference_speed * factor, 2) if status == 0 else 0.0

                event = self._build_event(
                    link_id=run.link_id,
                    data_as_of=datetime.now(timezone.utc),
                    status=status,
                    speed_mph=speed,
                    travel_time_s=0 if status != 0 else None,
                    borough=segment.get("borough"),
                    link_name=segment.get("link_name"),
                    link_points=None,
                    source="SYNTHETIC",
                )
                # Producer ist blockierend, gehoert nicht in die Event-Loop.
                await asyncio.to_thread(self.ingest.publish, event)
                run.published_events += 1
                if i < run.planned_events - 1:
                    await asyncio.sleep(interval)

            await asyncio.to_thread(self.ingest.flush)
            run.state = "done"
            run.detail = f"{run.published_events} Events zugestellt"
        except asyncio.CancelledError:
            run.state = "cancelled"
            run.detail = f"abgebrochen nach {run.published_events} Events"
            raise
        except Exception as exc:
            run.state = "failed"
            run.detail = str(exc)
            log.exception("Szenario %s fehlgeschlagen", run.id)
        finally:
            run.finished_at = datetime.now(timezone.utc)
