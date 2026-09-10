"""Einspeisung von UI-Events in die Ingestion (SCRUM-89).

Die Aufgabenstellung verlangt eine UI mindestens in der Rolle des
Datenlieferanten. Entscheidend ist dabei der Weg: ein hier erzeugtes Event
geht durch **denselben** Pfad wie eine echte DOT-Messung — Avro gegen die
Schema-Registry serialisiert, nach ``traffic.speeds.raw``, von dort in den
Spark-Job und erst ueber Bronze/Silver/Gold zurueck ins Dashboard. Ein
Seiteneingang, der direkt in die Gold-Schicht schreibt, waere schneller
sichtbar und wuerde genau das nicht zeigen, worum es geht.

Deshalb wird ``src/ingestion/common.py`` importiert und nicht nachgebaut:
Serialisierung, Registry-Anbindung und der Bau des ``event_key`` liegen an
genau einer Stelle. Waere das hier dupliziert, gaebe es zwei Definitionen
davon, wie ein Event aussieht.

Zwei Betriebsmodi, wie beim Gold-Reader eine Env-Variable statt einer
Code-Aenderung:

* ``INGEST_MODE=kafka``  — Abgabestand, echter Producer.
* ``INGEST_MODE=dryrun`` — Entwicklung ohne Kafka. Serialisiert NICHT und
  stellt NICHT zu, zaehlt nur mit. Jede Antwort traegt den Modus mit, damit
  ein versehentlich stehengebliebener Dry-Run nicht wie echte Einspeisung
  aussieht.
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

# common.py liegt im Repo unter src/ingestion, im Image daneben unter /app
# (siehe src/serving/Dockerfile). Der Suchpfad deckt beide Faelle ab, ohne
# dass die Datei kopiert oder ihr Inhalt wiederholt werden muesste.
_CANDIDATES = [
    pathlib.Path(__file__).resolve().parent,
    pathlib.Path(__file__).resolve().parents[1] / "ingestion",
]
for _d in _CANDIDATES:
    if (_d / "common.py").exists() and str(_d) not in sys.path:
        sys.path.insert(0, str(_d))
        break


def load_build_event() -> Callable[..., dict]:
    """``build_event`` aus der Ingestion. Bewusst spaet importiert: common.py
    zieht confluent_kafka nach, und die Lese-Endpunkte sollen ohne diese
    Abhaengigkeit starten koennen."""
    from common import build_event

    return build_event


class IngestError(RuntimeError):
    """Einspeisung nicht moeglich. Fuehrt zu 503: die API ist in Ordnung,
    Kafka oder die Schema-Registry sind es nicht."""


# ---------------------------------------------------------------------------
# Szenarien
# ---------------------------------------------------------------------------

# Warum ueberhaupt Szenarien und nicht nur ein Formular: die Aggregation laeuft
# ueber 5-Minuten-Fenster. Ein einzelnes Event verschiebt einen Mittelwert aus
# mehreren Messungen kaum sichtbar — im Dashboard passiert dann scheinbar
# nichts, obwohl die Kette funktioniert. Eine Folge von Events ueber mehrere
# Minuten erzeugt den Effekt, den man sehen kann.
SCENARIOS: dict[str, dict[str, Any]] = {
    "congestion": {
        "beschreibung": (
            "Geschwindigkeit faellt linear auf 30 % des Ausgangswerts. "
            "congestion_score steigt, das Segment wandert in die Rangliste."
        ),
        "status": 0,
        "speed_factor": lambda p: 1.0 - 0.7 * p,
    },
    "recovery": {
        "beschreibung": (
            "Gegenstueck zu congestion: von 30 % zurueck auf den Ausgangswert. "
            "Der Score faellt, das Segment verlaesst die Rangliste."
        ),
        "status": 0,
        "speed_factor": lambda p: 0.3 + 0.7 * p,
    },
    "sensor_outage": {
        "beschreibung": (
            "Serie mit status=-101 und speed=0 — das Sentinel-Triplett des "
            "echten Feeds. Der Statusfilter des Spark-Jobs verwirft sie in "
            "Silver, das Segment bekommt kein neues Fenster und faellt im "
            "Dashboard auf 'keine aktuelle Messung' zurueck. Sichtbarer "
            "Beleg dafuer, dass gefiltert wird und nicht 0 mph als Stau "
            "durchschlaegt."
        ),
        "status": -101,
        "speed_factor": lambda p: 0.0,
    },
}

# Ausweichwert, wenn fuer das Segment keine Baseline vorliegt (Fixture-Modus
# oder Zelle ohne Historie). Grob die mittlere Freiflussgeschwindigkeit des
# Seeds — es geht um eine sichtbare Veraenderung, nicht um Realitaetstreue.
DEFAULT_REFERENCE_SPEED_MPH = 30.0


def scenario_catalog() -> list[dict[str, str]]:
    return [
        {"name": name, "beschreibung": spec["beschreibung"]}
        for name, spec in SCENARIOS.items()
    ]


# ---------------------------------------------------------------------------
# Einspeisung
# ---------------------------------------------------------------------------


class KafkaIngest:
    """Echte Einspeisung ueber den Producer aus src/ingestion/common.py."""

    name = "kafka"

    def __init__(self) -> None:
        # Erst beim ersten Schreibzugriff verbinden: die Lese-Endpunkte
        # sollen auch dann antworten, wenn Kafka gerade nicht erreichbar ist.
        # Ein Import auf Modulebene wuerde die ganze API an librdkafka binden.
        self._publisher = None
        self._settings = None
        # Die POST-Endpunkte laufen im Threadpool von FastAPI. Ohne Sperre
        # koennten zwei gleichzeitige Anfragen zwei Producer aufbauen, von
        # denen einer nie geflusht wird.
        self._lock = threading.Lock()

    def _ensure(self):
        with self._lock:
            if self._publisher is not None:
                return self._publisher
            try:
                from common import EventPublisher, Settings as IngestSettings

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
    """Entwicklungsmodus ohne Kafka: zaehlt und protokolliert, stellt nicht zu.

    NICHT der Abgabestand — die Aufgabenstellung verlangt Events, die
    tatsaechlich durch die Pipeline laufen.
    """

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
    log.warning(
        "INGEST_MODE=%s — Events werden NICHT nach Kafka geschrieben. "
        "Fuer echten Betrieb INGEST_MODE=kafka setzen.",
        mode,
    )
    return DryRunIngest()


# ---------------------------------------------------------------------------
# Szenario-Laeufe
# ---------------------------------------------------------------------------


class ScenarioRun:
    """Zustand eines laufenden oder abgeschlossenen Szenarios.

    Der Zustand liegt im Prozess und wird bewusst nicht geteilt: er ist die
    Quittung eines Knopfdrucks, kein Betriebszustand. Bei mehreren Repliken
    kennt ihn nur der Pod, der das Szenario faehrt — deshalb traegt schon die
    Antwort auf POST den vollstaendigen Plan, damit die UI den Fortschritt
    ohne Rueckfrage anzeigen kann.
    """

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
        # Das erste Fenster, in dem der Effekt sichtbar wird: der Sink schiebt
        # sein Fenster im Minutentakt weiter und triggert alle 30 Sekunden.
        # Vor Ablauf einer Minute plus Trigger ist im Dashboard nichts zu
        # sehen, egal wie viele Events geschickt wurden.
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
    """Faehrt Szenarien im Hintergrund und haelt die letzten Laeufe vor.

    Echtzeit statt Rueckdatierung: der Spark-Job verwirft jedes Event, dessen
    ``data_as_of`` aelter ist als die Watermark (2 Minuten), und schickt es in
    die DLQ. Ein Szenario, das seine Events mit Zeitstempeln der letzten
    Viertelstunde auf einen Schlag schickt, kaeme im Dashboard nie an. Also
    laeuft es so lange, wie es dauert.
    """

    MAX_KEPT = 20

    def __init__(self, ingest, build_event_fn: Callable[..., dict]):
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
                    # Jetzt, nicht rueckdatiert — sonst greift die Watermark.
                    data_as_of=datetime.now(timezone.utc),
                    status=status,
                    speed_mph=speed,
                    # travel_time_s bleibt leer: die Segmentlaenge ist nicht
                    # bekannt, ein hergeleiteter Wert waere erfundene Genauig-
                    # keit. Das Feld ist nullable und wird in Gold nicht
                    # verwendet.
                    travel_time_s=0 if status != 0 else None,
                    borough=segment.get("borough"),
                    link_name=segment.get("link_name"),
                    link_points=None,
                    source="SYNTHETIC",
                )
                # Der Producer ist blockierend; er gehoert nicht in die
                # Event-Loop, sonst stehen waehrenddessen alle Leseanfragen.
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
