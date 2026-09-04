"""Gemeinsamer Unterbau fuer beide Producer-Modi (SCRUM-75).

Serialisierung, Registry-Anbindung, Kafka-Konfiguration und der Bau des
event_key liegen hier an genau einer Stelle. Das ist dieselbe Begruendung,
mit der wir uns in Bericht 3.1 gegen Lambda entschieden haben: eine Regel,
ein Ort. Synthetischer Lastgenerator und Live-Poller unterscheiden sich
ausschliesslich in der Datenquelle, nicht darin, wie ein Event aussieht.
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
import signal
from dataclasses import dataclass
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

log = logging.getLogger("ingestion")

SCHEMA_PATH = pathlib.Path(
    os.getenv("SCHEMA_PATH", "/app/schemas/traffic_speed_event.avsc")
)
SEED_PATH = pathlib.Path(os.getenv("SEED_PATH", "/app/data/dot_links_seed.json"))


@dataclass(frozen=True)
class Settings:
    """Alles konfigurierbar, nichts hart verdrahtet — der Betrieb kommt aus
    ConfigMap und Secret (SCRUM-81), nicht aus dem Image."""

    mode: str
    bootstrap: str
    schema_registry: str
    topic: str
    dlq_topic: str
    # synthetic
    events_per_second: float
    # live
    poll_interval_s: int
    initial_lookback_h: int
    app_token: str | None
    shard_index: int
    shard_count: int

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            mode=os.getenv("MODE", "synthetic").lower(),
            bootstrap=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
            schema_registry=os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
            topic=os.getenv("TOPIC", "traffic.speeds.raw"),
            dlq_topic=os.getenv("DLQ_TOPIC", "traffic.speeds.dlq"),
            events_per_second=float(os.getenv("EVENTS_PER_SECOND", "20")),
            poll_interval_s=int(os.getenv("POLL_INTERVAL_S", "300")),
            # Wie weit der erste Lauf zurueckgreift. Gemessen am 04.09.2026:
            # der DOT-Feed stand ohne Ankuendigung 13 Stunden still (letzter
            # Record 03.09. 23:07 UTC bei Abruf um 11:58 UTC). Ein Startfenster
            # von zwei Poll-Intervallen findet in so einem Fall garantiert
            # nichts. 24 h holt eine solche Luecke beim Start nach; danach
            # laeuft der Poller ohnehin auf dem Wasserstand weiter.
            initial_lookback_h=int(os.getenv("INITIAL_LOOKBACK_H", "24")),
            app_token=os.getenv("SOCRATA_APP_TOKEN") or None,
            # Bei einem StatefulSet liefert der Hostname das Ordinal; bei einem
            # Deployment wird SHARD_INDEX gesetzt. Default 0/1 = kein Sharding.
            shard_index=int(os.getenv("SHARD_INDEX", _ordinal_from_hostname())),
            shard_count=int(os.getenv("SHARD_COUNT", "1")),
        )


def _ordinal_from_hostname() -> str:
    """'live-poller-2' -> '2'. Faellt auf 0 zurueck, wenn der Hostname nicht
    auf eine Zahl endet (z. B. Deployment mit Hash-Suffix)."""
    tail = os.getenv("HOSTNAME", "").rsplit("-", 1)[-1]
    return tail if tail.isdigit() else "0"


def load_seed() -> list[dict]:
    """125 echte link_id-Werte mit Borough, einmalig aus dem DOT-Feed gezogen
    und im Repo abgelegt. Bewusst als Seed statt als Live-Abfrage beim Start:
    der synthetische Modus muss auch ohne Netz und ohne gueltigen Token
    starten, sonst haengt der Skalierungsnachweis (SCRUM-93) an der
    Verfuegbarkeit einer fremden API."""
    with SEED_PATH.open(encoding="utf-8") as fh:
        seed = json.load(fh)
    log.info("Seed geladen: %d Segmente", len(seed))
    return seed


def shard_of(seed: list[dict], settings: Settings) -> list[dict]:
    """Teilt die Segmente deterministisch auf die Repliken auf.

    Der Live-Poller darf nicht naiv skalieren: fuenf Repliken, die denselben
    Endpunkt abfragen, erzeugen fuenffache Duplikate. Ueber das Sharding hat
    jede Replik einen disjunkten Ausschnitt — damit ist auch diese Komponente
    horizontal skalierbar, wie es die Aufgabenstellung fuer *alle* Komponenten
    verlangt.

    Geshardet wird ueber borough, weil sich daraus ein serverseitiger
    $where-Filter bauen laesst und der Poller so nicht die ganze Stadt zieht,
    um 4/5 davon wegzuwerfen.
    """
    if settings.shard_count <= 1:
        return seed
    if not 0 <= settings.shard_index < settings.shard_count:
        # Waere die Konfiguration inkonsistent (etwa mehr Repliken als
        # SHARD_COUNT), zoege dieser Pod stillschweigend gar nichts oder
        # dasselbe wie ein anderer. Lieber laut abbrechen als leise
        # Duplikate oder Luecken erzeugen.
        raise SystemExit(
            f"SHARD_INDEX={settings.shard_index} liegt ausserhalb von "
            f"SHARD_COUNT={settings.shard_count} — replicas und SHARD_COUNT "
            f"muessen uebereinstimmen"
        )
    boroughs = sorted({s["borough"] for s in seed if s.get("borough")})
    mine = {
        b for i, b in enumerate(boroughs) if i % settings.shard_count == settings.shard_index
    }
    subset = [s for s in seed if s.get("borough") in mine]
    log.info(
        "Shard %d/%d: Boroughs %s, %d Segmente",
        settings.shard_index, settings.shard_count, sorted(mine), len(subset),
    )
    return subset


def build_event(
    *,
    link_id: str,
    data_as_of: datetime,
    status: int,
    speed_mph: float | None,
    travel_time_s: int | None,
    borough: str | None,
    link_name: str | None,
    link_points: str | None,
    source: str,
) -> dict:
    """Baut ein Event passend zum Avro-Schema aus SCRUM-74.

    Wichtig: es wird NICHT nach status gefiltert. Auch die ~49 % Sentinel-
    Records mit status=-101 gehen roh nach Kafka. Der Filter gehoert in den
    Spark-Job (SCRUM-77/82) — laege er hier, gaebe es ihn zweimal, einmal
    fuer den Live-Pfad und einmal fuer Reprocessing aus dem Lake.
    """
    ts = data_as_of.astimezone(timezone.utc)
    return {
        "link_id": link_id,
        "data_as_of": ts,
        # Zusammengesetzter Schluessel: der DOT-Feed hat keine eindeutige
        # Record-ID. Merge-Key fuer den Delta-Sink (SCRUM-86/95).
        "event_key": f"{link_id}|{ts.isoformat().replace('+00:00', 'Z')}",
        "status": status,
        "speed_mph": speed_mph,
        "travel_time_s": travel_time_s,
        "borough": borough,
        "link_name": link_name,
        "link_points": link_points,
        "ingested_at": datetime.now(timezone.utc),
        "source": source,
    }


class EventPublisher:
    """Duenne Huelle um Producer + AvroSerializer."""

    def __init__(self, settings: Settings):
        self.settings = settings
        schema_str = SCHEMA_PATH.read_text(encoding="utf-8")
        registry = SchemaRegistryClient({"url": settings.schema_registry})
        # auto.register.schemas=False: das Schema wurde vom Job in SCRUM-74
        # deklarativ registriert. Wuerde der Producer es selbst registrieren
        # duerfen, koennte ein fehlerhaft deployter Producer eine neue Version
        # anlegen und der Vertrag waere nicht mehr die Quelle der Wahrheit.
        self._serializer = AvroSerializer(
            registry, schema_str, conf={"auto.register.schemas": False}
        )
        self._key_serializer = StringSerializer("utf_8")
        self._producer = Producer(
            {
                "bootstrap.servers": settings.bootstrap,
                # acks=all + idempotence: keine stillen Verluste bei
                # Broker-Ausfall, keine Duplikate durch interne Retries.
                # Zusammen mit RF=3/minISR=2 die Producer-Haelfte der
                # Exactly-once-Kette aus SCRUM-95.
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "snappy",
                "linger.ms": 50,
                "client.id": os.getenv("HOSTNAME", "producer"),
            }
        )
        self._sent = 0
        self._failed = 0

    def _on_delivery(self, err, msg):
        if err is not None:
            self._failed += 1
            log.error("Zustellung fehlgeschlagen: %s", err)
        else:
            self._sent += 1

    def publish(self, event: dict) -> None:
        topic = self.settings.topic
        ctx = SerializationContext(topic, MessageField.VALUE)
        try:
            payload = self._serializer(event, ctx)
        except Exception:
            # Schema-Verletzung: nicht den Stream anhalten, nicht still
            # verschlucken. Roh in die DLQ, damit ein Schema-Bruch im
            # Nachhinein nachvollziehbar ist.
            log.exception("Serialisierung fehlgeschlagen, gehe in die DLQ")
            self._producer.produce(
                self.settings.dlq_topic,
                key=self._key_serializer(event.get("link_id", "unknown")),
                value=json.dumps(event, default=str).encode("utf-8"),
            )
            return

        self._producer.produce(
            topic,
            key=self._key_serializer(event["link_id"]),
            value=payload,
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush(30)
        log.info("zugestellt=%d fehlgeschlagen=%d", self._sent, self._failed)

    @property
    def stats(self) -> tuple[int, int]:
        return self._sent, self._failed


class GracefulExit:
    """SIGTERM sauber behandeln, damit beim Rollout gepufferte Events noch
    rausgehen und nicht im Producer-Buffer sterben."""

    def __init__(self):
        self.stop = False
        signal.signal(signal.SIGTERM, self._handle)
        signal.signal(signal.SIGINT, self._handle)

    def _handle(self, *_):
        log.info("Signal empfangen, fahre herunter")
        self.stop = True


def setup_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )