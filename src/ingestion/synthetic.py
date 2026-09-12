"""Synthetischer Verkehrsdatenstrom (SCRUM-75).

Zweck laut DATA_SOURCES.md Abschnitt 3: Lastquelle fuer den Skalierungs-
nachweis (SCRUM-93) und Demo-Betrieb ohne Abhaengigkeit von der Verfuegbarkeit
einer fremden API.

Der Generator ahmt drei gemessene Eigenschaften des echten Feeds nach, damit
die Last realistisch ist und nicht nur gross:

1. Echte link_id-Werte aus dem Seed — die Partitionsverteilung entspricht
   damit der des Live-Feeds und nicht einer kuenstlichen Gleichverteilung.
2. Rund 49 % Sentinel-Records mit status=-101 und speed=0/travel_time=0,
   wie im 24h-Fenster gemessen. Ohne die haette der Statusfilter im
   Spark-Job im Testbetrieb nichts zu tun und waere nie verifiziert.
3. Ein Tagesgang: nachts frei, zur Rushhour langsamer. Sonst waere jede
   Baseline flach und die Anomalie-Erkennung in SCRUM-83 liefe ins Leere.
"""

from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timedelta, timezone

import requests

from common import (
    EventPublisher,
    GracefulExit,
    Settings,
    build_event,
    load_seed,
    setup_logging,
)

log = logging.getLogger("ingestion.synthetic")

# Anteil ungueltiger Meldungen, gemessen im 24h-Fenster (DATA_SOURCES.md).
SENTINEL_SHARE = 0.49

# Wie viele Segmente gleichzeitig in einer Stauphase stecken, und wie stark.
# Ohne solche Phasen laege jeder Wert auf der Baseline und die Rangliste im
# Dashboard waere dauerhaft leer.
ACTIVE_EPISODES = 6
EPISODE_MIN_S, EPISODE_MAX_S = 300, 1200
EPISODE_FACTOR = (0.72, 0.93)

# Grober Tagesgang als Faktor auf die Freiflussgeschwindigkeit, Index = Stunde
# lokaler Zeit. Kein Anspruch auf Realismus im Detail — es geht darum, dass
# ueberhaupt eine Tageszeit-Struktur existiert, gegen die eine Baseline
# ueber Wochentag x Stunde (SCRUM-83) etwas messen kann.
HOURLY_FACTOR = [
    1.00, 1.00, 1.00, 1.00, 0.98, 0.92,  # 0-5
    0.80, 0.62, 0.48, 0.55, 0.70, 0.75,  # 6-11
    0.72, 0.70, 0.68, 0.60, 0.50, 0.45,  # 12-17
    0.52, 0.68, 0.82, 0.90, 0.95, 0.98,  # 18-23
]


def _free_flow_for(link_id: str) -> float:
    """Deterministische Freiflussgeschwindigkeit je Segment.

    Aus der link_id abgeleitet, nicht zufaellig gezogen: derselbe Sensor hat
    ueber Neustarts hinweg dasselbe Grundniveau. Sonst waere die Baseline
    nach jedem Pod-Restart eine andere und der Vergleich sinnlos.
    """
    rnd = random.Random(int(link_id) if link_id.isdigit() else hash(link_id))
    return rnd.uniform(25.0, 60.0)


def fetch_baseline(url: str) -> dict[str, tuple[float, float]]:
    """Erwartungswert und Streuung je Segment fuer die aktuelle Stunde."""
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    payload = resp.json()
    return {
        link_id: (cell["baseline_speed"], cell["baseline_stddev"])
        for link_id, cell in payload.get("items", {}).items()
    }


def refresh_episodes(seed: list[dict], episodes: dict, now: datetime) -> None:
    for link_id, (until, _) in list(episodes.items()):
        if now >= until:
            del episodes[link_id]
    while len(episodes) < min(ACTIVE_EPISODES, len(seed)):
        link_id = random.choice(seed)["link_id"]
        if link_id in episodes:
            continue
        episodes[link_id] = (
            now + timedelta(seconds=random.randint(EPISODE_MIN_S, EPISODE_MAX_S)),
            random.uniform(*EPISODE_FACTOR),
        )


def _measurement(
    link_id: str,
    now: datetime,
    cell: tuple[float, float] | None,
    factor: float,
) -> tuple[int, float | None, int | None]:
    """Liefert (status, speed_mph, travel_time_s) fuer einen Zeitpunkt."""
    if random.random() < SENTINEL_SHARE:
        # Sentinel-Triplett exakt so, wie im Feed beobachtet.
        return -101, 0.0, 0

    if cell is not None:
        # Um die Baseline streuen, mit deren eigener Streuung: ohne Stauphase
        # liegt der z-Score dann um null statt systematisch daneben.
        expected, stddev = cell
        speed = max(1.0, random.gauss(expected * factor, stddev))
    else:
        base = _free_flow_for(link_id)
        hourly = HOURLY_FACTOR[now.hour]
        # Wochenende laeuft fluessiger.
        if now.weekday() >= 5:
            hourly = min(1.0, hourly * 1.25)
        speed = max(1.0, random.gauss(base * hourly * factor, base * 0.08))
    # Segmentlaenge unbekannt; travel_time konsistent aus der Geschwindigkeit
    # ableiten, damit beide Felder nicht widerspruechlich sind.
    length_miles = 0.3 + (int(link_id) % 20) / 10 if link_id.isdigit() else 1.0
    travel_time = int(length_miles / speed * 3600)
    return 0, round(speed, 2), travel_time


def run(settings: Settings) -> None:
    seed = load_seed()
    publisher = EventPublisher(settings)
    exit_handler = GracefulExit()

    interval = 1.0 / settings.events_per_second if settings.events_per_second > 0 else 0.05
    log.info(
        "synthetischer Modus: %.1f Events/s ueber %d Segmente",
        settings.events_per_second, len(seed),
    )

    # Zeitversatz je Segment, damit nicht alle 125 Sensoren im selben
    # Millisekundenfenster melden.
    offsets = {s["link_id"]: random.uniform(0, 460) for s in seed}
    last_report = time.monotonic()

    baseline: dict[str, tuple[float, float]] = {}
    baseline_due = 0.0
    episodes: dict[str, tuple[datetime, float]] = {}

    while not exit_handler.stop:
        now = datetime.now(timezone.utc)

        if settings.baseline_url and time.monotonic() >= baseline_due:
            try:
                baseline = fetch_baseline(settings.baseline_url)
                log.info("Baseline geladen: %d Segmente", len(baseline))
                baseline_due = time.monotonic() + settings.baseline_refresh_s
            except Exception as exc:
                log.warning("Baseline nicht abrufbar (%s) — falle auf das eigene Profil zurueck", exc)
                baseline_due = time.monotonic() + 60

        refresh_episodes(seed, episodes, now)

        segment = random.choice(seed)
        link_id = segment["link_id"]
        # data_as_of liegt leicht in der Vergangenheit — das erzeugt genau die
        # Verzoegerung zwischen Event Time und Processing Time, an der sich
        # Watermarks und Late-Data-Handling (SCRUM-85) ueberhaupt zeigen lassen.
        data_as_of = now - timedelta(seconds=offsets[link_id])

        episode = episodes.get(link_id)
        status, speed, travel_time = _measurement(
            link_id, data_as_of, baseline.get(link_id), episode[1] if episode else 1.0
        )
        publisher.publish(
            build_event(
                link_id=link_id,
                data_as_of=data_as_of,
                status=status,
                speed_mph=speed,
                travel_time_s=travel_time,
                borough=segment.get("borough"),
                link_name=segment.get("link_name"),
                link_points=None,
                source="SYNTHETIC",
            )
        )

        if time.monotonic() - last_report > 30:
            sent, failed = publisher.stats
            log.info("Zwischenstand: zugestellt=%d fehlgeschlagen=%d", sent, failed)
            last_report = time.monotonic()

        time.sleep(interval)

    publisher.flush()


if __name__ == "__main__":
    setup_logging()
    run(Settings.from_env())
