"""Synthetischer Verkehrsdatenstrom (SCRUM-75): Lastquelle fuer den
Skalierungsnachweis und Demo-Betrieb ohne fremde API."""

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

# Im 24h-Fenster gemessener Anteil ungueltiger Meldungen (DATA_SOURCES.md).
SENTINEL_SHARE = 0.49

ACTIVE_EPISODES = 6
EPISODE_MIN_S, EPISODE_MAX_S = 300, 1200
EPISODE_FACTOR = (0.72, 0.93)

# Muss unter der Watermark (2 min) bleiben, Kafka-Laufzeit und Trigger kommen
# dazu. LATE_SHARE liegt bewusst darueber und speist die DLQ.
OFFSET_MAX_S = 30
LATE_SHARE = 0.12
LATE_OFFSET_S = (180, 420)

HOURLY_FACTOR = [
    1.00,
    1.00,
    1.00,
    1.00,
    0.98,
    0.92,  # 0-5
    0.80,
    0.62,
    0.48,
    0.55,
    0.70,
    0.75,  # 6-11
    0.72,
    0.70,
    0.68,
    0.60,
    0.50,
    0.45,  # 12-17
    0.52,
    0.68,
    0.82,
    0.90,
    0.95,
    0.98,  # 18-23
]


def _free_flow_for(link_id: str) -> float:
    # Aus der link_id abgeleitet, damit ein Sensor ueber Neustarts hinweg
    # dasselbe Grundniveau behaelt.
    rnd = random.Random(int(link_id) if link_id.isdigit() else hash(link_id))
    return rnd.uniform(25.0, 60.0)


def fetch_baseline(url: str) -> dict[str, tuple[float, float]]:
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    payload = resp.json()
    return {
        link_id: (cell["baseline_speed"], cell["baseline_stddev"])
        for link_id, cell in payload.get("items", {}).items()
    }


def build_offsets(seed: list[dict]) -> dict[str, float]:
    return {
        s["link_id"]: (
            random.uniform(*LATE_OFFSET_S)
            if random.random() < LATE_SHARE
            else random.uniform(0, OFFSET_MAX_S)
        )
        for s in seed
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
    if random.random() < SENTINEL_SHARE:
        return -101, 0.0, 0

    if cell is not None:
        # Um die Baseline streuen: ohne Stauphase liegt der z-Score dann um
        # null statt systematisch daneben.
        expected, stddev = cell
        speed = max(1.0, random.gauss(expected * factor, stddev))
    else:
        hourly = HOURLY_FACTOR[now.hour]
        if now.weekday() >= 5:
            hourly = min(1.0, hourly * 1.25)
        base = _free_flow_for(link_id)
        speed = max(1.0, random.gauss(base * hourly * factor, base * 0.08))

    length_miles = 0.3 + (int(link_id) % 20) / 10 if link_id.isdigit() else 1.0
    travel_time = int(length_miles / speed * 3600)
    return 0, round(speed, 2), travel_time


def run(settings: Settings) -> None:
    seed = load_seed()
    publisher = EventPublisher(settings)
    exit_handler = GracefulExit()

    interval = 1.0 / settings.events_per_second if settings.events_per_second > 0 else 0.05
    offsets = build_offsets(seed)
    late = sum(1 for v in offsets.values() if v > OFFSET_MAX_S)
    log.info(
        "synthetischer Modus: %.1f Events/s ueber %d Segmente, davon %d bewusst verspaetet",
        settings.events_per_second,
        len(seed),
        late,
    )

    last_report = time.monotonic()
    baseline: dict[str, tuple[float, float]] = {}
    baseline_due = 0.0
    baseline_hour: int | None = None
    episodes: dict[str, tuple[datetime, float]] = {}

    while not exit_handler.stop:
        now = datetime.now(timezone.utc)

        # Die Baseline-Zelle gilt je Wochentag und Stunde, deshalb auch beim
        # Stundenwechsel neu holen.
        stale = baseline_hour is not None and now.hour != baseline_hour
        if settings.baseline_url and (stale or time.monotonic() >= baseline_due):
            try:
                baseline = fetch_baseline(settings.baseline_url)
                baseline_hour = now.hour
                log.info(
                    "Baseline geladen: %d Segmente (Stunde %d UTC)", len(baseline), baseline_hour
                )
                baseline_due = time.monotonic() + settings.baseline_refresh_s
            except Exception as exc:
                log.warning("Baseline nicht abrufbar (%s) — nutze das eigene Profil", exc)
                baseline_due = time.monotonic() + 60

        refresh_episodes(seed, episodes, now)

        segment = random.choice(seed)
        link_id = segment["link_id"]
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
