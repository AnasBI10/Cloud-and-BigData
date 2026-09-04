"""Live-Poller gegen den NYC DOT Feed (i4gi-tjb9).

Deckt die Luecke, die im Backlog kein Ticket hatte: SCRUM-75 baut nur den
synthetischen Producer, den echten Socrata-Strom holt sonst nichts — obwohl
die README durchgehend mit echten Daten argumentiert.

Zwei Eigenschaften, die aus den Messungen in DATA_SOURCES.md folgen:

Sharding ueber borough. Mehrere Repliken, die denselben Endpunkt abfragen,
wuerden dieselben Records mehrfach einspeisen. Jede Replik zieht deshalb
serverseitig gefiltert nur ihre Boroughs. Damit ist auch diese Komponente
horizontal skalierbar (1 bis 5), wie es die Aufgabenstellung fuer alle
Komponenten verlangt.

Wasserstand statt blindem Poll. Der SODA2-Endpunkt liefert laut Header
X-SODA2-Data-Out-Of-Date mitunter Daten aus einem Cache, dessen Stand
Stunden hinter der Abrufzeit liegt. Ein fester Poll-Takt wuerde deshalb
wiederholt denselben Cache-Stand ziehen. Der Poller merkt sich das hoechste
gesehene data_as_of je Lauf und fragt nur Neueres ab.
"""

from __future__ import annotations

import logging
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
    shard_of,
)

log = logging.getLogger("ingestion.live")

ENDPOINT = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json"
PAGE_LIMIT = 50_000


def _to_float(raw: str | None) -> float | None:
    try:
        return float(raw) if raw not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _to_int(raw: str | None) -> int | None:
    value = _to_float(raw)
    return int(value) if value is not None else None


def _parse_ts(raw: str) -> datetime:
    """data_as_of kommt als floating_timestamp ohne Zonenangabe. Der Feed ist
    New Yorker Ortszeit; wir interpretieren ihn hier als UTC-naiv und haengen
    UTC an. Das ist eine bewusste Vereinfachung fuer den Prototyp — sie
    verschiebt alle Zeitstempel konsistent, verzerrt also den Baseline-
    Vergleich nicht, wohl aber die absolute Tageszeit. Gehoert in Abschnitt 12.
    """
    return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)


def _fetch(settings: Settings, boroughs: list[str], since: datetime) -> list[dict]:
    where = [f"data_as_of > '{since.strftime('%Y-%m-%dT%H:%M:%S')}'"]
    if boroughs:
        quoted = ",".join(f"'{b}'" for b in boroughs)
        where.append(f"borough in({quoted})")

    params = {
        "$where": " AND ".join(where),
        "$order": "data_as_of ASC",
        "$limit": PAGE_LIMIT,
    }
    headers = {"X-App-Token": settings.app_token} if settings.app_token else {}

    response = requests.get(ENDPOINT, params=params, headers=headers, timeout=60)
    response.raise_for_status()

    # Cache-Staleness sichtbar machen statt sie zu ignorieren — der Wert
    # gehoert in die Velocity-Kennzahlen in Kapitel 2.
    if response.headers.get("X-SODA2-Data-Out-Of-Date") == "true":
        log.warning(
            "Feed lieferte Cache-Stand: Truth-Last-Modified=%s",
            response.headers.get("Truth-Last-Modified"),
        )
    return response.json()


def run(settings: Settings) -> None:
    seed = load_seed()
    subset = shard_of(seed, settings)
    boroughs = sorted({s["borough"] for s in subset if s.get("borough")})
    geometry = {s["link_id"]: s for s in subset}

    if not settings.app_token:
        # Ohne Token gilt ein deutlich strengeres Rate-Limit. Kein Abbruch,
        # aber der Betrieb soll wissen, dass das Secret fehlt (SCRUM-81).
        log.warning("Kein SOCRATA_APP_TOKEN gesetzt — anonymes Rate-Limit greift")

    publisher = EventPublisher(settings)
    exit_handler = GracefulExit()

    # Erster Lauf: INITIAL_LOOKBACK_H zurueck, nicht nur ein paar Intervalle.
    # Der Feed kann stundenlang stillstehen (siehe Settings.initial_lookback_h);
    # ein enges Startfenster wuerde dann dauerhaft leer laufen, ohne dass am
    # Log erkennbar waere, ob der Poller kaputt ist oder die Quelle steht.
    watermark = datetime.now(timezone.utc) - timedelta(hours=settings.initial_lookback_h)
    log.info(
        "Live-Modus: Intervall %ds, Lookback %dh, Boroughs %s, Startwasserstand %s",
        settings.poll_interval_s,
        settings.initial_lookback_h,
        boroughs or "alle",
        watermark.isoformat(),
    )

    while not exit_handler.stop:
        started = time.monotonic()
        try:
            rows = _fetch(settings, boroughs, watermark)
        except requests.RequestException:
            # Netzfehler darf den Pod nicht toeten; naechster Zyklus versucht
            # es erneut, der Wasserstand bleibt stehen.
            log.exception("Abruf fehlgeschlagen, warte auf naechsten Zyklus")
            rows = []

        highest = watermark
        for row in rows:
            link_id = row.get("link_id")
            raw_ts = row.get("data_as_of")
            if not link_id or not raw_ts:
                continue

            ts = _parse_ts(raw_ts)
            highest = max(highest, ts)
            meta = geometry.get(link_id, {})

            publisher.publish(
                build_event(
                    link_id=link_id,
                    data_as_of=ts,
                    status=_to_int(row.get("status")) or 0,
                    speed_mph=_to_float(row.get("speed")),
                    travel_time_s=_to_int(row.get("travel_time")),
                    borough=row.get("borough") or meta.get("borough"),
                    link_name=row.get("link_name") or meta.get("link_name"),
                    link_points=row.get("link_points"),
                    source="DOT_LIVE",
                )
            )

        publisher.flush()
        if rows:
            # Abstand zwischen juengstem Messzeitpunkt und Abrufzeit. Das ist
            # die real gemessene Feed-Verzoegerung und geht als Velocity-
            # Kennzahl in Kapitel 2 ein — belegt statt geschaetzt.
            lag_min = (datetime.now(timezone.utc) - highest).total_seconds() / 60
            log.info(
                "%d Records verarbeitet, juengster Messzeitpunkt %s (Feed-Verzoegerung %.1f min)",
                len(rows), highest.isoformat(), lag_min,
            )
            watermark = highest
        else:
            log.info(
                "keine neuen Records seit %s — Feed steht oder liefert Cache-Stand",
                watermark.isoformat(),
            )

        # Verstrichene Zeit abziehen, damit das Intervall nicht driftet.
        sleep_for = max(5, settings.poll_interval_s - (time.monotonic() - started))
        for _ in range(int(sleep_for)):
            if exit_handler.stop:
                break
            time.sleep(1)

    publisher.flush()


if __name__ == "__main__":
    setup_logging()
    run(Settings.from_env())