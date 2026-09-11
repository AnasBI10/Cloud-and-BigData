"""Zugriff auf die Gold-Schicht (SCRUM-79)
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Protocol

import pyarrow.dataset as pads
from deltalake import DeltaTable

from config import Settings
from models import Segment, SegmentWindow

log = logging.getLogger("serving.readers")

WINDOW_MINUTES = 5


class ReaderError(RuntimeError):
    """Gold-Schicht nicht lesbar. Fuehrt zu 503, nicht zu 500."""


class GoldReader(Protocol):
    name: str

    def latest_windows(self) -> list[SegmentWindow]: ...

    def timeseries(self, link_id: str, hours: int) -> list[SegmentWindow]: ...

    def reference_speed(self, link_id: str, ts: datetime) -> float | None: ...

    def baseline_links(self) -> set[str]: ...

    def probe(self) -> tuple[bool, str]: ...


class TTLCache:
    def __init__(self, ttl_s: int):
        self._ttl = ttl_s
        self._lock = threading.Lock()
        self._value: Any = None
        self._expires = 0.0

    def get(self, produce: Callable[[], Any]) -> Any:
        with self._lock:
            now = time.monotonic()
            if self._value is None or now >= self._expires:
                self._value = produce()
                self._expires = now + self._ttl
            return self._value

    def invalidate(self) -> None:
        with self._lock:
            self._value = None
            self._expires = 0.0


def load_seed(settings: Settings) -> list[dict]:
    with settings.seed_path.open(encoding="utf-8") as fh:
        seed = json.load(fh)
    log.info("Seed geladen: %d Segmente", len(seed))
    return seed


def _stable_fraction(*parts: str) -> float:
    """Deterministischer Wert in [0,1) — dieselbe link_id liefert ueber
    Neustarts und Repliken hinweg dasselbe Profil."""
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") / float(1 << 64)


def floor_window(ts: datetime) -> datetime:
    minute = (ts.minute // WINDOW_MINUTES) * WINDOW_MINUTES
    return ts.replace(minute=minute, second=0, microsecond=0)


class FixtureReader:
    name = "fixture"

    def __init__(self, settings: Settings):
        self.settings = settings
        self.seed = load_seed(settings)
        self._no_baseline = {
            s["link_id"]
            for s in self.seed
            if _stable_fraction("baseline", s["link_id"]) < 31 / 125
        }

    def _baseline(self, link_id: str, ts: datetime) -> tuple[float, float]:
        base = 22.0 + 26.0 * _stable_fraction("speed", link_id)
        hour = ts.hour
        rush = math.exp(-(((hour - 8) / 2.2) ** 2)) + math.exp(
            -(((hour - 18) / 2.4) ** 2)
        )
        weekday_factor = 1.0 if ts.weekday() < 5 else 0.45
        expected = base * (1.0 - 0.42 * rush * weekday_factor)
        stddev = max(1.8, expected * 0.16)
        return round(expected, 2), round(stddev, 2)

    def _weather(self, ts: datetime, borough: str | None) -> tuple[str, float, float]:
        f = _stable_fraction("weather", borough or "NYC", ts.strftime("%Y-%m-%dT%H"))
        if f < 0.12:
            return "rain", 14.0 + 6 * f, round(2.5 + 6 * f, 1)
        if f < 0.22:
            return "drizzle", 16.0 + 5 * f, round(0.3 + f, 1)
        if f < 0.30:
            return "cloudy", 18.0 + 6 * f, 0.0
        return "clear", 19.0 + 9 * f, 0.0

    def _window_for(self, segment: dict, window_start: datetime) -> SegmentWindow | None:
        link_id = segment["link_id"]
        slot = window_start.strftime("%Y-%m-%dT%H:%M")

        if _stable_fraction("present", link_id, slot) < 0.28:
            return None

        expected, stddev = self._baseline(link_id, window_start)
        condition, temp, precip = self._weather(window_start, segment.get("borough"))

        noise = (_stable_fraction("noise", link_id, slot) - 0.5) * 2.0 * stddev
        chronic = _stable_fraction("chronic", link_id) < 0.10
        incident = _stable_fraction("incident", link_id, slot[:13]) < 0.06
        drop = 0.0
        if chronic:
            drop += 1.9 * stddev
        if incident:
            drop += 2.6 * stddev
        if condition in ("rain", "drizzle"):
            drop += 0.55 * stddev

        speed = max(2.0, expected - drop + noise)
        has_baseline = link_id not in self._no_baseline

        score = None
        if has_baseline:
            score = round((expected - speed) / stddev, 2)

        return SegmentWindow(
            link_id=link_id,
            window_start=window_start,
            window_end=window_start + timedelta(minutes=WINDOW_MINUTES),
            speed_avg=round(speed, 2),
            sample_count=1 + int(_stable_fraction("n", link_id, slot) * 3),
            baseline_speed=expected if has_baseline else None,
            baseline_stddev=stddev if has_baseline else None,
            congestion_score=score,
            has_baseline=has_baseline,
            borough=segment.get("borough"),
            link_name=segment.get("link_name"),
            link_points=segment.get("link_points"),
            weather_condition=condition,
            temperature_c=round(temp, 1),
            precipitation_mm=precip,
            is_late_arrival=_stable_fraction("late", link_id, slot) < 0.03,
        )

    def latest_windows(self) -> list[SegmentWindow]:
        window_start = floor_window(datetime.now(timezone.utc)) - timedelta(
            minutes=WINDOW_MINUTES
        )
        out = []
        for segment in self.seed:
            w = self._window_for(segment, window_start)
            if w is not None:
                out.append(w)
        return out

    def timeseries(self, link_id: str, hours: int) -> list[SegmentWindow]:
        segment = next((s for s in self.seed if s["link_id"] == link_id), None)
        if segment is None:
            return []
        end = floor_window(datetime.now(timezone.utc))
        steps = int(hours * 60 / WINDOW_MINUTES)
        out = []
        for i in range(steps, 0, -1):
            w = self._window_for(segment, end - timedelta(minutes=WINDOW_MINUTES * i))
            if w is not None:
                out.append(w)
        return out

    def reference_speed(self, link_id: str, ts: datetime) -> float | None:
        if link_id in self._no_baseline:
            return None
        return self._baseline(link_id, ts)[0]

    def baseline_links(self) -> set[str]:
        return {s["link_id"] for s in self.seed} - self._no_baseline

    def probe(self) -> tuple[bool, str]:
        return True, f"Fixture-Modus, {len(self.seed)} Segmente aus dem Seed"


class BaselineIndex:
    """Erwartungswert und Streuung je link_id x Wochentag x Stunde, aus
    baseline_profile gelesen und gecacht."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._cache = TTLCache(settings.baseline_ttl_s)

    @staticmethod
    def spark_weekday(ts: datetime) -> int:
        """Sparks ``dayofweek``-Zaehlung: 1 = Sonntag. Muss exakt wie in
        compute_baseline.py gebildet werden, sonst trifft der Join die
        falsche Zelle."""
        return (ts.isoweekday() % 7) + 1

    def _load(self) -> dict[tuple[str, int, int], tuple[float, float]]:
        try:
            dt = DeltaTable(
                self.settings.baseline_uri,
                storage_options=self.settings.storage_options(),
            )
            rows = dt.to_pyarrow_table(
                columns=[
                    "link_id",
                    "weekday",
                    "hour_of_day",
                    "baseline_speed",
                    "baseline_stddev",
                ]
            ).to_pylist()
        except Exception as exc:
            log.warning(
                "Baseline-Tabelle %s nicht lesbar (%s) — alle Segmente gelten "
                "als unbewertbar",
                self.settings.baseline_uri,
                exc,
            )
            return {}

        index: dict[tuple[str, int, int], tuple[float, float]] = {}
        for r in rows:
            stddev = r.get("baseline_stddev")
            speed = r.get("baseline_speed")
            # stddev=null (eine Messung) oder 0 (konstant): z-Score undefiniert.
            if speed is None or stddev is None or stddev <= 0:
                continue
            index[(r["link_id"], int(r["weekday"]), int(r["hour_of_day"]))] = (
                float(speed),
                float(stddev),
            )
        log.info(
            "Baseline geladen: %d Zellen ueber %d Segmente",
            len(index),
            len({k[0] for k in index}),
        )
        return index

    def lookup(self, link_id: str, window_start: datetime) -> tuple[float, float] | None:
        index = self._cache.get(self._load)
        ts = _as_utc(window_start)
        return index.get((link_id, self.spark_weekday(ts), ts.hour))

    def cells(self) -> int:
        return len(self._cache.get(self._load))

    def links(self) -> set[str]:
        return {link_id for link_id, _, _ in self._cache.get(self._load)}


def _round(value, digits: int = 2) -> float | None:
    return round(float(value), digits) if value is not None else None


def _as_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


class DeltaReader:
    """Liest die Gold-Tabelle von MinIO. Der Streaming-Job schreibt Baseline-
    Join und Score selbst (SCRUM-83); die API liest die Spalten nur, wie sie
    im Vertrag stehen."""

    name = "delta"

    # Vertragsname (models.SegmentWindow) -> Spalte im Sink. Eine Stelle fuer
    # eine kuenftige Abweichung, statt sie ueber den Reader zu verteilen.
    SINK_COLUMNS = {
        "link_id": "link_id",
        "window_start": "window_start",
        "window_end": "window_end",
        "speed_avg": "speed_avg",
        "sample_count": "sample_count",
        "baseline_speed": "baseline_speed",
        "baseline_stddev": "baseline_stddev",
        "congestion_score": "congestion_score",
        "has_baseline": "has_baseline",
        "borough": "borough",
        "link_name": "link_name",
        "link_points": "link_points",
        "weather_condition": "weather_condition",
        "temperature_c": "temperature_c",
        "precipitation_mm": "precipitation_mm",
        "is_late_arrival": "is_late_arrival",
    }

    REQUIRED = ("link_id", "window_start", "window_end", "speed_avg")

    def __init__(self, settings: Settings):
        self.settings = settings
        self._storage = settings.storage_options()
        self.baseline = BaselineIndex(settings)

    def _table(self):
        try:
            return DeltaTable(self.settings.delta_uri, storage_options=self._storage)
        except Exception as exc:
            raise ReaderError(f"Delta-Tabelle nicht lesbar: {exc}") from exc

    def _query(self, since: datetime, link_id: str | None = None) -> list[dict]:
        dt = self._table()
        dataset = dt.to_pyarrow_dataset()
        available = set(dataset.schema.names)

        missing = [c for c in self.REQUIRED if c not in available]
        if missing:
            raise ReaderError(
                f"Gold-Tabelle ohne Pflichtspalten {missing} — "
                "Vertrag und Sink laufen auseinander, siehe docs/gold-contract.md"
            )

        field = dataset.schema.field("window_start")
        since = since if getattr(field.type, "tz", None) else since.replace(tzinfo=None)

        expr = pads.field("window_start") >= since
        if "window_date" in available:
            expr = (pads.field("window_date") >= since.strftime("%Y-%m-%d")) & expr
        if link_id is not None:
            expr = expr & (pads.field("link_id") == link_id)

        columns = {
            alias: pads.field(source)
            for alias, source in self.SINK_COLUMNS.items()
            if source in available
        }
        absent = [a for a in self.SINK_COLUMNS if a not in columns]
        if absent:
            log.warning("Gold-Tabelle ohne Spalten %s — Vertrag pruefen", absent)

        try:
            table = dataset.to_table(filter=expr, columns=columns)
        except Exception as exc:
            raise ReaderError(f"Abfrage fehlgeschlagen: {exc}") from exc
        return table.to_pylist()

    def _to_model(self, row: dict) -> SegmentWindow:
        speed = row.get("speed_avg")
        score = row.get("congestion_score")
        return SegmentWindow(
            link_id=row["link_id"],
            window_start=_as_utc(row["window_start"]),
            window_end=_as_utc(row["window_end"]),
            speed_avg=round(float(speed), 2) if speed is not None else None,
            sample_count=int(row.get("sample_count") or 0),
            baseline_speed=_round(row.get("baseline_speed")),
            baseline_stddev=_round(row.get("baseline_stddev")),
            congestion_score=round(float(score), 2) if score is not None else None,
            has_baseline=bool(row.get("has_baseline")),
            borough=row.get("borough"),
            link_name=row.get("link_name"),
            link_points=row.get("link_points"),
            weather_condition=row.get("weather_condition"),
            temperature_c=_round(row.get("temperature_c")),
            precipitation_mm=_round(row.get("precipitation_mm")),
            is_late_arrival=bool(row.get("is_late_arrival") or False),
        )

    def _tumbling(self, rows: list[dict]) -> list[dict]:
        """Der Sink schreibt 5-Minuten-Fenster mit 1 Minute Versatz, also
        fuenf ueberlappende Zeilen je Segment und Fuenfminutenblock. Fuer
        eine Zeitreihe ist nur jede fuenfte eine neue Information."""
        if not self.settings.tumbling_only:
            return rows
        aligned = [r for r in rows if _as_utc(r["window_start"]).minute % WINDOW_MINUTES == 0]
        return aligned or rows

    def latest_windows(self) -> list[SegmentWindow]:
        since = datetime.now(timezone.utc) - timedelta(minutes=WINDOW_MINUTES * 3)
        newest: dict[str, dict] = {}
        for row in self._query(since):
            prev = newest.get(row["link_id"])
            if prev is None or _as_utc(row["window_start"]) > _as_utc(prev["window_start"]):
                newest[row["link_id"]] = row
        return [self._to_model(r) for r in newest.values()]

    def timeseries(self, link_id: str, hours: int) -> list[SegmentWindow]:
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        rows = self._tumbling(self._query(since, link_id=link_id))
        rows.sort(key=lambda r: _as_utc(r["window_start"]))
        return [self._to_model(r) for r in rows]

    def reference_speed(self, link_id: str, ts: datetime) -> float | None:
        cell = self.baseline.lookup(link_id, ts)
        return cell[0] if cell else None

    def baseline_links(self) -> set[str]:
        return self.baseline.links()

    def probe(self) -> tuple[bool, str]:
        try:
            dt = self._table()
            cells = self.baseline.cells()
            detail = f"Delta-Version {dt.version()} unter {self.settings.delta_uri}"
            if cells:
                detail += f", Baseline mit {cells} Zellen"
            else:
                detail += ", OHNE Baseline (alle Segmente unbewertbar)"
            return True, detail
        except ReaderError as exc:
            return False, str(exc)


def build_reader(settings: Settings) -> GoldReader:
    if settings.gold_reader == "delta":
        log.info("Gold-Reader: delta (%s)", settings.delta_uri)
        return DeltaReader(settings)
    log.warning("Gold-Reader: FIXTURE — Entwicklungsmodus, nicht der Abgabestand.")
    return FixtureReader(settings)


def segments_from(
    seed: list[dict],
    windows: list[SegmentWindow],
    baseline_links: set[str] | None = None,
) -> list[Segment]:
    """Kartengrundlage: alle Seed-Segmente, angereichert um den letzten
    bekannten Zustand. ``baseline_links`` unterscheidet "hat gerade nichts
    gemeldet" von "hat keine Historie"."""
    by_id = {w.link_id: w for w in windows}
    out = []
    for s in seed:
        link_id = s["link_id"]
        w = by_id.get(link_id)
        if baseline_links is not None:
            has_baseline = link_id in baseline_links
        else:
            has_baseline = bool(w.has_baseline) if w else False
        out.append(
            Segment(
                link_id=link_id,
                link_name=s.get("link_name"),
                borough=s.get("borough"),
                link_points=(w.link_points if w and w.link_points else s.get("link_points")),
                has_baseline=has_baseline,
                last_seen=w.window_start if w else None,
                last_speed=w.speed_avg if w else None,
                last_score=w.congestion_score if w else None,
            )
        )
    return out
