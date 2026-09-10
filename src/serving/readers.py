"""Zugriff auf die Gold-Schicht (SCRUM-79).

Zwei Implementierungen hinter einer Schnittstelle:

* ``FixtureReader``  — erzeugt den Vertrag aus docs/gold-contract.md aus den
  echten link_id-Werten des Seeds. Entwicklungsmodus, solange SCRUM-86 die
  Delta-Tabelle noch nicht schreibt. NICHT der Abgabestand.
* ``DeltaReader``    — liest die echte Delta-Tabelle von MinIO und verbindet
  sie ueber ``BaselineIndex`` mit der Baseline-Tabelle aus
  compute_baseline.py. Der Sink schreibt andere Spaltennamen als der Vertrag;
  die Abbildung steht in ``DeltaReader.SINK_COLUMNS``.

Bewusst ohne Spark: die API braucht keine Session, keinen Executor und keine
JVM, um eine Delta-Tabelle zu lesen. delta-rs liest das Transaktionslog nativ,
der Pod bleibt bei ~200 MB statt ~1,5 GB. Das ist auch der Grund, warum die
Serving-Schicht unabhaengig vom Spark-Job skaliert (SCRUM-93).
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

from config import Settings
from models import Segment, SegmentWindow

log = logging.getLogger("serving.readers")

WINDOW_MINUTES = 5


class ReaderError(RuntimeError):
    """Gold-Schicht nicht lesbar. Fuehrt zu 503, nicht zu 500 — der Dienst ist
    in Ordnung, seine Datenquelle nicht."""


class GoldReader(Protocol):
    name: str

    def latest_windows(self) -> list[SegmentWindow]: ...

    def timeseries(self, link_id: str, hours: int) -> list[SegmentWindow]: ...

    def reference_speed(self, link_id: str, ts: datetime) -> float | None: ...

    def probe(self) -> tuple[bool, str]: ...


# ---------------------------------------------------------------------------
# Hilfsmittel
# ---------------------------------------------------------------------------


class TTLCache:
    """Ein Wert, eine Ablaufzeit, ein Lock.

    Der Grund steht in config.py: das Dashboard pollt haeufiger, als die
    Gold-Tabelle neue Fenster bekommt.
    """

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
    """Dieselbe Datei, die auch die Producer verwenden (src/ingestion/common.py).
    Sie ist die Stammdatenquelle fuer die Karte: 125 Segmente mit Borough und
    Klartextnamen."""
    with settings.seed_path.open(encoding="utf-8") as fh:
        seed = json.load(fh)
    log.info("Seed geladen: %d Segmente", len(seed))
    return seed


def _stable_fraction(*parts: str) -> float:
    """Deterministischer Wert in [0,1) aus beliebigen Strings.

    Deterministisch und nicht zufaellig, damit dieselbe link_id ueber
    Neustarts und ueber mehrere API-Repliken hinweg dasselbe Profil hat.
    Bei zwei Repliken hinter einem Service wuerde ein echter Zufallswert
    sonst je nach getroffenem Pod andere Zahlen liefern.
    """
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") / float(1 << 64)


def floor_window(ts: datetime) -> datetime:
    minute = (ts.minute // WINDOW_MINUTES) * WINDOW_MINUTES
    return ts.replace(minute=minute, second=0, microsecond=0)


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


class FixtureReader:
    """Erzeugt vertragskonforme Fenster ohne Delta-Tabelle.

    Modelliert bewusst die Eigenschaften, die im Dashboard sichtbar werden
    muessen und die in der README als Befund dokumentiert sind:

    * Tagesgang (nachts frei, Feierabend langsam)
    * ~25 % der Segmente ohne Messung im aktuellen Fenster (Meldefrequenz
      ~7,7 Minuten trifft nicht jedes 5-Minuten-Fenster)
    * 31 von 125 Segmenten ohne Baseline -> unbewertbar, nicht unauffaellig
    * einige dauerhaft auffaellige Segmente, damit die Rangliste nicht leer ist
    """

    name = "fixture"

    def __init__(self, settings: Settings):
        self.settings = settings
        self.seed = load_seed(settings)
        # Deterministische Auswahl der Segmente ohne Historie. Entspricht dem
        # gemessenen Befund: 94 von 125 Segmenten haben Baseline-Abdeckung.
        self._no_baseline = {
            s["link_id"]
            for s in self.seed
            if _stable_fraction("baseline", s["link_id"]) < 31 / 125
        }

    # -- Modell ----------------------------------------------------------

    def _baseline(self, link_id: str, ts: datetime) -> tuple[float, float]:
        """Erwartungswert und Streuung fuer link_id x Wochentag x Stunde."""
        base = 22.0 + 26.0 * _stable_fraction("speed", link_id)
        hour = ts.hour
        # Tagesgang: Minimum gegen 8 und 18 Uhr, Maximum nachts.
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

        # Nicht jedes Segment meldet in jedem Fenster.
        if _stable_fraction("present", link_id, slot) < 0.28:
            return None

        expected, stddev = self._baseline(link_id, window_start)
        condition, temp, precip = self._weather(window_start, segment.get("borough"))

        # Abweichung: meist Rauschen, bei einigen Segmenten dauerhaft nach unten.
        noise = (_stable_fraction("noise", link_id, slot) - 0.5) * 2.0 * stddev
        chronic = _stable_fraction("chronic", link_id) < 0.10
        incident = _stable_fraction("incident", link_id, slot[:13]) < 0.06
        drop = 0.0
        if chronic:
            drop += 1.9 * stddev
        if incident:
            drop += 2.6 * stddev
        if condition in ("rain", "drizzle"):
            # Regen macht alle langsamer — das ist genau der Effekt, den die
            # Baseline herausrechnen soll (README 1.1).
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

    # -- Schnittstelle ---------------------------------------------------

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

    def probe(self) -> tuple[bool, str]:
        return True, f"Fixture-Modus, {len(self.seed)} Segmente aus dem Seed"


# ---------------------------------------------------------------------------
# Baseline
# ---------------------------------------------------------------------------


class BaselineIndex:
    """Erwartungswert und Streuung je ``link_id`` x Wochentag x Stunde.

    Der Gold-Sink (SCRUM-86) schreibt keine Baseline mit — er kennt nur die
    Absolutgeschwindigkeit des Fensters. Die Historie liegt in einer zweiten
    Tabelle, die ``src/processing/compute_baseline.py`` als Batch aus der
    Silver-Schicht berechnet. Die API verbindet beide.

    Damit bleibt die Aussage erhalten, auf der das Dashboard steht: ein
    Segment ist erst dann auffaellig, wenn es gemessen an SEINER eigenen
    Historie langsam ist, nicht wenn es absolut langsam ist. Der Lincoln
    Tunnel faehrt immer 20 mph — das ist kein Stau, das ist Dienstag.

    Fehlt die Tabelle, ist der Index leer. Dann ist ``has_baseline`` ueberall
    ``false``: unbewertbar, nicht unauffaellig. Kein Grund fuer einen 503.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self._cache = TTLCache(settings.baseline_ttl_s)

    @staticmethod
    def spark_weekday(ts: datetime) -> int:
        """Wochentag in der Zaehlung von Sparks ``dayofweek``: 1 = Sonntag.

        Python zaehlt ab Montag. Die Schluessel muessen exakt so gebildet
        werden wie in compute_baseline.py, sonst trifft der Join die falsche
        Zelle und der Score ist um Tage verschoben.
        """
        return (ts.isoweekday() % 7) + 1

    def _load(self) -> dict[tuple[str, int, int], tuple[float, float]]:
        try:
            from deltalake import DeltaTable
        except ImportError as exc:  # pragma: no cover
            raise ReaderError("deltalake nicht installiert") from exc

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
            # Haeufigster Fall: compute_baseline.py ist noch nicht gelaufen.
            log.warning(
                "Baseline-Tabelle %s nicht lesbar (%s) — alle Segmente gelten "
                "als unbewertbar, has_baseline=false",
                self.settings.baseline_uri,
                exc,
            )
            return {}

        index: dict[tuple[str, int, int], tuple[float, float]] = {}
        for r in rows:
            stddev = r.get("baseline_stddev")
            speed = r.get("baseline_speed")
            # stddev ist null bei Zellen mit genau einer Messung und 0.0 bei
            # konstanter Geschwindigkeit. In beiden Faellen ist der z-Score
            # nicht definiert — die Zelle zaehlt nicht als Baseline.
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


def _as_utc(ts: datetime) -> datetime:
    """Delta liefert je nach Schreiber tz-behaftete oder naive Zeitstempel.
    Der Vertrag sagt UTC — naive Werte werden entsprechend gelesen."""
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Delta
# ---------------------------------------------------------------------------


class DeltaReader:
    """Liest die Gold-Tabelle von MinIO (SCRUM-86) und verbindet sie mit der
    Baseline (compute_baseline.py).

    Bewusst ohne Spark: delta-rs liest das Transaktionslog nativ, der Pod
    bleibt bei ~200 MB statt ~1,5 GB. Das ist auch der Grund, warum die
    Serving-Schicht unabhaengig vom Spark-Job skaliert (SCRUM-93).

    Die Spaltennamen des Sinks sind NICHT die des Vertrags. Die Abbildung
    steht in ``SINK_COLUMNS`` und ist die einzige Stelle, die beides kennt —
    Abweichungen und ihr Stand sind in docs/gold-contract.md protokolliert.
    """

    name = "delta"

    # Vertragsname (models.SegmentWindow) -> Spalte, wie der Sink sie schreibt
    # (src/processing/streaming_job_bsg.py, Funktion process_batch).
    SINK_COLUMNS = {
        "link_id": "link_id",
        "window_start": "window_start",
        "window_end": "window_end",
        "speed_avg": "avg_speed_mph",
        "sample_count": "sample_count",
        "speed_index": "congestion_score",
        "borough": "borough",
        "is_late_arrival": "late_event_detected",
    }

    # Pflichtspalten. Fehlt eine davon, passt die Tabelle nicht zum Vertrag
    # und ein stiller Teil-Erfolg waere schlimmer als ein klarer Fehler.
    REQUIRED = ("link_id", "window_start", "window_end", "avg_speed_mph")

    def __init__(self, settings: Settings):
        self.settings = settings
        self._storage = settings.storage_options()
        self.baseline = BaselineIndex(settings)

    def _table(self):
        try:
            from deltalake import DeltaTable
        except ImportError as exc:  # pragma: no cover
            raise ReaderError("deltalake nicht installiert") from exc
        try:
            return DeltaTable(self.settings.delta_uri, storage_options=self._storage)
        except Exception as exc:
            # Haeufigster Fall im Betrieb: der Spark-Job hat noch nichts
            # geschrieben, die Tabelle existiert nicht. Das ist kein Absturz,
            # sondern ein "noch nicht bereit".
            raise ReaderError(f"Delta-Tabelle nicht lesbar: {exc}") from exc

    def _query(self, since: datetime, link_id: str | None = None) -> list[dict]:
        import pyarrow.dataset as pads

        dt = self._table()
        dataset = dt.to_pyarrow_dataset()
        available = set(dataset.schema.names)

        missing = [c for c in self.REQUIRED if c not in available]
        if missing:
            raise ReaderError(
                f"Gold-Tabelle ohne Pflichtspalten {missing} — "
                "Vertrag und Sink laufen auseinander, siehe docs/gold-contract.md"
            )

        # Zeitstempel muessen zur Spalte passen: vergleicht man einen
        # tz-behafteten Wert mit einer naiven Spalte, wirft pyarrow.
        field = dataset.schema.field("window_start")
        since = since if getattr(field.type, "tz", None) else since.replace(tzinfo=None)

        expr = pads.field("window_start") >= since
        if "window_date" in available:
            # Partitionspruning, sobald SCRUM-87 die Spalte schreibt: schneidet
            # ganze Verzeichnisse weg, bevor eine Datei geoeffnet wird. Der
            # aktuelle Sink partitioniert noch nicht, deshalb optional.
            expr = (pads.field("window_date") >= since.strftime("%Y-%m-%d")) & expr
        if link_id is not None:
            expr = expr & (pads.field("link_id") == link_id)

        # pyarrow erwartet Ausdruecke, keine Spaltennamen — so wird die
        # Umbenennung schon beim Lesen erledigt und nicht zeilenweise danach.
        columns = {
            alias: pads.field(source)
            for alias, source in self.SINK_COLUMNS.items()
            if source in available
        }
        absent = [a for a in self.SINK_COLUMNS if a not in columns]
        if absent:
            # Optionale Spalten (Wetter-Enrichment SCRUM-84, Late-Marker) sind
            # ein Uebergangszustand. Nicht abbrechen, aber laut genug loggen.
            log.warning(
                "Gold-Tabelle ohne Spalten %s — Vertrag pruefen (gold-contract.md)",
                absent,
            )

        try:
            table = dataset.to_table(filter=expr, columns=columns)
        except Exception as exc:
            raise ReaderError(f"Abfrage fehlgeschlagen: {exc}") from exc
        return table.to_pylist()

    def _to_model(self, row: dict) -> SegmentWindow:
        """Sink-Zeile plus Baseline-Zelle -> Vertragsobjekt.

        Der Score des Sinks (0-100 aus der Absolutgeschwindigkeit) wandert
        unveraendert nach ``speed_index``. ``congestion_score`` ist die
        standardisierte Abweichung des Vertrags und bleibt ``null``, wenn es
        fuer diese Zelle keine Historie gibt.
        """
        link_id = row["link_id"]
        window_start = _as_utc(row["window_start"])
        speed = row.get("speed_avg")
        speed = float(speed) if speed is not None else None

        baseline = self.baseline.lookup(link_id, window_start)
        score = None
        expected = stddev = None
        if baseline is not None:
            expected, stddev = baseline
            if speed is not None:
                score = round((expected - speed) / stddev, 2)

        return SegmentWindow(
            link_id=link_id,
            window_start=window_start,
            window_end=_as_utc(row["window_end"]),
            speed_avg=round(speed, 2) if speed is not None else None,
            sample_count=int(row.get("sample_count") or 0),
            baseline_speed=round(expected, 2) if expected is not None else None,
            baseline_stddev=round(stddev, 2) if stddev is not None else None,
            congestion_score=score,
            has_baseline=baseline is not None,
            speed_index=(
                float(row["speed_index"]) if row.get("speed_index") is not None else None
            ),
            borough=row.get("borough"),
            # link_name und link_points aggregiert der Sink nicht mit. Beide
            # kommen fuer die Anzeige aus dem Seed (readers.segments_from,
            # main.timeseries), deshalb hier bewusst leer statt geraten.
            link_name=None,
            link_points=None,
            is_late_arrival=bool(row.get("is_late_arrival") or False),
        )

    def _tumbling(self, rows: list[dict]) -> list[dict]:
        """Aus den gleitenden Fenstern des Sinks die nicht ueberlappenden
        herausgreifen.

        Der Job schreibt 5-Minuten-Fenster mit 1 Minute Versatz, also fuenf
        Zeilen je Segment und Fuenfminutenblock, die einander zu 80 Prozent
        enthalten. Fuer eine Zeitreihe ist nur jede fuenfte davon eine neue
        Information.
        """
        if not self.settings.tumbling_only:
            return rows
        aligned = [r for r in rows if _as_utc(r["window_start"]).minute % WINDOW_MINUTES == 0]
        # Nie alles wegfiltern: haette der Job eine andere Fenstergroesse,
        # bliebe sonst eine leere Zeitreihe statt einer dichten.
        return aligned or rows

    def latest_windows(self) -> list[SegmentWindow]:
        # Drei Fenster zurueck, dann je Segment das juengste behalten: bei einer
        # Meldefrequenz von ~7,7 Minuten ist das letzte 5-Minuten-Fenster fuer
        # viele Segmente leer.
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
        """Erwartungswert des Segments fuer diese Stunde.

        Grundlage fuer den Szenario-Generator (SCRUM-89): ein Stau-Szenario
        soll von der ueblichen Geschwindigkeit DIESES Segments ausgehen, nicht
        von einem pauschalen Wert — sonst erzeugt es auf einem langsamen
        Segment eine Beschleunigung.
        """
        cell = self.baseline.lookup(link_id, ts)
        return cell[0] if cell else None

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
    log.warning(
        "Gold-Reader: FIXTURE — Entwicklungsmodus, nicht der Abgabestand. "
        "Fuer echten Betrieb GOLD_READER=delta setzen."
    )
    return FixtureReader(settings)


def segments_from(seed: list[dict], windows: list[SegmentWindow]) -> list[Segment]:
    """Kartengrundlage: alle Seed-Segmente, angereichert um den letzten
    bekannten Zustand. Segmente ohne aktuelle Messung fallen nicht weg —
    sie erscheinen als grau, nicht als nicht vorhanden."""
    by_id = {w.link_id: w for w in windows}
    out = []
    for s in seed:
        w = by_id.get(s["link_id"])
        out.append(
            Segment(
                link_id=s["link_id"],
                link_name=s.get("link_name"),
                borough=s.get("borough"),
                link_points=(w.link_points if w else s.get("link_points")),
                has_baseline=bool(w.has_baseline) if w else False,
                last_seen=w.window_start if w else None,
                last_speed=w.speed_avg if w else None,
                last_score=w.congestion_score if w else None,
            )
        )
    return out
