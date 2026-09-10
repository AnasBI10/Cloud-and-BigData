# Gold-Layer-Vertrag (SCRUM-79 ↔ SCRUM-82/83/84/86)

Schnittstelle zwischen Processing und Serving. Stand: 10.09.2026, abgeglichen
mit dem realen Sink (`src/processing/streaming_job_bsg.py`) und dem realen
Reader (`src/serving/readers.py`).

## Tabellen

### 1. Fenster-Tabelle — `s3a://gold/congestion_scores`

Geschrieben vom Streaming-Job, Merge-Key `(link_id, window_start)`,
partitioniert nach `window_date`.

| Spalte | Typ | Bedeutung |
|---|---|---|
| `link_id` | string | Segment-ID aus dem DOT-Feed |
| `window_start` | timestamp (UTC) | Beginn des Aggregationsfensters |
| `window_end` | timestamp (UTC) | Ende des Aggregationsfensters |
| `window_date` | string `YYYY-MM-DD` | Partitionsspalte |
| `speed_avg` | double | Mittlere Geschwindigkeit im Fenster (mph), nur `status = 0` |
| `sample_count` | int | Anzahl gueltiger Messungen im Fenster |
| `baseline_speed` | double, nullable | Erwartungswert aus der Baseline |
| `baseline_stddev` | double, nullable | Streuung der Baseline-Zelle |
| `congestion_score` | double, nullable | `(baseline_speed - speed_avg) / baseline_stddev`, vom Job berechnet |
| `has_baseline` | boolean | `false` ohne ausreichende Historie |
| `borough` | string, nullable | aus dem Seed angereichert |
| `link_name` | string, nullable | Klartext |
| `link_points` | string, nullable | Polylinie, vom Event durchgereicht |
| `weather_condition`, `temperature_c`, `precipitation_mm` | nullable | Wetter-Join, noch offen (SCRUM-84b) — bis dahin durchgehend `null` |
| `is_late_arrival` | boolean | derzeit `false` (Platzhalter, siehe unten) |
| `updated_at` | timestamp | Schreibzeitpunkt, von der API nicht gelesen |

**Fenster: 5 Minuten Laenge, 1 Minute Versatz (gleitend).** Fuer die
Zeitreihe waehlt die API nur die auf 5 Minuten ausgerichteten Fenster aus
(`TUMBLING_ONLY=true`). Fenster ohne gueltige Messung werden nicht
geschrieben — keine Interpolation.

**`congestion_score` steht direkt in der Tabelle**, vom Spark-Job berechnet.
Die API liest ihn unveraendert (`readers.DeltaReader._to_model`), rechnet
nichts nach. Positiv = langsamer als erwartet. Ohne Baseline bleibt er
`null`, nicht `0`.

**`has_baseline = false` ist ein Befund, kein Fehler.** Die API zaehlt
solche Segmente separat (`segments_without_baseline`) statt sie als
unauffaellig zu behandeln.

**`is_late_arrival` ist aktuell ein Platzhalter.** Der Job schliesst als
"late" markierte Events komplett von der Aggregation aus (DLQ statt Gold) —
dadurch kann kein Fenster als "late-korrigiert" markiert sein. Sichtbar wird
Late-Data ueber die DLQ (`traffic.speeds.dlq`), nicht ueber dieses Feld.
Eine echte pro-Fenster-Markierung braucht eine inkrementelle Merge-Logik in
`upsert_gold()` (aktuell ersetzt `whenMatchedUpdateAll()` eine Zeile
komplett statt `avg`/`sample_count` mit dem Altbestand zu verrechnen) —
eigener, spaeterer Schritt.

### 2. Baseline-Tabelle — `s3a://gold/baseline_profile`

Geschrieben vom CronJob `baseline-profile` (`compute_baseline.py`), Batch
ueber die Silver-Schicht, jeder Lauf ueberschreibt vollstaendig.

| Spalte | Typ | Bedeutung |
|---|---|---|
| `link_id` | string | Segment-ID |
| `weekday` | int | Sparks `dayofweek`-Zaehlung: 1 = Sonntag, 7 = Samstag |
| `hour_of_day` | int | Stunde 0–23 |
| `baseline_speed` | double | Mittelwert der Zelle |
| `baseline_stddev` | double, nullable | Streuung der Zelle |
| `sample_count` | int | Messungen in der Zelle, `>= 5` (`MIN_SAMPLES_PER_CELL`) |

Die API liest diese Tabelle zusaetzlich (`readers.BaselineIndex`) fuer zwei
Fragen, die die Fenster-Tabelle allein nicht beantwortet: welche Segmente
ueberhaupt bewertbar sind (`links()`, fuer die Karte — ein Segment ohne
aktuelle Messung ist sonst nicht von einem ohne Historie zu unterscheiden)
und welche Geschwindigkeit fuer eine Stunde ueblich ist (`lookup()`,
Ausgangswert des Szenario-Generators).

## Zeitzone

Alles UTC, auch der Baseline-Schluessel. Keine explizite
`from_utc_timestamp`-Konvertierung im Code: der Feed liefert schon
NY-Lokalzeit, nur mit UTC-Label versehen (`live_poller.py._parse_ts`) — eine
zusaetzliche Konvertierung wuerde das ein zweites Mal verschieben.
`compute_baseline.py` und `streaming_job_bsg.py` bilden den Wochentag/Stunde-
Schluessel deshalb beide direkt aus dem Zeitstempel, ohne TZ-Umrechnung.
Umrechnung auf `America/New_York` fuer die Anzeige passiert erst in der UI.

## Was die API zusaetzlich braucht

`GET /api/segments` liefert die Kartengrundlage fuer alle 125 Segmente, auch
ohne aktuelle Messung. `link_name`, `borough` und `link_points` (falls im
letzten Fenster leer) kommen aus `data/dot_links_seed.json`.

## Umstellung pruefen

Abgabestand: `GOLD_READER=delta` (ConfigMap `serving-config`).

```bash
# Baseline einmalig erzeugen, statt auf den 6-Stunden-Slot zu warten
kubectl -n bigdata create job baseline-initial --from=cronjob/baseline-profile
kubectl -n bigdata logs -f job/baseline-initial | tail -5

# /ready muss "ready" liefern, latest_window darf nicht null sein
kubectl -n bigdata port-forward svc/serving-api 8000:80 &
curl -s localhost:8000/ready | jq
```
