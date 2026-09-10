# Gold-Layer-Vertrag (SCRUM-79 ↔ SCRUM-82/83/84/86)

Schnittstelle zwischen Processing (VT/AG) und Serving (BT).

Stand: 10.09.2026, abgeglichen mit dem realen Sink aus SCRUM-86
(`src/processing/streaming_job_bsg.py`, Funktion `process_batch`).

**Der Vertrag beschreibt ab hier den Ist-Zustand, nicht den Wunschzustand.**
Die frueher hier festgehaltene Fassung war mit dem Sink nie abgestimmt worden;
alle Abweichungen sind unten protokolliert. Angepasst wurde die lesende Seite
(`src/serving/readers.py`, `models.py`) — der Sink blieb unangetastet, weil
SCRUM-86 abgeschlossen ist und SCRUM-83/87 darauf aufsetzen.

## Tabellen

Die Serving-API liest **zwei** Delta-Tabellen und verbindet sie im Speicher.

### 1. Fenster-Tabelle — `s3a://gold/congestion_scores`

Geschrieben vom Streaming-Job, Merge-Schluessel `(link_id, window_start)`.
Unpartitioniert.

| Spalte im Sink | Typ | Vertragsname in der API | Bedeutung |
|---|---|---|---|
| `link_id` | string | `link_id` | Segment-ID aus dem DOT-Feed |
| `window_start` | timestamp (UTC) | `window_start` | Beginn des Aggregationsfensters |
| `window_end` | timestamp (UTC) | `window_end` | Ende des Aggregationsfensters |
| `avg_speed_mph` | double | `speed_avg` | Mittlere Geschwindigkeit im Fenster (mph), nur aus `status = 0` |
| `sample_count` | bigint | `sample_count` | Anzahl gueltiger Messungen im Fenster |
| `congestion_score` | int | **`speed_index`** | 0–100 allein aus der Absolutgeschwindigkeit, siehe unten |
| `borough` | string, nullable | `borough` | Join-Schluessel Wetter, aus dem Seed angereichert |
| `late_event_detected` | boolean | `is_late_arrival` | derzeit konstant `false`, siehe offene Punkte |
| `updated_at` | timestamp | — | Schreibzeitpunkt, von der API nicht gelesen |

**Fenster: 5 Minuten Laenge, 1 Minute Versatz (gleitend, nicht tumbling).**
Je Segment und Fuenfminutenblock entstehen dadurch fuenf Zeilen, die einander
zu 80 % enthalten. Fuer die Zeitreihe im Dashboard waehlt die API die auf
volle fuenf Minuten ausgerichteten Fenster aus (`TUMBLING_ONLY=true`), sonst
zeigt der Chart viermal dieselbe Messung. Fenster ohne gueltige Messung
werden nicht geschrieben — die API interpoliert nicht, sie zeigt Luecken als
Luecken.

### 2. Baseline-Tabelle — `s3a://gold/baseline_profile`

Geschrieben vom CronJob `baseline-profile`
(`src/processing/compute_baseline.py`, Manifest
`deploy/helm/congestion-watch/templates/baseline.yaml`), Batch ueber die
Silver-Schicht, jeder Lauf ueberschreibt vollstaendig.

| Spalte | Typ | Bedeutung |
|---|---|---|
| `link_id` | string | Segment-ID |
| `weekday` | int | Wochentag in Sparks `dayofweek`-Zaehlung: **1 = Sonntag**, 7 = Samstag |
| `hour_of_day` | int | Stunde 0–23 |
| `baseline_speed` | double | Mittelwert der Zelle |
| `baseline_stddev` | double, nullable | Streuung der Zelle |
| `sample_count` | bigint | Messungen in der Zelle, `>= 5` (`MIN_SAMPLES_PER_CELL`) |

## Was die API aus beidem macht

`congestion_score` steht **nicht** in der Gold-Tabelle. Die API bildet ihn beim
Lesen (`readers.DeltaReader._to_model`):

    congestion_score = (baseline_speed - speed_avg) / baseline_stddev

**Vorzeichen: positiv = langsamer als erwartet = auffaellig.** Ein Wert von
+2.0 heisst: zwei Standardabweichungen langsamer als fuer diesen `link_id` ×
Wochentag × Stunde ueblich. Negative Werte (schneller als erwartet) sind
gueltig, werden aber im Dashboard nicht als Anomalie gelistet.

Dass die API hier rechnet, ist eine bewusste Abweichung vom urspruenglichen
Grundsatz „die API rechnet nicht". Die Alternative waere gewesen, den
Baseline-Join nachtraeglich in den fertigen Streaming-Job zu bauen. Der
Rechenaufwand ist ein Dictionary-Zugriff je Zeile ueber maximal 125 × 168
Zellen; die Baseline wird mit eigenem TTL (`BASELINE_TTL_S`, 900 s) gecacht,
weil sie sich nur alle sechs Stunden aendert.

**`has_baseline = false` ist kein Fehler, sondern ein Befund.** Findet die API
fuer `link_id` × Wochentag × Stunde keine Zelle — oder ist deren `stddev` null
bzw. 0 —, bleiben `baseline_speed`, `baseline_stddev` und `congestion_score`
`null` und `has_baseline` ist `false`. Solche Segmente erscheinen **nicht** in
der Anomalie-Rangliste; ihre Zahl steht stattdessen im Antwortumschlag
(`segments_without_baseline`). Das Dashboard muss sie optisch von
„unauffaellig" trennen: das ist der Unterschied zwischen „kein Stau" und
„wissen wir nicht" (README, Abschnitt 2, Veracity).

Fehlt die Baseline-Tabelle ganz, laeuft die API weiter und meldet jedes
Segment als unbewertbar. `/ready` bleibt `ready`, das Detail nennt aber
`OHNE Baseline`.

`speed_index` ist der Score, den der Spark-Job selbst schreibt: 0 bei ≥ 35 mph,
100 bei ≤ 15 mph, linear dazwischen. Er wird unveraendert durchgereicht, damit
nichts stillschweigend umgedeutet wird, ist aber **kein** Stau-Mass — der
Lincoln Tunnel faehrt planmaessig 20 mph und bekaeme dauerhaft 75, ohne dass
etwas los waere. Fuer die Rangliste zaehlt `congestion_score`.

**Zeitzone: alles UTC**, auch der Baseline-Schluessel (siehe offene Punkte).
Umrechnung auf `America/New_York` passiert erst in der UI.

## Was die API zusaetzlich braucht

`GET /api/segments` liefert die Kartengrundlage fuer alle 125 Segmente, auch
fuer die ohne aktuelle Messung. `link_name` und `borough` kommen aus
`data/dot_links_seed.json`, weil der Sink sie nicht mit aggregiert.

## Umstellung pruefen

Die API laeuft im Abgabestand auf `GOLD_READER=delta` (ConfigMap
`serving-config`). Nach dem Deploy:

```bash
# 1. Baseline einmalig erzeugen, statt auf den 6-Stunden-Slot zu warten
kubectl -n bigdata create job baseline-initial --from=cronjob/baseline-profile
kubectl -n bigdata logs -f job/baseline-initial | tail -5

# 2. Readiness: muss "ready" liefern, latest_window darf nicht null sein,
#    und das Detail muss eine Zellenzahl nennen, nicht "OHNE Baseline"
kubectl -n bigdata port-forward svc/serving-api 8000:80 &
curl -s localhost:8000/ready | python3 -m json.tool

# 3. Rangliste: segments_with_baseline muss > 0 sein. Ist sie 0, laeuft der
#    Baseline-Join ins Leere — dann Punkt 1 der offenen Punkte pruefen.
curl -s "localhost:8000/api/anomalies?limit=5" | python3 -m json.tool
```

Lokal ohne Cluster geht dasselbe gegen ein Verzeichnis: `DELTA_URI` und
`BASELINE_URI` auf lokale Pfade zeigen lassen, `GOLD_READER=delta`,
`SEED_PATH=data/dot_links_seed.json`, dann
`python -m uvicorn main:app --app-dir src/serving`.

## Offene Punkte

Alle fuenf betreffen das Processing, nicht die API.

1. **Baseline-Schluessel in UTC statt NYC-Ortszeit.** `compute_baseline.py`
   bildet `weekday`/`hour_of_day` direkt aus `event_time`, also aus UTC. Der
   New Yorker Feierabend (17 Uhr EDT) landet damit in der Zelle 21 Uhr, und
   bei der Zeitumstellung wandert er um eine Stunde. Die API bildet den
   Schluessel bewusst genauso, sonst trifft der Join die falsche Zelle. Fuer
   die Abgabe entweder in `compute_baseline.py` auf
   `from_utc_timestamp(event_time, 'America/New_York')` umstellen (dann muss
   `BaselineIndex.lookup` mitziehen) oder in README Kapitel 5 als bekannte
   Einschraenkung begruenden.
2. **Keine Partitionsspalte `window_date`** (SCRUM-87). Jede Abfrage liest
   damit alle Parquet-Dateien der Tabelle. Der Reader nutzt die Spalte
   automatisch zum Partitionspruning, sobald der Sink sie schreibt —
   `readers.DeltaReader._query` prueft das zur Laufzeit, es braucht dafuer
   keine Aenderung an der API.
3. **`late_event_detected` ist konstant `false`** (`lit(False)` im Sink). Die
   Erkennung verspaeteter Events existiert, sie schreibt aber in den
   DLQ-Topic statt in die Spalte. Solange das so ist, kann das Dashboard keine
   korrigierten Fenster markieren.
4. **Kein Wetter-Enrichment in Gold** (SCRUM-84, „wird ueberprueft").
   `weather_condition`, `temperature_c` und `precipitation_mm` sind in jeder
   API-Antwort `null`. Die Felder bleiben im Modell, damit der Vertrag steht,
   sobald der Join geliefert wird.
5. **Keine Geometrie fuer die Karte (SCRUM-90).** `link_points` liefert nur
   der Live-Poller in die Bronze-/Silver-Schicht; der Gold-Sink aggregiert es
   weg und `data/dot_links_seed.json` enthaelt es nicht. Fuer die Karte muss
   entweder der Seed einmalig um `link_points` aus dem DOT-Feed ergaenzt
   werden (einmaliger Datenlauf, keine Pipeline-Aenderung) oder die Karte
   kommt ohne Polylinien aus.
