# DATA_SOURCES.md

Dokumentation der genutzten und geprüften Datenquellen für NYC Congestion Watch.
Pflicht nach Local Law 11 (§ 23-502 d): Quelle, Version/Abrufdatum und
vorgenommene Änderungen sind bei Weiterveröffentlichung anzugeben.

Stand: 02.09.2026 — Datenbeschaffung für Sprint 0 abgeschlossen.
Offene Anschlusspunkte für spätere Sprints stehen am Ende dieser Datei.

---

## 1. NYC DOT Traffic Speeds NBE (Leitquelle)

| | |
|---|---|
| Datensatz-ID | `i4gi-tjb9` |
| Endpunkt | `https://data.cityofnewyork.us/resource/i4gi-tjb9.json` |
| API-Version | SODA2 (bestätigt am 01.09.2026, kein Bruch durch die laufende SODA3-Migration) |
| Zugriff | App Token (`X-App-Token`-Header), erstellt am 01.09.2026 |
| Lizenz | NYC Open Data / Local Law 11 (2012), § 23-502 (d) — keine Registrierungs-, Lizenz- oder Nutzungsbeschränkung; Pflicht zur Angabe von Quelle, Version, Änderungen bei Weiterveröffentlichung |
| Gewährleistung | Ausdrücklich ausgeschlossen (§ 23-504) |

### Feldsatz

```
id, speed, travel_time, status, data_as_of, link_id, link_points,
encoded_poly_line, encoded_poly_line_lvls, owner, transcom_id,
borough, link_name
```

Typen laut API: `id`, `speed`, `travel_time`, `status` = text;
`data_as_of` = floating_timestamp; alle übrigen Felder = text.

**Verifizierte Schlüsselstruktur:** `link_id` und `transcom_id` sind
identisch und referenzieren dieselbe stabile TRANSCOM-Segment-Kennung
(Beispiel: `4362250`/`4362250`, Cross Island Parkway nordbound bei
Willets Point Blvd, Queens). `id` ist **kein** eindeutiger
Record-Identifier — am Bulk-Export bestätigt: `id` hat exakt so viele
distinkte Werte wie `link_id` und korrespondiert konstant mit genau
einer `link_id` über hunderte Zeitstempel hinweg. **Es gibt kein Feld,
das eine einzelne Messung eindeutig identifiziert** — der einzige
verlässliche Schlüssel ist die Kombination `link_id` + `data_as_of`.
→ **Baseline- und Partitionierungsschlüssel: `link_id`. Avro-Schema
(SCRUM-74) braucht den zusammengesetzten Schlüssel `link_id` +
`data_as_of` für Kafka-Message-Key und Deduplizierung.**

**Weitere Feldeigenschaften:**
- `speed`/`travel_time` sind trotz `text`-Typisierung sauber numerisch
  formatiert (Dezimalpunkt, keine Einheiten im String) — einfache
  Konvertierung im Job.
- `link_points` enthält eine vollständige Polylinie (Lat/Lon-Punktfolge)
  je Segment — Segmentgeometrie direkt nutzbar für die Kartenanzeige in
  SCRUM-90, keine separate Geometriequelle nötig.
- `owner` = `"NYC-DOT-Region 10"` (bislang einziger beobachteter Wert).

### Cache-Verhalten

Response-Header zeigen mitunter `X-SODA2-Data-Out-Of-Date: true` mit
einem `Truth-Last-Modified`-Zeitstempel bis zu ~3 Stunden vor der
tatsächlichen Antwortzeit. **Konsequenz:** Die reale Aktualisierungs-
frequenz des Feeds kann von der Sensor-Meldefrequenz abweichen — bei der
Poll-Intervall-Wahl (SCRUM-81) berücksichtigen, nicht ungeprüft von der
Meldefrequenz ableiten.

### Kennzahlen (Stand 01.09.2026, 24h-Fenster)

| Kennzahl | Wert |
|---|---|
| Aktive Sensoren (`link_id`) | **125** |
| Records/Tag (alle Status) | **23.394** |
| Abgeleitete Meldefrequenz je Sensor | 23.394 ÷ 125 ≈ **187/Tag** ≈ alle 7,7 Min. |
| Bytes/Record (roh, JSON) | **~624** (Stichprobe, 100 Records gemittelt) |
| Abgeleiteter Durchsatz | 23.394 × 624 Byte ≈ **14,6 MB/Tag** (nur DOT-Feed, roh) |
| Kumulierte Records gesamt | 58,8 Mio. (Stand 09.03.2022, öffentliche Metadaten) |
| Datensatz-Beginn | 17.04.2017 |

Hinweis zur Methodik: `count(distinct link_id)` bzw. `count(*)` ohne
Zeitfilter über die komplette Historie läuft in einen Timeout
(Full-Table-Scan über ~59 Mio. Zeilen, keine Fehlermeldung, einfach
keine Antwort). Alle Kennzahlen-Queries daher auf ein 24h-Zeitfenster
eingegrenzt — für Baseline-Dichte ist das ohnehin die richtige
Bezugsgröße (aktuell aktive Sensoren), nicht die historische Gesamtzahl.

### Status-Feld — Sentinel-Wert verifiziert

Verteilung im 24h-Fenster:

| `status` | count | Anteil |
|---|---:|---:|
| `-101` | 12.755 | ~49 % |
| `0` | 13.389 | ~51 % |

`status=-101` geht in ~84 % der Fälle (10.704 von 12.755) exakt mit
`speed=0` und `travel_time=0` einher — ein Sentinel-Triplett für „keine
gültige Messung", keine reale Nullgeschwindigkeit. `status=0` streut über
plausible, realistische Geschwindigkeitswerte und ist der gültige
Datenpunkt.

**Veracity-Argument (belastbar, selbst gemessen):** Rund die Hälfte
aller Rohmeldungen im DOT-Feed ist keine gültige Geschwindigkeitsmessung.
**Harte Konsequenz für die Implementierung:** Der Streaming-Job
(SCRUM-77/-82) muss `status = 0` als Filterbedingung vor jeder
Aggregation anwenden — sonst verzerren Nullwerte in ~50 % der Records
Baseline und Congestion-Score massiv.

### Baseline-Dichte

```
Zellen je Segment = 168 (7 Wochentage × 24 Stunden)
Records/Jahr (hochgerechnet aus 24h-Wert) = 23.394 × 365 ≈ 8.538.810
Messungen je Zelle (roh) = 8.538.810 ÷ 125 link_id ÷ 168 ≈ 407
Messungen je Zelle (nach Sentinel-Filter, ~halbiert) ≈ 200
```

**Entscheidung:** Baseline wird nach dem feinen Raster **Wochentag ×
Stunde** (168 Zellen) berechnet — auch nach Abzug der Sentinel-Records
liegt die Dichte deutlich über der kritischen Schwelle, kein gröberes
Binning nötig. **Vorgabe an SCRUM-83:** Baseline-Schlüssel = `link_id` ×
Wochentag × Stunde, vorberechnet aus der Historie und als Broadcast
geladen (nicht im Streaming-State angelernt).

⚠️ Die Jahreshochrechnung nimmt konstante Sensoraktivität und
Meldefrequenz übers Jahr an (Full-Table-Scan nicht möglich, siehe oben).
Für den Prototyp vertretbar, im Bericht als Annahme zu kennzeichnen,
nicht als gemessenen Wert.

### Baseline-Historie: Strategiewechsel (SCRUM-83)

**Ursprünglicher Plan (verworfen):** Ein separates Pagination-Skript
(`fetch_dot_baseline.py`) sollte einmalig 6 Wochen Historie (21.07.–31.08.2026,
940.234 Zeilen, 94 von 125 Sensoren) per Bulk-Export ziehen. Diese Datei
(`dot_baseline_full.parquet`) ist nicht mehr auffindbar — weder im Repo noch
auf einer der Cluster-Instanzen.

**Zweiter Strategiewechsel (11.09.2026):** Reiner Live-Aufbau der Baseline
(vorheriger Absatz) ist korrekt, aber langsam — volle Wochentag×Stunde-
Abdeckung braucht eine volle Woche Laufzeit. Für eine heute vorführbare
Anwendung reicht das nicht. `src/processing/backfill_silver.py` lädt daher
einmalig echte historische Messungen (ab 21.07.2026) direkt von Socrata in
`s3a://bronze/traffic_speeds_valid` nach,  bewusst am Streaming-Pfad vorbei,
kein Dauerbetrieb, kein CronJob. Danach liefert `compute_baseline.py` sofort
Zellen für praktisch jede Wochentag×Stunde-Kombination. Echte Daten aus
derselben geprüften Quelle (Abschnitt 1), nur einmalig per Batch statt über
Kafka nachgezogen — die einzige Abweichung vom Kappa-Prinzip in diesem
Projekt, hier bewusst und dokumentiert in Kauf genommen.

Der Live-Poller (`producer-live`) und der synthetische Producer
(`producer-synthetic`) laufen unverändert weiter und befüllen dieselbe
Silver-Schicht kontinuierlich über den echten Streaming-Pfad — der Backfill
ersetzt das nicht, er überbrückt nur die Anlaufzeit.

Der synthetische Producer wurde nicht zufällig mit einem Tagesgang gebaut
(`HOURLY_FACTOR` in `src/ingestion/synthetic.py`) — genau damit die Baseline
auch dann nicht flach ist, wenn noch nicht wochenlang echte Live-Daten
vorliegen.

**Bewusste, offen benannte Grenze:** Solange das System nicht mindestens eine
volle Woche durchgehend läuft, fehlen einzelne Wochentage in der Baseline —
betroffene `link_id × Wochentag × Stunde`-Zellen tauchen einfach nicht in
`baseline_profile` auf. Der Join in `streaming_job_bsg.py` behandelt das
bereits als `has_baseline=false`, kein Sonderfall nötig. Zellen mit weniger als
`MIN_SAMPLES_PER_CELL = 5` Messungen werden ebenfalls nicht geschrieben, weil
eine Standardabweichung aus zu wenigen Werten nicht aussagekräftig ist.

**Ablage:** `s3a://gold/baseline_profile` (Delta), MinIO, siehe SCRUM-78.

### Segmentgeometrie im Seed (SCRUM-90)

**Abgerufen:** 10.09.2026 aus `i4gi-tjb9`, Feld `link_points`, für alle 125
`link_id` des Seeds vollständig vorhanden.

**Vorgenommene Änderung an den Rohdaten** (Angabepflicht nach Local Law 11,
§ 23-502 d): `data/dot_links_seed.json` wurde um das Feld `link_points`
ergänzt. Die Koordinaten wurden dabei

* auf sechs Nachkommastellen gerundet (rund 0,1 m Auflösung, mehr gibt der
  Feed nicht her),
* auf den Bereich 40,3–41,1 N / −74,4–−73,5 E gefiltert; vereinzelte
  Ausreißer und abgeschnittene Koordinatenpaare des Feeds hätten die Karte
  sonst unlesbar aufgezogen,
* verworfen, wo weniger als zwei gültige Punkte übrig blieben (ein einzelner
  Punkt ist keine Strecke).

Reproduzierbar über `src/ingestion/enrich_seed_geometry.py`. Die Geometrie ist
eine Stammdatenangabe und wird bewusst nicht bei jedem Start live abgerufen —
das wäre eine Fremdabhängigkeit ohne Gegenwert.

**Warum im Seed und nicht in der Pipeline:** Der Gold-Sink gruppiert je Fenster
und Segment und aggregiert `link_points` dabei weg. Die Serving-API liest die
Geometrie deshalb aus dem Seed (`readers.segments_from`); der Streaming-Job
bleibt unangetastet.

---

## 2. Open-Meteo (Forecast + Archive)

| | |
|---|---|
| Endpunkt Live | `https://api.open-meteo.com/v1/forecast` |
| Endpunkt Historie | `https://archive-api.open-meteo.com/v1/archive` |
| Zugriff | Kein API-Key, kein Sign-up |
| Lizenz | CC BY 4.0 — Attribution **verpflichtend** (Quelle, Lizenzlink, Änderungshinweis) |
| Nutzungsbedingung | Kostenlos nur für nicht-kommerzielle Nutzung, max. 10.000 Aufrufe/Tag; Bildung und öffentlich finanzierte Forschung ausdrücklich als nicht-kommerziell genannt — Studienarbeit fällt darunter |
| Gewährleistung | Ausdrücklich ausgeschlossen („as is") |

### Kennzahlen

Testabruf über alle 5 Boroughs, identischer Zeitraum wie der
DOT-Bulk-Export (21.07.–31.08.2026), zusammengeführt in
`weather_baseline.parquet`:

| Kennzahl | Wert |
|---|---|
| Zeilen gesamt | 5.040 (5 Boroughs × 1.008 Stunden) |
| Zeitraum je Borough | 21.07.2026 00:00 – 31.08.2026 23:00, lückenlos |
| Variablen | `temperature_2m` (°C), `precipitation`/`rain` (mm), `snowfall` (cm), `wind_speed_10m` (km/h) |
| NaN / Duplikate | keine |
| Temperatur-Wertebereich | 16,6 – 35,9 °C (plausibel, NYC Hochsommer) |
| Niederschlag | bis 16 mm/h, kein Schnee im Zeitraum (erwartungsgemäß) |

`precipitation` und `rain` sind in diesem (schneefreien) Zeitraum
identisch — `precipitation` reicht als alleinige Variable für den
Enrichment-Join in SCRUM-84.

**Einschränkung:** Abgefragte Koordinaten werden vom Wettermodell auf den
nächstgelegenen Gitterpunkt gerundet (Modellauflösung 1–11 km). Bei
schmalen Bezirken wie Manhattan (angefragt −73.9712, geliefert −74.0199,
≈5 km Differenz) kann eine Gitterzelle Wetterdaten eines Nachbarbezirks
liefern. Für den Prototyp akzeptiert, als Einschränkung in Abschnitt 12
zu nennen.

**Poll-Intervall (Entscheidung):** 5 Minuten × 5 Boroughs = 1.440
Aufrufe/Tag — deutlich unter dem 10.000er-Limit, bewusst nah an der
DOT-Meldefrequenz (~7,7 Min.) gewählt, da eine feinere Wetterauflösung
keinen Mehrwert für den Join bringt. Wert geht als Vorgabe an SCRUM-81.

**Attributionstext** (Dashboard-Footer und README 1.3):
> Wetterdaten: [Open-Meteo](https://open-meteo.com/), lizenziert unter
> [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

---

## 3. Eigener Producer (synthetischer Verkehrsdatenstrom)

| | |
|---|---|
| Rolle | Lastquelle für Skalierungsnachweis (SCRUM-93), Demo-Betrieb ohne Abhängigkeit von externer API-Verfügbarkeit |
| Verwendet | Echte `link_id`-Werte aus Quelle 1 → gilt als abgeleitetes Werk, Angabepflicht nach Local Law 11 gilt entsprechend |
| Lizenz | Keine externe — eigener Code |

---

## 4. Geprüft und bewusst nicht verwendet

| Quelle | Grund |
|---|---|
| TLC Trip Records | Schlüssel (Taxi-Zone statt Straßensegment) und Messgröße (Distanz/Dauer statt Geschwindigkeit) passen nicht zur Baseline-Anforderung aus Abschnitt 1.1 |
| MTA Bus Time | Busse ≠ Straßensegmente; Developer-Key nötig; Logos/Marken nicht mitlizenziert |
| 511NY Events | Ereignismeldungen, keine Geschwindigkeitsmessungen |

---

## Offene Anschlusspunkte für spätere Sprints

- [ ] An SCRUM-74: Avro-Schema mit zusammengesetztem Schlüssel
      `link_id` + `data_as_of` umsetzen
- [ ] An SCRUM-78: Entscheidung *single* vs. *distributed mode* für
      MinIO vorziehen (horizontale Skalierung nur im distributed mode)
- [ ] An SCRUM-81: Poll-Intervalle (Wetter 5 Min.), App-Token als
      Secret, Cache-Staleness-Verhalten berücksichtigen
- [ ] An SCRUM-83: Baseline-Schlüssel und Broadcast-Ladevorgang
      umsetzen; Sonderbehandlung für Segmente ohne Baseline-Historie
- [x] An SCRUM-90: Open-Meteo-Attribution im Dashboard-Footer umsetzen
      (erledigt 10.09.2026, Footer beider UI-Seiten)
- [ ] Baseline-Parquet-Dateien nach MinIO verschieben, sobald SCRUM-78
      steht; Pfad hier nachtragen
- [ ] Cache-Staleness über mehrere Abrufe hinweg beobachten, um reale
      Update-Frequenz statt einmaligen Cache-Timestamp zu ermitteln