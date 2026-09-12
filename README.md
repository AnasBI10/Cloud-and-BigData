# NYC Congestion Watch

*Datengetriebener Stauerkennungsdienst auf Kubernetes*

## 1. Use Case und Motivation

### 1.1 Problemstellung

Die Verkehrsleitzentrale des NYC DOT steuert den Stadtverkehr anhand von
Sensor- und Kamerafeeds mehrerer Behörden. Die zentrale Schwierigkeit ist nicht
das **Volumen** der Daten, sondern das **Problem der Signalextraktion**: Aus einem
kontinuierlichen Strom von Messungen über hunderte Straßensegmente muss der
Dienst laufend die Handvoll Segmente herausfiltern, bei denen ein Eingriff
lohnt — und zwar in Echtzeit, nicht hinterher.

#### Das Kernproblem: Kontextabhängige Anomalieerkennung

Reine Rohgeschwindigkeiten sind nicht aussagekräftig:
- **Bei Regen werden ALLE Segmente langsamer** — ist das eine Anomalie?
- **Bei Feierabend (17–19 Uhr) sind Verkehrskorridore grundsätzlich überlastet** — ist das ein Stau?

Auffällig ist ein Segment erst dann, wenn es **langsamer ist, als die Umstände
erklären** — also messbar langsamer, als für diese Kombination aus Wochentag,
Uhrzeit und Wetterlage zu erwarten wäre.

#### Was der Dienst beantworten muss

> **Welche Straßensegmente sind gerade anomal langsam (≥ z-Score-Threshold
> über ihrer Baseline) — und seit wann? Priorisiere nach Anomaliegröße und
> Betroffenenzahl der Verkehrsteilnehmer.**

Das System berechnet je Straßensegment und Zeitfenster einen **Congestion-Score**
als standardisierte Abweichung der beobachteten Geschwindigkeit vom erwarteten
Baseline-Profil (korrigiert um Wetterlage) und macht die verbleibenden Ausreißer
auf einer Karte sichtbar.

#### Warum ist das ein Big-Data-Problem?

Das löst kein Query-Tool und keine Excel-Pivot-Table. Hier sind die V5-Kriterien:

| Kriterium | Problem | Lösung |
|-----------|---------|--------|
| **Volume** | Baseline braucht 6–12 Monate × 125 Segmente = ~10 Mio. Zeilen. Eine einzelne CSV-Query schlägt fehl. | Stream-Verarbeitung mit vorberechneter Broadcast-Baseline (nicht im State angelernt). |
| **Velocity** | Meldung nach 30 Min. ist wertlos — Stauwarnung verfällt mit ihrem Alter. "Alte Daten sind keine schlechten Daten, sie sind gar keine Daten." | Live-Stream-Verarbeitung: Event → Filter → Join → Score → Dashboard in < 5 Min. |
| **Variety** | Zwei Ströme mit ungleicher Frequenz (Verkehr ~7,7 Min. je Segment, Wetter ~5 Min. je Borough) und Granularität (125 Segmente vs. 5 Boroughs) erfordern explizites Enrichment-Design. | Stream-Static-Join: Verkehr gegen letzten Wetterstand, nicht beidseitige Watermarks. |
| **Veracity** | 49 % der Rohmeldungen sind Sentinel-Werte (status=-101, speed=0). Dieser Filter muss überall greifen — Live und Reprocessing identisch. | Kappa-Architektur: ein Codepfad, eine Filterregel, überall angewendet. |
| **Value** | Rohdaten über 125 Segmente unsortiert = Information Overload. Nur der Score zeigt, wo Eingriff lohnt. | Aggregation + Ranking + Visualisierung: Top-5-Anomalien auf Karte, nicht 125 Zahlenwerte. |

---

### 1.2 Datenquellen

| Quelle | Rolle | Zugriff | Format |
|---|---|---|---|
| NYC DOT Traffic Speeds NBE (`i4gi-tjb9`) | Leitquelle: Live-Strom **und** Baseline-Historie | Socrata SODA, App-Token | JSON |
| Open-Meteo Forecast + Archive | Zweiter Strom: Wetter je Borough, live und historisch | `api.open-meteo.com`, kein Key | JSON |
| Eigener Producer | Lastquelle für den Skalierungsnachweis, echte Link-IDs | intern | Avro |

Die Leitquelle erfüllt eine kritische Bedingung: **Historie und Live-Strom sind
derselbe Datensatz** — mit derselben `link_id` und derselben Geschwindigkeitsdefinition.
Der Baseline-Vergleich braucht deshalb keine Schlüsselübersetzung und keine
separate Geometriequelle (Segmentpolylinien sind im DOT-Feed enthalten).

#### Warum nicht die TLC Trip Records? (Bewusste Nicht-Verwendung)

Die TLC Taxi-Daten locken mit ~1,5 Mrd. historischen Zeilen — eine eindrucksvolle
Zahl für ein Volume-Argument. Aber sie passen nicht:

| Anforderung | NYC DOT Traffic Speeds | TLC Trip Records | Bewertung |
|---|---|---|---|
| Messgröße | Geschwindigkeit je Straßensegment (km/h) | Fahrzeit pro Taxi-Zone (Minuten) | Nicht vergleichbar |
| Raum-Schlüssel | link_id (TRANSCOM Segment, ~150–250 m Länge) | Taxi-Zone-ID (Quartiere) | Zu grobkörnig, falsche Aggregation |
| Vollständigkeit | 24/7 Live-Feed seit 17.04.2017 | Nur Trip-Records (wenn Fahrt endet) | Messereignisse unterschiedlich |
| Baseline-Gleichheit | Historische Geschwindigkeit = Live-Geschwindigkeit | Ableitung aus Trip-Zeiten ≠ direkte Messung | Apfel-Birnen-Vergleich |

**Entscheidung:** NYC DOT nutzen. Die Größe anderer Datasets ist kein ehrliches Argument,
wenn sie nicht auf die Problemstellung passen.

---

### 1.3 Lizenz

**NYC Open Data:** Nach Local Law 11 von 2012 (§ 23-502 d) ohne Registrierungs-,
Lizenz- oder Nutzungsbeschränkung verfügbar. Pflicht: Angabe von Quelle, Version und
Änderungen bei Weiterveröffentlichung. Da wir Daten in der Abgabe mitliefern und unser
Producer echte Link-IDs nutzt, ist die Angabepflicht aktiv — siehe `DATA_SOURCES.md`.

**Open-Meteo:** CC BY 4.0 — Attribution ist Lizenzbedingung, nicht Höflichkeit.
Der Hinweis steht im Dashboard-Footer und in `DATA_SOURCES.md`.

**Gewährleistung:** Ausdrücklich ausgeschlossen in beiden Fällen (Local Law 11 § 23-504
bzw. Open-Meteo-AGB). Das fließt als Veracity in Abschnitt 2 ein.

**Personenbezogene Daten:** Keine.

---

## 2. Datencharakteristik

Alle Zahlen in diesem Abschnitt stammen aus eigenen Messungen am 01./02.09.2026
gegen den DOT-Feed und die Open-Meteo-API, nicht aus Fremdquellen oder Schätzung.
Details und Rohabfragen stehen in [`DATA_SOURCES.md`](./DATA_SOURCES.md).

### Volume

| Kennzahl | Wert | Herkunft |
|---|---|---|
| Aktive Sensoren (`link_id`), aktuell | 125 | Live-Abfrage, 24h-Fenster |
| Records/Tag, aktuell (alle Status) | 23.394 | Live-Abfrage, 24h-Fenster |
| Bytes/Record (roh, JSON) | ~624 | Stichprobe, 100 Records gemittelt |
| Durchsatz, aktuell | ~14,6 MB/Tag (nur DOT-Feed, roh) | 23.394 × 624 Byte |
| Baseline-Datensatz (Historie) | 940.234 Zeilen, 94 Sensoren, 42 Tage | eigener Bulk-Export, 21.07.–31.08.2026 |
| Wetter-Baseline | 5.040 Zeilen (5 Boroughs × 1.008 Stunden) | eigener Bulk-Export, identischer Zeitraum |

Der DOT-Datensatz selbst läuft seit dem 17.04.2017 und hatte am 09.03.2022 rund
58,8 Mio. Records kumuliert (öffentliche Metadaten) — die Größenordnung, in der
sich sechs Wochen unserer eigenen Baseline-Daten bewegen, ist damit ein kleiner,
aber selbst erhobener und nachprüfbarer Ausschnitt aus einem deutlich größeren,
produktiv laufenden System.

**Bewusst nicht verwendet für das Volume-Argument:** die TLC Trip Records mit ihren
rund 1,5 Mrd. historischen Zeilen. Die Zahl wäre eindrucksvoller, bezieht sich aber
auf eine Quelle, die wir aus Schlüssel- und Messgrößengründen nicht nutzen (siehe
Abschnitt 1.2) — eine fremde Zahl über nicht genutzte Daten wäre kein ehrliches Argument.

### Velocity

| Kennzahl | Wert |
|---|---|
| Meldefrequenz je Sensor, DOT-Feed | ~alle 7,7 Minuten (23.394 ÷ 125) |
| Poll-Intervall Wetter | 5 Minuten je Borough |
| Cache-Staleness DOT-Feed | beobachtet bis zu ~3 Stunden zwischen Antwortzeit und `Truth-Last-Modified` |
| Ziel-Latenz Ende-zu-Ende (Event → Dashboard) | [zu messen, sobald SCRUM-77 läuft] |

Der Cache-Staleness-Befund ist praktisch relevant, nicht nur akademisch: Der
SODA2-Endpunkt liefert laut Response-Header (`X-SODA2-Data-Out-Of-Date: true`)
mitunter Daten aus einem Cache, dessen Stand mehrere Stunden hinter der
tatsächlichen Abrufzeit liegt. Für den Live-Poller heißt das: Die reale
Aktualisierungsfrequenz kann von der Sensorfrequenz abweichen, und ein zu
aggressives Poll-Intervall würde wiederholt denselben Cache-Stand abfragen,
ohne neue Daten zu bekommen.

### Variety

Fünf Formate in einer Pipeline:

1. **JSON** — Socrata-API und Open-Meteo (semi-strukturiert)
2. **Avro** — Events im Kafka-Topic, versioniert über die Schema-Registry
3. **Parquet/Delta** — Lake-Schichten und die selbst gezogenen Baseline-Exporte
4. **CSV** — falls eine Zonentabelle zur Anreicherung ergänzt wird
5. **GeoJSON-ähnliche Polylinien** — `link_points` im DOT-Feed liefert die
   Segmentgeometrie direkt mit, keine separate Geometriequelle nötig

Dazu zwei unabhängige Live-Ströme mit unterschiedlicher Frequenz und Granularität:
Verkehr alle ~7,7 Minuten je Segment, Wetter alle 5 Minuten je Borough (5 Boroughs
stehen 125 Segmenten gegenüber). Genau diese Ungleichheit macht den Enrichment-Join
in SCRUM-84 nicht-trivial — ein Stream-Static-Join gegen den jeweils letzten
Wetterstand ist die passende Lösung, kein Stream-Stream-Join mit beidseitigen Watermarks.

**Konkretes Schema-Drift-Beispiel:** Sollte das Event-Schema im Verlauf von SCRUM-74
erweitert werden (etwa um ein zusätzliches Segment-Attribut), dokumentieren wir
diesen Vorgang als eigenes, selbst erzeugtes Drift-Beispiel für Schema Evolution —
näher an der eigenen Pipeline als ein Verweis auf die `cbd_congestion_fee`-Spalte
der (nicht genutzten) TLC-Daten.

### Veracity

Dies ist der am stärksten durch eigene Messung belegte Abschnitt:

- **Rund 49 % aller Rohmeldungen** des DOT-Feeds im 24h-Fenster tragen
  `status=-101` — ein Sentinel-Wert, der praktisch immer mit `speed=0`/`travel_time=0`
  einhergeht (in der Stichprobe: 10.704 von 12.755 `-101`-Records exakt in dieser
  Kombination). Das ist kein Rauschen, sondern ein Regelfall: Nahezu die Hälfte
  aller Meldungen ist keine gültige Geschwindigkeitsmessung. Der Streaming-Job
  filtert `status != 0` konsequent heraus, bevor irgendeine Aggregation läuft.
- **Baseline-Abdeckung ist unvollständig:** Von 125 aktuell aktiven Sensoren finden
  sich nur 94 im sechswöchigen Baseline-Zeitraum wieder. Die verbleibenden ~31
  Sensoren haben keine ausreichende Historie — vermutlich, weil sie erst nach dem
  Exportzeitraum aktiv wurden.
- **Cache-Staleness** (siehe Velocity) bedeutet, dass ein einzelner Abruf nicht
  garantiert den aktuellsten Sensorstand zeigt.
- Die Stadt und Open-Meteo schließen Gewährleistung zu Vollständigkeit und
  Richtigkeit ausdrücklich aus (Local Law 11 § 23-504 bzw. Open-Meteo-Nutzungsbedingungen).

Diese vier Punkte zusammen ergeben ein Veracity-Argument, das nicht auf einer
Annahme beruht, sondern auf eigener Prüfung — mit einer konkreten Konsequenz für
die Implementierung (Statusfilter vor jeder Aggregation) und einer offen benannten
Grenze (Baseline-Lücke, siehe Abschnitt 12).

### Value

Der Congestion-Score je Segment und Zeitfenster ist die Entscheidungsgrundlage
für die Verkehrsleitzentrale aus Abschnitt 1.1: Er zeigt an, wo ein Eingriff
wahrscheinlich lohnt, statt Rohdaten über 125 Segmente unsortiert bereitzustellen.

---

## 3. Architekturentscheidung

### 3.1 Kappa statt Lambda

Wir setzen die in der Vorlesung als Standard vorgesehene **Kappa-Architektur** um.
Die Begründung folgt aus dem konkreten Datenverhalten, das wir in Abschnitt 2
gemessen haben, nicht aus der bloßen Vorlesungsvorgabe.

**Ein Codepfad für eine Filterregel, die überall gelten muss.** Rund die Hälfte aller
Rohmeldungen im DOT-Feed ist `status=-101` und damit ungültig (siehe Abschnitt 2, Veracity).
Diese Filterregel muss exakt gleich greifen, egal ob ein Record gerade live über
Kafka hereinkommt oder ob wir Monate später dieselben Daten aus dem Lake neu verarbeiten.
Bei Lambda müssten Speed-Layer und Batch-Layer dieselbe Regel unabhängig voneinander
implementieren — ein Ort, an dem sich zwei Implementierungen schleichend unterscheiden
können, obwohl beide „dieselbe" Logik meinen. Bei Kappa gibt es nur einen Ort, an dem
der Filter steht.

**Historie ist bei uns nur ein langsamer Stream.** Der Bulk-Export, den wir für die
Baseline gezogen haben (`DATA_SOURCES.md`, Abschnitt A5), läuft über dieselbe
Spark-Structured-Streaming-Anwendung wie der Live-Betrieb — nur über eine File-Source
statt Kafka, mit identischem Statusfilter und identischer Aggregationslogik. Reprocessing
heißt bei uns: Checkpoint zurücksetzen oder Delta-Version wählen, denselben Job erneut
laufen lassen. Ein separater Batch-Layer würde diese Arbeit duplizieren, ohne einen
Vorteil zu bringen, den wir tatsächlich brauchen.

**Kein natürlicher Einzelschlüssel verlangt nach Idempotenz statt nach zwei Layern.**
Wie in `DATA_SOURCES.md` dokumentiert, hat keine einzelne DOT-Messung eine eindeutige
Record-ID — das Feld `id` ist redundant zu `link_id`. Der einzige verlässliche Schlüssel
ist die Kombination `link_id` + `data_as_of`. Genau darauf ist Delta Lake ausgelegt:
ACID-Commits und ein zusammengesetzter Schlüssel machen Exactly-once-Semantik über
einen einzigen Schreibpfad möglich (siehe SCRUM-95), ohne dass ein zweiter Layer zur
Konsistenzsicherung nötig wäre.

**Betriebsaufwand.** Auf einem Studienprojekt-Cluster ist jede zusätzliche Komponente
ein Deployment, ein PVC und eine potenzielle Fehlerquelle mehr. Ein Batch-Layer würde
Punkte im Kriterium „Kubernetes-Deployment" kosten, ohne fachlich etwas beizutragen,
das wir nicht ohnehin schon abdecken.

**Wo Lambda die bessere Wahl gewesen wäre:** Für abrechnungsrelevante Zahlen zur
Innenstadtmaut — also exakte, revisionssichere Beträge statt Trendaussagen — wäre
ein separater, nachts laufender Batch-Layer mit vollständiger Neuberechnung das
robustere Modell, weil dort Nachvollziehbarkeit einzelner Korrektionen wichtiger
ist als Aktualität. Diese Anforderung liegt bewusst außerhalb unseres Scopes (Abschnitt 12).

### 3.2 Architekturdiagramm

![Kappa-Architektur](abb/architektur-final.svg)

Web-UI und Wetter-Poller schreiben nach Kafka, abgesichert durch eine Schema-Registry
mit Avro-Verträgen. Spark Structured Streaming wendet den Statusfilter an, joint gegen
den Wetterstrom und aggregiert in Zeitfenstern. Ergebnisse landen als Delta Lake auf
MinIO und werden über eine Serving-API an die UI zurückgegeben.

### 3.3 Bewusste Abweichungen vom Vorlesungsstand

| Vorlesung | Unsere Wahl | Begründung |
|---|---|---|
| HDFS | **MinIO (S3-kompatibel)** | HDFS ist auf Data Locality ausgelegt. Auf Kubernetes ist Compute ohnehin von Storage getrennt, der NameNode wäre ein Single Point of Failure und ein zusätzlicher Single Point of Failure. MinIO ist S3-kompatibel und skaliert horizontal. |
| Reines Parquet | **Delta Lake** | ACID-Commits verhindern, dass die Serving-Schicht halbgeschriebene Dateien liest. Zusätzlich Schema Evolution, falls das Event-Schema im Verlauf erweitert wird. |
| JSON auf dem Bus | **Avro + Schema-Registry** | Ein durchsetzbarer Schema-Vertrag zwischen Producer und Consumer. Die Registry prüft Kompatibilität beim Registrieren, nicht erst beim Absturz eines Consumer. |
| Einzelner Datenstrom | **Zwei unabhängige Ströme mit Join** | Verkehr allein erklärt keine Auffälligkeit (siehe Abschnitt 1.1: Bei Regen wird alles langsamer). Der Wetterstrom macht aus der bloßen Verkehrsmessung ein kontextabhängiges Signal. |

### 3.4 Neue Infrastrukturanforderungen aus der Architekturentscheidung

Zwei Konsequenzen aus 3.1 und 3.3, die zum Zeitpunkt der ursprünglichen Backlog-Planung
noch nicht sichtbar waren und als Tickets nachgezogen werden müssen:

- **Spark muss auf Kubernetes deployt werden**, nicht nur lokal laufen (SCRUM-77 deckt
  aktuell nur `Kafka → Console`). Nötig: Entscheidung zwischen Spark-Operator (CRD
  `SparkApplication`) und Driver-Deployment mit `spark-submit --master k8s://`, dazu
  RBAC für den Driver (Executor-Pods erzeugen) und ein PVC für den Checkpoint.
- **Der Wetter-Poller existiert noch nicht als Ticket.** SCRUM-84 (Join mit Wetterstrom)
  setzt ihn voraus, aber kein Sprint-1-Ticket erzeugt ihn. Nötig: Deployment, das
  Open-Meteo im 5-Minuten-Takt abfragt und nach Kafka schreibt (Poll-Intervall-Begründung
  in `DATA_SOURCES.md`, Abschnitt A6).

Beide gehören vor SCRUM-77/-84 in den Sprint-1-Zeitplan, siehe Backlog-Übersicht.

---

## Offene Punkte

### Abgeschlossen (Sprint 0)

Alle Datenbeschaffung (App-Token, SODA3-Test, Kennzahlen, Baseline-Export, Wetter-Export)
sowie die Abschnitte 1–3 dieser README sind fertig — siehe [`DATA_SOURCES.md`](./DATA_SOURCES.md)
für alle Rohwerte und Herleitungen.

### Noch offen, vor bzw. während Sprint 1 zu klären

- [ ] **Abschnitt 12 fehlt in dieser Datei** — Inhalt (Scope-Grenzen: keine Vorhersage,
      keine Ursachenzuordnung, keine Routenberechnung, keine Abrechnungszahlen,
      Baseline-Lücke bei ~31 Sensoren, Wetterraster-Einschränkung) liegt bereits
      vorformuliert vor, muss nur noch eingefügt werden.
- [ ] **Zwei neue Tickets ins Backlog aufnehmen** (siehe Abschnitt 3.4): Wetter-Poller
      (Sprint 1, blockiert SCRUM-84) und Spark auf Kubernetes deployen (Sprint 1, RBAC + Checkpoint-PVC).
- [ ] **An SCRUM-74 weitergeben:** Avro-Schema braucht den zusammengesetzten Schlüssel
      `link_id` + `data_as_of` — es gibt kein Feld, das eine Einzelmessung eindeutig identifiziert.
- [ ] **An SCRUM-78 weitergeben:** Entscheidung *single* vs. *distributed mode* für MinIO
      vorziehen — horizontale Skalierung ist nur im distributed mode möglich und wird beim
      Deployment festgelegt.
- [ ] **An SCRUM-81 weitergeben:** Poll-Intervall Wetter (5 Min.), Socrata-App-Token als
      Secret, Cache-Staleness-Verhalten beim DOT-Poll-Intervall berücksichtigen.
- [ ] **An SCRUM-83 weitergeben:** Baseline-Schlüssel = `link_id` × Wochentag × Stunde;
      Segmente ohne ausreichende Historie (~31 von 125) laufen ohne Baseline-Vergleich,
      bis genug Live-Daten vorliegen.
- [ ] **An SCRUM-90 weitergeben:** Open-Meteo-Attributionstext im Dashboard-Footer einbauen
      (Lizenzpflicht, kein Nice-to-have) — Text liegt in `DATA_SOURCES.md` vor.
- [ ] Ende-zu-Ende-Latenz messen, sobald SCRUM-77 läuft (aktuell Platzhalter in Abschnitt 2, Velocity).
- [ ] Sprachwahl (Python durchgängig für Ingestion, Processing, Serving) in Abschnitt 4 als
      ein Satz begründen, sobald Abschnitt 4 geschrieben wird.
- [ ] Baseline-Parquet-Dateien nach MinIO verschieben, sobald SCRUM-78 steht; Pfad in
      `DATA_SOURCES.md` nachtragen.


## 4. Komponenten und Datenfluss

## 5. Processing-Logik

## 6. Speicherkonzept

## 7. User-facing UI

Die Aufgabenstellung verlangt eine Oberfläche **mindestens in der Rolle des
Datenlieferanten**, real an die Pipeline angebunden. Umgesetzt sind beide
Rollen: Einspeisung (SCRUM-89) und Anzeige (SCRUM-90), als ein Bundle unter
`src/ui/`, containerisiert und über Traefik exponiert (SCRUM-91).

### 7.1 Rolle Datenlieferant: der Weg ist der Punkt

Entscheidend ist nicht, dass die UI Events erzeugt, sondern **welchen Weg sie
nehmen**. Ein hier erzeugtes Event ist von einer echten DOT-Messung im Topic
nicht zu unterscheiden — nur der Zeitstempel verrät es:

```
Formular / Szenario  (Browser)
        │  POST /api/events
        ▼
Serving-API  ──►  EventPublisher aus src/ingestion/common.py
        │              │ Avro-Serialisierung gegen die Schema-Registry
        │              ▼
        │         Kafka: traffic.speeds.raw
        │              ▼
        │         Spark Structured Streaming (Bronze → Silver → Gold)
        │              ▼
        │         Delta Lake auf MinIO
        │              ▼
        └──────►  Serving-API  ──►  Dashboard
                  GET /api/anomalies, /api/segments, .../timeseries
```

Zwei Festlegungen dahinter:

- **Kein Seiteneingang.** Die UI schreibt nichts direkt in die Gold-Schicht.
  Täte sie es, wäre die Änderung sofort im Dashboard sichtbar — und der
  vorgeführte Datenfluss wäre eine Behauptung statt eines Nachweises.
- **Keine zweite Serialisierung.** `POST /api/events` benutzt denselben
  `EventPublisher` und dieselbe `build_event()` wie die beiden Producer
  (`src/ingestion/common.py`). Eine eigene Avro-Logik in der API hätte
  bedeutet, dieselbe Definition eines Events an zwei Stellen zu pflegen; die
  erste Abweichung hätte niemand bemerkt.

Die API bleibt damit *lesend plus einspeisend*, aber sie kennt die Gold-Schicht
weiterhin nur lesend.

### 7.2 Bedienablauf Einspeisung

**Einzelnes Event.** Segment aus den 125 bekannten `link_id` wählen,
Geschwindigkeit setzen, Status wählen. Der Status ist bewusst bedienbar: `0`
ist eine gültige Messung, `-101` der Sentinel des echten Feeds. Wer `-101`
sendet, sieht im Dashboard, dass für dieses Segment **kein** neues Fenster
entsteht — der Statusfilter des Spark-Jobs (Abschnitt 2, Veracity) wird damit
vorführbar, statt nur behauptet zu sein.

**Szenario-Generator.** Ein einzelnes Event verschiebt einen Mittelwert über
ein 5-Minuten-Fenster kaum sichtbar; im Dashboard passiert dann scheinbar
nichts, obwohl die Kette funktioniert. Deshalb erzeugt der Generator eine Folge
von Events über X Minuten, in drei Verläufen:

| Verlauf | Was passiert | Was es zeigt |
|---|---|---|
| `congestion` | Geschwindigkeit fällt auf 30 % des Ausgangswerts | Segment wandert in die Anomalie-Rangliste, Karte färbt um |
| `recovery` | Gegenstück, zurück auf den Ausgangswert | Score fällt, Segment verlässt die Rangliste |
| `sensor_outage` | Serie mit `status=-101` | Statusfilter greift, Segment bekommt keine neuen Fenster |

Zwei Eigenschaften der Pipeline bestimmen dabei das Verhalten der UI:

- **Der Lauf ist echtzeitgebunden, nicht rückdatiert.** Der Streaming-Job
  markiert jedes Event, dessen `data_as_of` älter ist als die Watermark
  (2 Minuten), als verspätet, schließt es aus der Aggregation aus und schickt
  es in die DLQ. Ein Szenario, das seine Zeitstempel über die letzte
  Viertelstunde verteilt auf einen Schlag sendet, käme im Dashboard nie an.
- **Verspätung ist trotzdem vorführbar.** Wer beim Einzelevent bewusst
  zurückdatiert, bekommt eine Ablehnung mit Begründung — oder setzt
  `allow_late`, dann geht das Event absichtlich den DLQ-Weg aus SCRUM-85 und
  die Quittung sagt das auch.

Als Ausgangsgeschwindigkeit nimmt der Generator die **Baseline des gewählten
Segments** für die aktuelle Stunde, nicht einen Pauschalwert. Ein fester
Startwert hätte auf einem ohnehin langsamen Segment eine Beschleunigung
erzeugt statt eines Staus.

### 7.3 Bedienablauf Anzeige

Das Dashboard konsumiert **ausschließlich** die Serving-API — kein direkter
Zugriff auf Kafka, Delta oder MinIO aus dem Browser. Was dort steht, hat die
Pipeline durchlaufen.

- **Karte** aller 125 Segmente, gezeichnet aus `link_points` des DOT-Feeds,
  eingefärbt nach `congestion_score`.
- **Top-Anomalien** aus `/api/anomalies` ab 2 σ, mit Borough-Filter.
- **Zeitreihe** je Segment: gemessene gegen erwartete Geschwindigkeit mit
  1-σ-Band, umschaltbar auf 6/24/72 Stunden.

Zwei Entscheidungen sind inhaltlich, nicht gestalterisch:

**„Unbewertbar" ist kein Wert auf der Skala.** Die Karte trennt drei Zustände,
die sonst in eins fallen: bewertete Segmente (sequenzielle Farbskala), Segmente
ohne aktuelle Messung (grau, dünn) und Segmente ohne Baseline (violett,
gestrichelt). Andere Farbfamilie **und** andere Strichart, damit „wissen wir
nicht" nicht wie „unauffällig" aussieht. Das ist das Veracity-Argument aus
Abschnitt 2 — 31 von 125 Segmenten ohne ausreichende Historie — als
Darstellungsregel. Sie grün zu färben würde eine Aussage behaupten, die die
Daten nicht hergeben. Aus demselben Grund zeigt die Rangliste immer mit an, wie
viele Segmente gemessen, davon bewertbar und davon ohne Baseline sind: eine
Liste, die nur Treffer zeigt, verschweigt die Lücke.

**Lücken bleiben Lücken.** Bei einer Meldefrequenz von ~7,7 Minuten je Sensor
(Abschnitt 2, Velocity) enthält nicht jedes 5-Minuten-Fenster für jedes Segment
einen Wert. Die Zeitreihe bricht die Linie an solchen Stellen ab, statt
durchzuziehen. Eine durchgezogene Linie über eine Stunde ohne Messung wäre eine
Behauptung, die die Daten nicht decken; der Gold-Vertrag hält entsprechend
fest, dass die API nicht interpoliert.

Das Polling-Intervall liegt bei 20 Sekunden und entspricht damit dem
Server-Cache der API (`CACHE_TTL_S`). Häufiger zu fragen liefert dieselbe
Antwort und erzeugt nur Last.

### 7.4 Technische Umsetzung

Reines HTML/CSS/ES-Modules **ohne Build-Schritt**. Das Bundle wird ausgeliefert,
wie es im Repo liegt; das Image besteht aus nginx plus rund 70 kB Dateien und
braucht keine Node-Toolchain im Build.

Auch die Karte ist eigener Code statt einer Kartenbibliothek: 125 Polylinien
mit rund 1.300 Punkten, projiziert per Web-Mercator in ein SVG. Eine Bibliothek
vom CDN wäre eine Fremdabhängigkeit zur Laufzeit, dazu käme die
Kachel-Lizenzierung. So bleibt das Bundle self-contained und funktioniert auch
ohne Internetzugang bei der Vorführung.

Die Segmentgeometrie liegt im Seed (`data/dot_links_seed.json`), einmalig aus
dem DOT-Feed ergänzt über `src/ingestion/enrich_seed_geometry.py`. Der
Gold-Sink aggregiert `link_points` je Fenster zwar mit, liefert es aber nur für
Segmente, die der Live-Poller speist — für die übrigen greift der Seed als
Rückfall. Die vorgenommenen Änderungen an den Rohdaten sind in
`DATA_SOURCES.md` dokumentiert, wie es Local Law 11 für die
Weiterveröffentlichung verlangt (Abschnitt 1.3).

**Konfiguration statt fester Adressen:** Die API-Basis-URL steht nicht im
Bundle. Ein Skript unter `/docker-entrypoint.d/` schreibt `config.js` beim
Containerstart aus `API_BASE_URL`, sodass dasselbe Image lokal gegen
`localhost:8000` und im Cluster gegen den Ingress läuft. Im Cluster bleibt der
Wert leer: UI und API liegen hinter demselben Ingress (`/` bzw. `/api`), also
gleiche Herkunft — der Browser braucht dafür kein CORS.

### 7.5 Grenzen

- **`source` bleibt `SYNTHETIC`.** Das Avro-Enum kennt `DOT_LIVE`, `SYNTHETIC`
  und `REPLAY`. Ein eigenes Symbol `UI` wäre eine nicht abwärtskompatible
  Schemaänderung gewesen; der Nutzen rechtfertigt das Risiko am Vertrag nicht.
  Von Hand erzeugte Events sind damit im Lake nicht von denen des
  Lastgenerators zu unterscheiden.
- **Der Fortschritt eines Szenarios ist pod-lokal.** Er lebt im Prozess, der
  den Lauf fährt. Bei mehreren Repliken kann die Statusabfrage bei einem
  anderen Pod landen; die UI sagt dann, dass der Fortschritt nicht abfragbar
  ist, statt zu raten. Der Lauf selbst läuft weiter. Geteilter Zustand hätte
  einen weiteren zustandsbehafteten Dienst bedeutet — für die Quittung eines
  Knopfdrucks zu teuer.
- **`CORS_ORIGINS` steht auf `*`,** weil der Ingress-Host im Chart nicht
  feststeht. Seit die API POST entgegennimmt, heißt das, dass eine beliebige
  Seite im Browser eines Nutzers Events einspeisen könnte. Für den internen
  Prototyp vertretbar, vor einem echten Betrieb einzugrenzen.
- **Die Wetterfelder sind durchgehend `null`,** solange der Enrichment-Join
  (SCRUM-84b) fehlt. Die Felder bleiben im Modell, damit der Vertrag steht,
  sobald der Join geliefert wird.


## 8. Kubernetes-Deployment

## 9. Deployment-Anleitung

### Voraussetzungen

| Anforderung | Grund |
|---|---|
| Kubernetes-Cluster mit mindestens 3 Nodes | Kafka laeuft mit Replikationsfaktor 3 und drei Broker-Repliken; auf weniger Nodes verliert man echte Ausfalldomaenen. Getestet auf k3s v1.36.4, 1 Control-Plane + 2 Worker. |
| Default-StorageClass vorhanden | Alle PVCs (Kafka, MinIO, Processing-Checkpoints) verzichten bewusst auf eine explizite storageClassName und nutzen die Cluster-Default (local-path bei k3s). Fehlt eine Default-Klasse, bleiben PVCs auf Pending. |
| helm (v3) und kubectl, konfiguriert gegen den Ziel-Cluster | Das gesamte Deployment laeuft ueber ein einziges Helm-Chart (deploy/helm/congestion-watch). |
| docker auf jedem Node, auf dem eigene Images laufen sollen | Das Projekt nutzt keine eigene Container-Registry. Eigene Images (ingestion, processing, serving, ui) werden lokal gebaut und muessen manuell auf jeden Node verteilt werden (siehe Schritt 3). |

Pruefen:

    kubectl get nodes
    kubectl get storageclass

### Schritt 1: Namespace-Konfiguration

Der Namespace ist zentral in deploy/helm/congestion-watch/values.yaml (Zeile 1, namespace: bigdata) hinterlegt, nicht ueber helm install -n. Fuer einen abweichenden Namespace genuegt --set namespace=NAME beim Install.

### Schritt 2: Secrets/Zugangsdaten setzen

Zwei Platzhalter in values.yaml muessen vor dem Deploy ersetzt werden: minio.password (MinIO Root-Passwort) und socrata.appToken (NYC-DOT-API-Token, kostenlos unter data.cityofnewyork.us). Ohne gueltiges Socrata-Token laeuft der live-Poller anonym mit strengerem Rate-Limit weiter.

### Schritt 3: Eigene Docker-Images bauen und auf alle Nodes verteilen

Vier Images werden aus dem Repo-Root gebaut (Build-Kontext bewusst das Repo-Root, da mehrere Dockerfiles auf schemas/ und data/ zugreifen):

    cd bigdata-repo
    docker build -t congestion-watch/ingestion:0.1.0  -f src/ingestion/Dockerfile .
    docker build -t congestion-watch/processing:0.1.0 -f src/processing/Dockerfile .
    docker build -t congestion-watch/serving:0.1.0    -f src/serving/Dockerfile .
    docker build -t congestion-watch/ui:0.1.0          -f src/ui/Dockerfile .

Auf dem Build-Node ins Cluster-Runtime importieren (k3s nutzt containerd, nicht den Docker-Daemon):

    for img in ingestion processing serving ui; do
      docker save congestion-watch/${img}:0.1.0 -o /tmp/${img}.tar
      sudo k3s ctr images import /tmp/${img}.tar
    done

Auf jeden weiteren Node kopieren und dort importieren (kein Node darf ausgelassen werden, sonst ImagePullBackOff sobald der Scheduler dort landet):

    for node in WORKER_IP_1 WORKER_IP_2; do
      scp /tmp/*.tar ubuntu@${node}:/tmp/
      ssh ubuntu@${node} 'for f in /tmp/*.tar; do sudo k3s ctr images import "$f"; done'
    done

Die drei externen Images (quay.io/minio/minio, apache/kafka:3.9.0, confluentinc/cp-schema-registry:7.6.1) werden automatisch von den jeweiligen oeffentlichen Registries gezogen.

### Schritt 4: Image-Tags in values.yaml referenzieren

    producer.image: congestion-watch/ingestion:0.1.0
    processing.image: congestion-watch/processing:0.1.0
    serving.image: congestion-watch/serving:0.1.0
    ui.image: congestion-watch/ui:0.1.0

Bei jeder Codeaenderung neuen Tag vergeben (nicht latest wiederverwenden), sonst zieht imagePullPolicy: IfNotPresent den alten Stand nicht neu.

### Schritt 5: Deployen

    helm install congestion-watch deploy/helm/congestion-watch -n bigdata --create-namespace

Fuer einen abweichenden Namespace zusaetzlich --set namespace=NAME anhaengen. Das Chart legt beim Install/Upgrade automatisch zwei Hook-Jobs an: minio-create-buckets (Bucket-Anlage) und schema-register (Avro-Schema-Registrierung).

### Schritt 6: Verifikation

    kubectl get pods -n bigdata -o wide

Erwartet: alle Pods Running, kafka-0/1/2 ueber verschiedene Nodes verteilt. Erststart kann mehrere Minuten dauern (Kafka-Image ca. 400 MB, Schema-Registry-Image ca. 1,4 GB).

    kubectl logs -n bigdata job/schema-register

Erwartet: eine registrierte Schema-ID je Subject, keine Fehlermeldung.

    kubectl get pods -n bigdata -l app=producer-synthetic
    kubectl logs -n bigdata deploy/producer-synthetic --tail=20

Erwartet: "zugestellt=N fehlgeschlagen=0" in regelmaessigen Abstaenden.

    kubectl port-forward -n bigdata svc/serving-api 8000:80
    curl localhost:8000/health

Erwartet: Status "ready".

### Bekannte Fallstricke

- Kafka podManagementPolicy: Parallel ist zwingend. Bei OrderedReady wartet Kubernetes auf die Readiness von kafka-0, die dieser mangels KRaft-Quorum ohne die anderen beiden Broker nie erreicht (Deadlock).
- Schema-Registry enableServiceLinks: false ist zwingend. Kubernetes injiziert sonst pro Service Umgebungsvariablen, die das cp-Image faelschlich als eigene Konfiguration interpretiert und mit Exit 1 abbricht.
- Live-Poller laeuft als StatefulSet, nicht als Deployment. Der Shard-Index wird aus dem Pod-Ordinal abgeleitet; ein Deployment wuerde allen Repliken denselben Shard zuweisen.
- Keine Registry vorhanden: Bei jedem neuen Image-Tag muss Schritt 3 auf allen Nodes wiederholt werden, sonst ImagePullBackOff sobald der Pod auf einem anderen Node landet.
- Hook-Jobs mit statischem Namen (schema-register, minio-create-buckets) muessen bei einem erneuten helm upgrade ggf. manuell geloescht werden: kubectl delete job schema-register minio-create-buckets -n bigdata.

## 10. Wesentliche Codeabschnitte

Alle Pfade sind relativ zum Repo-Root. Zeilenangaben beziehen sich auf den
Abgabestand; bei Aenderungen bitte mit `grep -n "^def \|^class " <datei>`
neu abgleichen.

### Ingestion

| Datei / Zeilen | Was passiert dort |
|---|---|
| src/ingestion/common.py#L38-L99 | `Settings`-Dataclass (Konfiguration aus ENV/ConfigMap) sowie `load_seed()`/`shard_of()` zum Laden und Sharding der Segment-Stammdaten je Producer-Replik. |
| src/ingestion/common.py#L137-L251 | `build_event()` baut das Event-Dict inkl. `event_key` (Merge-Key fuer Exactly-once), `EventPublisher`-Klasse kapselt Avro-Serialisierung + Kafka-Producer mit `acks=all`/`enable.idempotence=True`. |
| src/ingestion/synthetic.py#L83 | `run()`: Lastgenerator-Modus, erzeugt synthetische Verkehrsereignisse mit konfigurierbarer Rate (`EVENTS_PER_SECOND`). |
| src/ingestion/live_poller.py#L68-L127 | `_fetch()`/`run()`: pollt den echten NYC-DOT-Feed (Socrata-API), gesharded ueber mehrere StatefulSet-Replikas. |
| src/ingestion/weather.py#L69-L163 | `fetch_borough()`/`run()`: Open-Meteo-Poller je Borough, publiziert Wetterereignisse mit `borough` als Kafka-Key (SCRUM-84). |
| src/ingestion/main.py#L23 | Einstiegspunkt, verzweigt nach `MODE` (synthetic/live/weather) in den jeweiligen Producer. |

### Stream Processing

| Datei / Zeilen | Was passiert dort |
|---|---|
| src/processing/streaming_job_bsg.py#L78-L123 | `enrich_with_seed()` (Broadcast-Join gegen Segment-Stammdaten) und `read_weather_stream()` (Wetter-Stream lesen + Spaltenumbenennung zur Kollisionsvermeidung). |
| src/processing/streaming_job_bsg.py#L154-L176 | `append_delta()`: idempotenter Bronze/Silver-Append via `txnAppId`/`txnVersion` (Exactly-once, SCRUM-95). |
| src/processing/streaming_job_bsg.py#L177-L212 | `upsert_gold()`: MERGE der aggregierten Congestion-Scores in die Gold-Tabelle (Windowing, Baseline-Join, Score-Berechnung). |
| src/processing/streaming_job_bsg.py#L213-L302 | `upsert_anomaly_state()`: Stateful Processing -- fuehrt je Segment einen Zaehler aufeinanderfolgender auffaelliger Fenster, bestaetigt Anomalien erst ab einer Mindestanzahl (SCRUM-83). |
| src/processing/streaming_job_bsg.py#L375 | `main()`: verdrahtet die drei Streams (Traffic, Weather, DLQ), Watermarks, Stream-Stream-Join und die drei parallelen `writeStream`-Queries. |
| src/processing/compute_baseline.py#L37 | Batch-Job zur Baseline-Berechnung (Erwartungswert/Stddev je Segment x Wochentag x Stunde), Grundlage fuer den Congestion-Score. |

### Serving (API)

| Datei / Zeilen | Was passiert dort |
|---|---|
| src/serving/readers.py#L279-L422 | `DeltaReader`: liest die Gold-Tabelle direkt ueber `deltalake`/PyArrow (ohne Spark/JVM), Spaltenvertrag ueber `SINK_COLUMNS`. |
| src/serving/readers.py#L200-L268 | `BaselineIndex`: gecachter Zugriff auf die Baseline-Tabelle fuer Referenzgeschwindigkeiten. |
| src/serving/main.py#L96-L124 | `/health`, `/ready`: Liveness/Readiness-Endpunkte, inkl. Pruefung der Gold-Tabellen-Erreichbarkeit. |
| src/serving/main.py#L125-L161 | `/api/anomalies`: liefert Segmente mit Congestion-Score ueber Schwellenwert, sortiert nach Score. |
| src/serving/main.py#L187-L212 | `/api/segments`: aktueller Zustand aller Segmente fuer die Kartenansicht. |
| src/serving/main.py#L213-L296 | `/api/events` (POST): Einspeisung einzelner Events von der UI aus (Datenlieferant-Rolle). |
| src/serving/main.py#L297-L347 | Szenario-Endpunkt: startet vordefinierte Event-Batches (z. B. simulierter Vorfall) zur Demonstration. |

### User-facing UI

| Datei / Zeilen | Was passiert dort |
|---|---|
| src/ui/js/dashboard.js#L96-L162 | `buildMap()`/`addBoroughLabels()`: rendert die Segmentkarte (SVG) mit Congestion-Einfaerbung. |
| src/ui/js/dashboard.js#L184-L283 | Tooltip-Handling und Live-Update-Zyklus (Polling der Serving-API). |
| src/ui/js/dashboard.js#L289-L398 | `renderChart()`: Zeitreihen-Diagramm je ausgewaehltem Segment. |
| src/ui/js/ingest.js#L69-L125 | `wireEventForm()`: UI-Formular zum manuellen Einspeisen von Events (Datenlieferant-Rolle) gegen `/api/events`. |
| src/ui/js/ingest.js#L139-L210 | `wireScenarioForm()`/`followRun()`: Start und Live-Verfolgung eines Szenario-Laufs. |
| src/ui/js/api.js | Zentrale Fetch-Wrapper fuer alle Serving-API-Aufrufe der UI. |

### Kubernetes-Manifeste (Helm-Chart)

| Datei | Was passiert dort |
|---|---|
| deploy/helm/congestion-watch/templates/ingestion.yaml | `Deployment`s fuer synthetic/weather-Producer, `StatefulSet` fuer den live-Poller (Sharding braucht stabile Identitaet), zugehoerige `ConfigMap`/`HPA`. |
| deploy/helm/congestion-watch/templates/kafka.yaml | Kafka-`StatefulSet` (3 Broker, KRaft-Mode), Topic-Init-Job und Schema-Registrierungs-Job (Post-Install/Upgrade-Hook). |
| deploy/helm/congestion-watch/templates/processing.yaml | `Deployment` fuer den Spark-Streaming-Job, `PersistentVolumeClaim` fuer Checkpoints, Env-Konfiguration (Topics, Schema-Pfade, MinIO-Zugang). |
| deploy/helm/congestion-watch/templates/baseline.yaml | `CronJob` fuer die periodische Baseline-Neuberechnung. |
| deploy/helm/congestion-watch/templates/minio.yaml | MinIO-`StatefulSet` (S3-kompatibler Objektspeicher fuer Bronze/Silver/Gold), Bucket-Init-Job. |
| deploy/helm/congestion-watch/templates/serving.yaml | `Deployment` + `Service` + `Ingress` der Serving-API, horizontale Skalierung ueber mehrere Replikas. |
| deploy/helm/congestion-watch/templates/ui.yaml | `Deployment` + `Service` + `Ingress` der containerisierten UI (nginx). |
| deploy/helm/congestion-watch/templates/resourcequota.yaml | `ResourceQuota` fuer den Namespace, begrenzt CPU/Memory je Component fair auf dem geteilten Cluster. |

## 11. Screenshots und Nachweise

## 12. Grenzen des Prototyps und Ausblick

### Was bewusst nicht umgesetzt wurde

- Kafka-DLQ-Sink ist at-least-once, nicht exactly-once. Anders als die Delta-Sinks (Bronze/Silver via txnAppId/txnVersion, Gold und Anomaly-State via MERGE) hat Spark Structured Streaming keine native transaktionale Producer-API fuer Kafka-Sinks. Bei einer Batch-Wiederholung nach einem Absturz koennen doppelte Eintraege in traffic.speeds.dlq entstehen. Bewusst akzeptiert, da die DLQ ein Diagnosepfad ist und keine Downstream-Aggregation auf ihr aufsetzt.

- Keine eigene Container-Registry. Eigene Images werden manuell per docker save/scp/k3s ctr images import auf jeden Node verteilt (siehe Kapitel 9). Das ist fuer drei Nodes noch handhabbar, skaliert aber nicht und ist eine haeufige Fehlerquelle (ImagePullBackOff, wenn ein Node beim Verteilen vergessen wird). Eine private Registry haette das strukturell geloest, war aber aus Zeitgruenden nicht mehr Teil des Prototyps.

- is_late_arrival ist ein Platzhalter. Der Streaming-Job schliesst als "late" markierte Events komplett von der Gold-Aggregation aus (Ablage nur in der DLQ) statt sie nachtraeglich in ein bereits geschriebenes Fenster einzurechnen. Dadurch kann das Feld in der Gold-Tabelle nie true werden. Eine vollstaendige Late-Data-Korrektur haette einen Re-Aggregations-Mechanismus mit laengerem Watermark oder Update-Mode statt Append-Mode erfordert.

- Wetter-Aufloesung ist grob. Open-Meteo liefert Wetterdaten je Borough-Zentroid (5 feste Koordinatenpaare), nicht je Segment. Bei einem Stadtgebiet wie Manhattan kann die tatsaechliche lokale Wetterlage an einem Segment mehrere Kilometer vom Abfragepunkt abweichen. Fuer den Prototyp ausreichend, fuer eine produktive Nutzung waere ein feineres Gitter oder ein Wetterdienst mit segment-genauen Daten noetig.

- Anomalie-Bestaetigung ist einfach gehalten. Der Zustandsautomat in upsert_anomaly_state zaehlt aufeinanderfolgende auffaellige Fenster pro Segment und bestaetigt eine Anomalie ab einer festen Schwelle (Default 3 Fenster). Komplexere Muster (z. B. Erholung nach Anomalie, Unterscheidung zwischen kurzfristigem Ausreisser und dauerhafter Verschlechterung) werden nicht abgebildet.

### Bekannte betriebliche Einschraenkungen

- CI/CD deckt Build, Lint und Image-Push ab, ersetzt aber keine automatisierten Tests. Es existieren keine Unit- oder Integrationstests fuer die Streaming-Logik; Verifikation erfolgte manuell durch Log-Beobachtung und Stichproben in Kafka/Delta.

- Ressourcenlimits wurden iterativ per Trial-and-Error auf die konkrete 3-Node-Umgebung (je 4 vCPU) eingestellt, nicht systematisch dimensioniert. Der Processing-Pod lief unter der urspruenglichen Grenze von 2Gi bei aktivierter Anomalie-Erkennung in einen OutOfMemoryError; nach Erhoehung auf 4Gi und Reduktion der Spark-Parallelitaet (local[2] statt local[*]) stabil. Auf anderer Hardware waeren die Werte erneut zu pruefen.

- Der erste Batch nach einem Neustart mit taeglich wachsendem historischem Datenbestand (Bronze/Silver/Gold) braucht spuerbar laenger als Folge-Batches (Startoffset earliest liest den kompletten bisherigen Kafka-Verlauf). Fuer den Prototyp unkritisch, in einer produktiven Umgebung wuerde man Kafka-Retention und Startoffset-Strategie bewusst gegeneinander abwaegen.

### Ausblick

Naheliegende naechste Schritte, absteigend nach vermutetem Aufwand-Nutzen-Verhaeltnis:

1. Serving-API um einen Endpunkt fuer den Anomalie-Zustand (gold/anomaly_state) ergaenzen, damit bestaetigte Anomalien auch ueber die UI sichtbar werden, nicht nur in der Delta-Tabelle.
2. Private Container-Registry im Cluster (z. B. registry:2 als eigenes Deployment) einrichten, um die manuelle Multi-Node-Image-Verteilung abzuloesen.
3. Unit-Tests fuer die reinen Transformationsfunktionen (z. B. derive_condition, _stable_fraction, upsert_anomaly_state-Zustandsuebergaenge) ergaenzen und in die bestehende CI-Pipeline einhaengen.
4. Late-Data-Korrektur ueber Update-Mode statt der aktuellen Ausschluss-Logik, damit is_late_arrival tatsaechlich befuellt werden kann.
5. Leaflet/OpenStreetMap statt der selbstgebauten SVG-Projektion fuer die Kartenansicht, fuer einen geografisch korrekten Kartenhintergrund.
