# Deploy & Test: SCRUM-89 (Einspeisung über die UI)

Schritt-für-Schritt, um den geänderten Stand auf den k3s-Cluster zu bringen und
zu prüfen, dass ein über die UI erzeugtes Event **wirklich** durch die Pipeline
läuft (Kafka → Spark → Gold → API). Erst danach ist SCRUM-89 abnahmefähig.

> Stand: committet auf `dev`, aber **nicht gepusht** und nicht am Cluster
> verifiziert. Zeigt der Test einen Fehler, wird im Arbeitsverzeichnis
> nachgebessert, nicht im Cluster.

## Wer macht was

| Teil | Wer | Wo |
|---|---|---|
| A — Image bauen | BT oder Anas | Rechner mit Docker |
| B — Image in den Cluster | Anas | k3s-Node (`ny-traffic-master`) |
| C — Helm upgrade | Anas | k3s-Node |
| D — Baseline-Job | Anas | k3s-Node |
| E — Rauchtest API | Anas | k3s-Node / lokal per port-forward |
| F — End-to-End über die UI | **BT** | lokal, gegen port-forward |
| G — Screenshots Kapitel 11 | BT | lokal |

## Was sich geändert hat

- `src/serving/` — neuer `publisher.py`, zwei POST-Endpunkte, Reader liest das
  Gold-Schema nach SCRUM-83 (Score und `has_baseline` kommen fertig aus dem
  Job, die Baseline-Tabelle nur noch für Kartenzustand und Szenario-Startwert).
- `src/ui/` — dazu das Dashboard aus SCRUM-90 (Karte, Rangliste, Zeitreihe).
- `src/serving/Dockerfile` — kopiert zusätzlich `src/ingestion/common.py` und das
  Avro-Schema ins Image (Build-Kontext bleibt Repo-Root).
- `deploy/helm/.../serving.yaml` — ConfigMap um `GOLD_READER=delta`, `INGEST_MODE=kafka`
  und die Kafka-/Registry-Adressen erweitert; `enableServiceLinks: false`.
- `deploy/helm/.../baseline.yaml` — **neuer** CronJob, der `compute_baseline.py`
  fährt. Ohne ihn bleibt `congestion_score` überall `null`.
- `values.yaml` — `serving.image` von `0.1.0` auf **`0.2.0`**.
- `src/ui/` — Formular + Szenario-Generator (Containerisierung ist SCRUM-91,
  hier läuft die UI noch lokal).

Seit dem Merge mit `origin/dev` kommt **das Processing-Image mit dazu**:
Victor hat den Baseline-Join in den Streaming-Job gezogen (SCRUM-83), das Image
steht in `values.yaml` jetzt auf `congestion-watch/processing:0.6.0`. Es muss
also ebenfalls gebaut und importiert werden — dieselben Schritte wie für die
Serving-API, nur mit `-f src/processing/Dockerfile` und dem anderen Tag.

**Neue Reihenfolge-Abhängigkeit:** `streaming_job_bsg.py` lädt
`s3a://gold/baseline_profile` beim Start und broadcastet sie. Existiert die
Tabelle noch nicht, startet der Job nicht. **Teil D muss deshalb vor dem
Neustart des Streaming-Jobs laufen**, beim Erstaufbau einmal von Hand.

---

## Teil 0 — Arbeitsstand auf den Cluster-Node bringen

Alles läuft über die DHBW-Cloud (OpenStack). Der k3s-Node ist `ny-traffic-master`
(intern `192.168.10.53`), erreichbar über VPN / DHBW-Netz. Der geänderte Stand
ist **nicht committet**, muss also als Dateien auf den Node.

Da der Stand committet, aber noch nicht gepusht ist, geht der Transport
weiterhin über ein Archiv statt über `git pull` auf dem Node.

**Auf deinem Rechner**, im Repo-Wurzelverzeichnis:

```bash
tar --exclude=venv --exclude=.git --exclude='*.tfstate*' --exclude=.terraform \
    -czf /tmp/cw-scrum89.tgz .

# Node-Adresse und Key wie gewohnt (aus deiner OpenStack-Config).
scp -i <dein-key> /tmp/cw-scrum89.tgz ubuntu@<node>:/tmp/
```

**Auf dem Node:**

```bash
ssh -i <dein-key> ubuntu@<node>
mkdir -p ~/cw-scrum89 && tar -xzf /tmp/cw-scrum89.tgz -C ~/cw-scrum89
cd ~/cw-scrum89
```

Ab hier laufen die folgenden Teile in diesem Verzeichnis auf dem Node.

**Build-Werkzeug prüfen:**

```bash
which docker && docker version --format '{{.Server.Version}}'
```

- **Docker da** → Teil A wie beschrieben.
- **Kein Docker** → mit k3s' containerd bauen:
  ```bash
  sudo k3s ctr version                     # containerd vorhanden
  # nerdctl + buildkit einmalig:
  sudo apt-get install -y buildah 2>/dev/null || true
  ```
  Dann in Teil A `docker build` durch
  `sudo buildah bud -f src/serving/Dockerfile -t congestion-watch/serving:0.2.0 .`
  ersetzen und in Teil B
  `sudo buildah push congestion-watch/serving:0.2.0 \
     oci-archive:/tmp/serving-0.2.0.tar` → `sudo k3s ctr -n k8s.io images import /tmp/serving-0.2.0.tar`.

---

## Vorbedingungen (einmal prüfen)

Auf dem k3s-Node:

```bash
sudo k3s kubectl -n bigdata get pods
```

Es müssen laufen und `Running`/`Ready` sein:
`kafka-0/1/2`, `schema-registry-*`, `minio-0`, `processing-streaming-*`,
`producer-synthetic-*`. Wenn `processing-streaming` noch nie lief, gibt es keine
Silver-Daten und Teil D bleibt leer — dann zuerst den Streaming-Job ein paar
Stunden laufen lassen.

```bash
# Silver-Tabelle vorhanden? (Voraussetzung für die Baseline)
sudo k3s kubectl -n bigdata exec deploy/processing-streaming -- \
  ls -la /tmp 2>/dev/null || true
# einfacher: im MinIO-Console-UI (Port 9001) nachsehen, ob
# bronze/traffic_speeds_valid Objekte hat.
```

`helm` ist auf dem Node installiert (Story 80/81). Falls nicht:
`curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash`

---

## Teil A — Serving-Image bauen

Auf einem Rechner mit Docker, im Repo-Wurzelverzeichnis (`Cloud-and-BigData/`):

```bash
git status          # nur die SCRUM-89-Änderungen, sonst nichts
docker build -f src/serving/Dockerfile -t congestion-watch/serving:0.2.0 .
```

Der Build-Kontext ist bewusst `.` (Repo-Root), nicht `src/serving/` — das Image
braucht `data/dot_links_seed.json`, `src/ingestion/common.py` und
`schemas/traffic_speed_event.avsc`.

Kurz gegenprüfen, dass die Ingest-Dateien drin sind:

```bash
docker run --rm congestion-watch/serving:0.2.0 \
  sh -c 'ls -1 /app /app/schemas'
# erwartet u.a.: common.py, publisher.py, main.py, schemas/traffic_speed_event.avsc
```

Image als Tar exportieren (für den Transport auf den Node):

```bash
docker save congestion-watch/serving:0.2.0 -o /tmp/serving-0.2.0.tar
```

---

## Teil B — Image in den Cluster laden

k3s nutzt **containerd**, nicht Docker, und es gibt keine Registry. Das Image
muss in containerd importiert werden.

**Wenn auf dem Node selbst gebaut wurde:**

```bash
docker save congestion-watch/serving:0.2.0 | sudo k3s ctr -n k8s.io images import -
```

**Wenn woanders gebaut wurde:** Tar auf den Node kopieren, dann importieren:

```bash
scp -i <key> /tmp/serving-0.2.0.tar ubuntu@<node>:/tmp/
ssh -i <key> ubuntu@<node>
sudo k3s ctr -n k8s.io images import /tmp/serving-0.2.0.tar
```

Prüfen:

```bash
sudo k3s ctr -n k8s.io images ls | grep serving
# congestion-watch/serving:0.2.0  ...
```

> **Wichtig:** Es muss ein **neuer Tag** sein (`0.2.0`, nicht `0.1.0`
> überschreiben). Wegen `imagePullPolicy: IfNotPresent` würde ein
> überschriebener Tag nicht neu gezogen — der alte Stand liefe weiter.

---

## Teil C — Helm upgrade

Auf dem Node, im Chart-Verzeichnis. Dieselben `--set`-Werte wie beim letzten
`helm install` verwenden (echtes MinIO-Passwort, echter Socrata-Token — nicht
`CHANGE_ME`):

```bash
cd deploy/helm

# Vorschau: was ändert sich?
helm diff upgrade congestion-watch ./congestion-watch -n bigdata \
  --set minio.password='<PW>' --set socrata.appToken='<TOKEN>'  \
  || true   # helm-diff-Plugin optional; wenn nicht da, überspringen

helm upgrade congestion-watch ./congestion-watch -n bigdata \
  --set minio.password='<PW>' --set socrata.appToken='<TOKEN>'
```

Erwartete Änderungen: `configmap/serving-config` aktualisiert,
`deployment/serving-api` neu ausgerollt (Image `0.2.0`),
`cronjob/baseline-profile` **neu**.

```bash
sudo k3s kubectl -n bigdata rollout status deployment/serving-api --timeout=5m
sudo k3s kubectl -n bigdata get pods -l app=serving-api
```

Wenn ein Pod nicht hochkommt:

```bash
sudo k3s kubectl -n bigdata logs deploy/serving-api
sudo k3s kubectl -n bigdata describe pod -l app=serving-api | tail -30
```

Häufigste Ursachen:
- `ImagePullBackOff` → Teil B nicht gemacht oder falscher Tag.
- `probe failed: /ready 503` → Gold-Tabelle noch nicht lesbar. Kurz normal,
  solange `processing-streaming` noch kein Fenster geschrieben hat. `/health`
  muss trotzdem grün sein, sonst startet der Pod in einer Schleife neu.

---

## Teil D — Baseline einmalig erzeugen

**Vor** dem Rollout des Streaming-Jobs, nicht danach: er lädt die Tabelle beim
Start und kommt ohne sie nicht hoch. Der CronJob läuft sonst alle 6 Stunden.

```bash
sudo k3s kubectl -n bigdata create job baseline-initial --from=cronjob/baseline-profile
sudo k3s kubectl -n bigdata logs -f job/baseline-initial
```

Am Ende steht eine Zeile wie
`Baseline geschrieben: N Zellen ueber M Segmente nach s3a://gold/baseline_profile`.

- `N = 0` → die Silver-Tabelle ist leer oder zu dünn
  (`MIN_SAMPLES_PER_CELL = 5`). Streaming-Job länger laufen lassen.
- Job schlägt mit S3-Fehler fehl → `minio-credentials`-Secret prüfen.

Dann die API den neuen Stand lesen lassen (Cache ist 15 min):

```bash
sudo k3s kubectl -n bigdata rollout restart deployment/serving-api
sudo k3s kubectl -n bigdata rollout status deployment/serving-api --timeout=5m
```

---

## Teil E — Rauchtest der API

```bash
sudo k3s kubectl -n bigdata port-forward svc/serving-api 8000:80
```

In einem zweiten Terminal:

```bash
curl -s localhost:8000/ready | python3 -m json.tool
```

Erwartet:
```json
{
  "status": "ready",
  "reader": "delta",
  "ingest": "kafka",
  "detail": "Delta-Version ... , Baseline mit N Zellen",
  "latest_window": "2026-09-..."
}
```

- `"ingest": "dryrun"` statt `"kafka"` → ConfigMap nicht übernommen, Teil C
  wiederholen.
- `"detail": "... OHNE Baseline ..."` → Teil D hat nichts geschrieben.
- `latest_window: null` → der Streaming-Job schreibt gerade keine Fenster.

```bash
curl -s "localhost:8000/api/anomalies?limit=5" | python3 -m json.tool
# segments_with_baseline muss > 0 sein.
```

Einzelnes Event durch den echten Producer:

```bash
curl -s -X POST localhost:8000/api/events \
  -H 'content-type: application/json' \
  -d '{"link_id":"4329472","speed_mph":9.5}' | python3 -m json.tool
```

Erwartet: `"published": true`, `"ingest": "kafka"`,
`"topic": "traffic.speeds.raw"`, ein `event_key`.

Wenn das mit `503` und `Producer nicht verfuegbar` scheitert:
```bash
sudo k3s kubectl -n bigdata logs deploy/serving-api | grep -i -E "producer|schema|kafka"
```
Meist: Schema-Registry nicht erreichbar, oder das Avro-Schema im Image passt
nicht zur registrierten Version.

---

## Teil F — End-to-End über die UI (der eigentliche SCRUM-89-Nachweis)

`port-forward` aus Teil E offen lassen.

**1. Synthetischen Producer kurz anhalten** — damit das Szenario-Signal im
Fenstermittel nicht untergeht:

```bash
sudo k3s kubectl -n bigdata scale deployment/producer-synthetic --replicas=0
```

**2. UI lokal starten** (im Repo):

```bash
cd src/ui
printf 'window.APP_CONFIG = { apiBase: "http://localhost:8000" };\n' > config.js
python3 -m http.server 8080
```

`config.js` wird nicht committet — es ist die lokale Testkonfiguration.

**3. Browser:** <http://localhost:8080/index.html>

- Kopf zeigt `API ok · delta · kafka` (grün, **nicht** gelb/`dryrun`).
- Segment wählen, das eine Baseline hat (Kontextzeile: „Baseline vorhanden").
  Notiere die `link_id` aus der Kontextzeile.
- Karte „Szenario": Segment = dasselbe, Verlauf = **congestion**,
  Dauer = **10** min, Frequenz = **30** Events/min. Starten.
- Der Fortschrittsbalken läuft, „X von 300 Events".

**4. Wirkung beobachten** (zweites Terminal, `LINK` = die notierte ID):

```bash
LINK=4329472
watch -n 20 "curl -s localhost:8000/api/segments/$LINK/timeseries?hours=1 \
  | python3 -c 'import json,sys; p=json.load(sys.stdin)[\"points\"][-3:]; \
  [print(x[\"window_start\"], \"speed\", x[\"speed_avg\"], \"score\", x[\"congestion_score\"]) for x in p]'"
```

Nach ~2–4 Minuten muss ein neues Fenster erscheinen, in dem `speed_avg` deutlich
gefallen und `congestion_score` gestiegen ist. Damit ist bewiesen:
UI → Kafka → Spark-Aggregation → Gold → API, ohne Seiteneingang.

**5. Statusfilter zeigen** (optional, zweites Szenario): Verlauf =
**sensor_outage** auf einem Segment mit aktueller Messung. Nach ein paar Minuten
bekommt das Segment **kein** neues Fenster mehr (die `-101`-Events werden in
Silver verworfen) — in `/api/segments` fällt `last_seen` zurück.

**6. Aufräumen:**

```bash
sudo k3s kubectl -n bigdata scale deployment/producer-synthetic --replicas=1
```

---

## Teil G — Screenshots für Kapitel 11

Aus der laufenden UI (Teil F):

1. **Formular in Aktion** — Segment gewählt, Werte eingetragen, eine
   Event-Quittung sichtbar (`Event zugestellt → traffic.speeds.raw`).
2. **Szenario läuft** — Fortschrittsbalken + „sichtbar ab …".
3. **Wirkung in den Daten** — das Terminal aus Schritt 4 mit dem Knick in
   `speed_avg`, oder der `/api/anomalies`-Ausschnitt, in dem das Segment nach
   oben wandert.

Ablage: `screenshots/` mit sprechenden Namen
(`04-ui-formular.png`, `05-ui-szenario.png`, `06-ui-wirkung.png`).

---

## Rollback

```bash
helm rollback congestion-watch -n bigdata
sudo k3s kubectl -n bigdata scale deployment/producer-synthetic --replicas=1
```

Der CronJob `baseline-profile` und die Tabelle `s3a://gold/baseline_profile`
bleiben dabei bestehen — sie stören den alten Stand nicht (die alte API liest
sie nicht).

---

## Wenn der Test durch ist

- Screenshots in `screenshots/` ablegen.
- Zurückmelden, ob der Kafka-Pfad sauber lief — dann schreibe ich README
  Kapitel 7 und wir committen SCRUM-89.
- Auffälligkeiten (Latenz, Fehlermeldungen, unerwartete DLQ-Einträge) notieren.
