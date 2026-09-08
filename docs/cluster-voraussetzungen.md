# Cluster-Voraussetzungen

Was auf dem Cluster vorhanden sein muss, bevor die Manifeste greifen. Vorlage
für Kapitel 9 der README (Deployment-Anleitung, SCRUM-97).

Alle Angaben verifiziert am 04.09.2026 auf einem kind-Cluster mit einer
Control Plane und drei Workern, Kubernetes v1.36.1.

## 1. Cluster

Ein Cluster mit **mindestens drei Worker-Nodes**. Kafka läuft mit
Replikationsfaktor 3 und verteilt seine Broker per Anti-Affinity auf
verschiedene Nodes. Auf weniger Nodes startet es zwar (die Anti-Affinity ist
`preferred`, nicht `required`), aber RF=3 bedeutet dann keine drei echten
Ausfalldomänen mehr.

```bash
kind create cluster --name ccbd --config kind-config.yaml
```

## 2. StorageClass mit Default

Die drei Kafka-PVCs beziehen sich auf keine explizite `storageClassName`,
verwenden also die Default-StorageClass. Fehlt eine, bleiben die PVCs auf
`Pending` und das StatefulSet startet nie.

```bash
kubectl get storageclass
```

Erwartet: ein Eintrag mit `(default)`. Bei kind ist das
`standard (rancher.io/local-path)`.

`VOLUMEBINDINGMODE: WaitForFirstConsumer` ist normal — das Volume entsteht
erst, wenn der Pod einem Node zugewiesen ist. Die PVCs stehen deshalb beim
Hochfahren kurz auf `Pending`.

## 3. metrics-server (für den HPA)

**Ohne diesen Schritt ist der HorizontalPodAutoscaler wirkungslos.** Er wird
angelegt, meldet aber dauerhaft `cpu: <unknown>/70%` und skaliert nie. kind
bringt den metrics-server nicht mit.

```bash
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml

# Ohne dieses Flag scheitert der metrics-server beim Abfragen der Kubelets:
# kind verwendet selbstsignierte Node-Zertifikate.
kubectl patch deployment metrics-server -n kube-system --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'

kubectl rollout status deployment/metrics-server -n kube-system
```

Prüfen (erste Messwerte brauchen ca. eine Minute):

```bash
kubectl top pods
kubectl get hpa producer-synthetic
```

In `TARGETS` muss ein Prozentwert stehen, nicht `<unknown>`.

## 4. Namespace und Secret

```bash
kubectl create namespace congestion-watch
kubectl config set-context --current --namespace=congestion-watch

# Socrata App Token. Bewusst NICHT als Manifest im Repo — sonst überschreibt
# ein kubectl apply den echten Wert mit einem Platzhalter.
# Token kostenlos unter data.cityofnewyork.us (Profil -> App Tokens).
kubectl create secret generic socrata-credentials \
  --from-literal=app-token='<TOKEN>'
```

Ohne Token läuft der Live-Poller anonym mit strengerem Rate-Limit und
protokolliert eine Warnung. Für ein Poll-Intervall von 5 Minuten reicht das.

## 5. ConfigMap mit dem Avro-Schema

Der Registrierungs-Job mountet das Schema aus einer ConfigMap.

```bash
kubectl create configmap avro-schemas \
  --from-file=schemas/traffic_speed_event.avsc
```

## 6. Images ins Cluster laden

Bei kind sieht der Cluster den lokalen Docker-Cache nicht.

```bash
docker build -f src/ingestion/Dockerfile -t congestion-watch/ingestion:0.1.2 .
kind load docker-image congestion-watch/ingestion:0.1.2 --name ccbd
```

Bei jeder Codeänderung einen **neuen Tag** vergeben und ihn im Manifest
nachziehen. Ein überschriebener Tag wird wegen
`imagePullPolicy: IfNotPresent` nicht neu gezogen — der alte Stand läuft
weiter, ohne dass es auffällt.

## 7. Deploy-Reihenfolge

```bash
kubectl apply -f k8s/kafka/00-kafka-config.yaml
kubectl apply -f k8s/kafka/10-kafka-services.yaml
kubectl apply -f k8s/kafka/20-kafka-statefulset.yaml
kubectl rollout status statefulset/kafka --timeout=10m

kubectl apply -f k8s/kafka/30-kafka-topics-job.yaml
kubectl apply -f k8s/kafka/40-schema-registry.yaml
kubectl rollout status deployment/schema-registry --timeout=15m
kubectl apply -f k8s/kafka/50-schema-register-job.yaml

kubectl apply -f k8s/ingestion/60-producers.yaml
```

Die Reihenfolge ist nicht beliebig: Die Topics brauchen laufende Broker, die
Registry braucht Kafka für ihr `_schemas`-Topic, der Register-Job braucht die
Registry, und die Producer brauchen Schema und Topics.

## Beim Deployen aufgetretene Fallstricke

Dokumentiert, weil sie beim Nachbauen sonst erneut Zeit kosten.

**Kafka: `podManagementPolicy: Parallel` ist zwingend.** Bei `OrderedReady`
wartet Kubernetes auf die Readiness von `kafka-0`, die dieser nie erreicht,
solange `kafka-1` und `kafka-2` für das KRaft-Quorum fehlen. Deadlock.

**Schema-Registry: `enableServiceLinks: false` ist zwingend.** Kubernetes
injiziert für jeden Service im Namespace Umgebungsvariablen. Aus dem Service
`schema-registry` entsteht `SCHEMA_REGISTRY_PORT=tcp://10.96.x.x:8081`. Das
cp-Image liest jede `SCHEMA_REGISTRY_*`-Variable als Konfiguration, hält den
Wert für die veraltete `port`-Einstellung und bricht mit Exit 1 ab — ohne
verwertbare Fehlermeldung. Der Service sabotiert damit sein eigenes
Deployment.

**Live-Poller: StatefulSet, kein Deployment.** Der Shard-Index kommt aus dem
Pod-Ordinal. Ein Deployment gibt allen Repliken dieselbe Env-Variable und
Hostnamen mit Zufalls-Suffix; im Test zogen daraufhin beide Pods dieselben
drei Boroughs, während Brooklyn und Queens gar nicht mehr abgefragt wurden.

**Image-Pull dauert.** Das Kafka-Image ist ca. 400 MB, `cp-schema-registry`
rund 1,5 GB und wird auf mehrere Nodes gezogen. Der erste Rollout kann über
zehn Minuten brauchen; das Standard-Timeout von `kubectl rollout status`
reicht dafür nicht.