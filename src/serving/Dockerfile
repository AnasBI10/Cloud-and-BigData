# Build-Kontext ist das Repo-Wurzelverzeichnis, nicht src/serving:
#   docker build -f src/serving/Dockerfile -t congestion-watch/serving:0.1.0 .
#
# Grund: das Image braucht neben src/serving auch data/dot_links_seed.json,
# src/ingestion/common.py und das Avro-Schema — dieselben Quellen, die die
# Producer verwenden. Der Seed liefert die Kartengrundlage fuer /api/segments,
# common.py und das Schema die Einspeisung aus SCRUM-89.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY src/serving/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/serving/*.py ./
COPY data/dot_links_seed.json ./data/dot_links_seed.json

# Einspeisung (SCRUM-89): die API baut und serialisiert Events mit demselben
# Code wie die Producer, statt die Avro-Logik ein zweites Mal zu pflegen.
# common.py landet neben den Serving-Modulen, publisher.py findet es dort.
COPY src/ingestion/common.py ./common.py
COPY schemas/traffic_speed_event.avsc ./schemas/traffic_speed_event.avsc

# Nicht als root: die Aufgabenstellung verlangt sinnvolle Workload-Typen,
# ein unnoetig privilegierter Container gehoert nicht dazu.
RUN useradd --create-home --uid 10001 serving
USER 10001

EXPOSE 8000

# Ein Worker je Pod. Horizontal wird ueber Repliken skaliert, nicht ueber
# Prozesse im Container — sonst zaehlt der HPA CPU eines Pods, der intern
# schon mehrfach parallelisiert.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
