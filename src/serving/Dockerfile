# Build-Kontext ist das Repo-Wurzelverzeichnis:
#   docker build -f src/serving/Dockerfile -t congestion-watch/serving:0.1.0 .
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY src/serving/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/serving/*.py ./
COPY data/dot_links_seed.json ./data/dot_links_seed.json

# Einspeisung (SCRUM-89): dieselbe Avro-Logik wie die Producer.
COPY src/ingestion/common.py ./common.py
COPY schemas/traffic_speed_event.avsc ./schemas/traffic_speed_event.avsc

RUN useradd --create-home --uid 10001 serving
USER 10001

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
