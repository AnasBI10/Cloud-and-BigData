# Producer-Image fuer beide Modi (SCRUM-75).
# Python 3.12, weil confluent-kafka fuer 3.14 noch keine Wheels liefert und
# ein Quellcode-Build librdkafka als Systemabhaengigkeit nach sich zoege.
FROM python:3.12-slim

WORKDIR /app

# Zuerst nur die Requirements kopieren, damit der pip-Layer im Cache bleibt,
# solange sich die Abhaengigkeiten nicht aendern.
COPY src/ingestion/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src/ingestion/*.py /app/
# Schema und Seed liegen im Image, nicht in einer ConfigMap: beide sind Teil
# des Codevertrags und aendern sich nur mit einem neuen Build. Die ConfigMap
# ist fuer Betriebsparameter da (SCRUM-81), nicht fuer Artefakte.
COPY schemas/traffic_speed_event.avsc /app/schemas/traffic_speed_event.avsc
COPY data/dot_links_seed.json /app/data/dot_links_seed.json

# Nicht als root laufen.
RUN useradd --create-home --uid 10001 appuser
USER appuser

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/app/main.py"]
