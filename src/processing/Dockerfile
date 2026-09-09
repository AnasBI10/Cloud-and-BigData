FROM spark:3.5.8-python3

USER root
WORKDIR /opt/spark-app

COPY src/processing/streaming_job_bsg.py ./streaming_job_bsg.py
COPY schemas/traffic_speed_event.avsc ./schemas/traffic_speed_event.avsc
COPY data/dot_links_seed.json ./data/dot_links_seed.json

RUN mkdir -p /opt/spark-app/checkpoints /opt/spark-app/ivy-cache && \
    chown -R 185:185 /opt/spark-app

USER 185

ENTRYPOINT ["/opt/spark/bin/spark-submit", \
            "--master", "local[*]", \
            "--conf", "spark.jars.ivy=/opt/spark-app/ivy-cache", \
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,org.apache.spark:spark-avro_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4", \
            "/opt/spark-app/streaming_job_bsg.py"]
