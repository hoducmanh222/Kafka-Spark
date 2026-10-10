FROM python:3.11.13-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl ca-certificates && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3 \
    SPARK_LOCAL_IP=0.0.0.0
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY scripts/resolve_connector.py scripts/resolve_connector.py
# Resolve the exact Scala 2.12 connector and transitive dependencies at build time.
RUN python -c "import pyspark; print(pyspark.__version__)" && \
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 scripts/resolve_connector.py
COPY . /app
CMD ["python", "streaming_etl.py"]
