FROM apache/airflow:2.7.2-python3.10

USER root

# Java 17 (requis par PySpark)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Dépendances Python du projet (pyspark==3.5.8 pour rester aligné avec hadoop-aws:3.3.4)
RUN pip install --no-cache-dir --quiet \
    yfinance \
    "pyspark==3.5.8" \
    awscli \
    awscli-local \
    boto3

# Copier le code source dans l'image (plus besoin de bind mount)
COPY --chown=airflow:root dags /opt/airflow/dags
COPY --chown=airflow:root src  /opt/airflow/src
