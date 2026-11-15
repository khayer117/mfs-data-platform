FROM apache/airflow:3.0.2

# Java is required by PySpark (runs bronze_to_silver.py in local[*] mode)
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    psycopg2-binary==2.9.9 \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-postgres \
    dbt-postgres==1.9.1
