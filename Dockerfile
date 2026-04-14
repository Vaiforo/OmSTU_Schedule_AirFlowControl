FROM apache/airflow:3.1.6

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         libkrb5-dev \
         libsasl2-dev \
         libssl-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    airflow-code-editor \
    hdfs \
    apache-airflow-providers-apache-hdfs \
    pyspark \
    clickhouse-connect
