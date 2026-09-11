# syntax=docker/dockerfile:1.7
FROM python:3.10-slim-bookworm

# method: thrift -- dbt is just a PyHive/Thrift client talking to the
# spark-thrift service (docker-compose.yml), which itself runs on the real
# spark-master cluster. No local JVM/pyspark/Hudi jars needed here; all
# Spark/Hudi config lives on the cluster side (spark-conf/*.template via
# spark-thrift's entrypoint.sh), not in this image.
RUN pip install --no-cache-dir "dbt-spark[PyHive]==1.11.0" thrift_sasl

WORKDIR /usr/app
