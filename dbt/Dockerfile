# syntax=docker/dockerfile:1.7
FROM python:3.10-slim-bookworm

# default-jre-headless (not openjdk-17-jre-headless alone) is what provides
# the /usr/lib/jvm/default-java symlink JAVA_HOME points at below.
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

# pyspark==3.5.3 must match the Iceberg jars below and the streaming job's
# Spark version (spark_streaming/Dockerfile, apache/spark:3.5.3-python3) —
# don't drift these. dbt's session mode embeds its own JVM via py4j, so it
# doesn't need the full Spark cluster image, just a matching pyspark version.
# Pinned to 3.5.3, not 4.x — see the note in spark_streaming/Dockerfile:
# Iceberg's Spark 4.1 support (1.11.0) is still unfinished upstream (hit an
# NPE from an open, unmerged Iceberg PR), and Spark 4.2 has no
# iceberg-spark-runtime at all yet. 3.5.3 is Iceberg 1.11.0's maintained target.
RUN pip install --no-cache-dir \
    "dbt-spark[session]==1.11.0" \
    "pyspark==3.5.3"

# 2.12, not 2.13: PyPI's pyspark wheel bundles Scala 2.12 jars (same as
# apache/spark:3.5.3-python3) -- a 2.13 Iceberg jar loads but throws
# NoClassDefFoundError at runtime (see spark_streaming/Dockerfile's note).
ARG ICEBERG_VERSION=1.11.0
ARG SCALA_VERSION=2.12

# dbt-spark's `session` method builds its own local SparkSession in-process
# (no cluster to hand jars to at submit time), so the Iceberg jars have to
# already be on pyspark's classpath at image-build time. iceberg-aws-bundle
# brings S3FileIO (AWS SDK v2), replacing hadoop-aws/s3a:// entirely.
RUN JARS_DIR="$(python3 -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')/jars" && \
    curl -fsSL -o "$JARS_DIR/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" && \
    curl -fsSL -o "$JARS_DIR/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

# dbt-spark's session method reads Spark config from $SPARK_CONF_DIR (it
# builds the SparkSession itself, no spark-submit involved) — entrypoint.sh
# renders this dir's spark-defaults.conf at container start from
# $ICEBERG_CATALOG_TYPE (rest for dev, glue for aws).
ENV SPARK_CONF_DIR=/opt/spark-conf
RUN mkdir -p /opt/spark-conf

COPY spark-conf/ /opt/spark-conf-templates/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

WORKDIR /usr/app
