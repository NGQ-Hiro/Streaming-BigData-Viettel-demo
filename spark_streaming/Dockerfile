FROM apache/spark:3.5.3-python3

USER root

# wget fetches the connector/Iceberg jars below (python3 is already bundled).
# openjdk-17: Iceberg 1.11.0's jars are compiled to class file version 61
# (Java 17) and won't load on this base image's default Java 11
# (UnsupportedClassVersionError) -- install 17 alongside and make it the
# active JAVA_HOME; Spark 3.5.3 itself supports both.
RUN apt-get update && \
    apt-get install -y wget openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s "$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" /opt/java17

ENV JAVA_HOME=/opt/java17
ENV PATH="$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

# ponytail: jar versions below are best-effort pins for a demo repo. If
# `spark-submit --version` doesn't print 3.5.3, or a NoSuchMethodError /
# ClassNotFoundException shows up at runtime, check what actually landed with
# `ls $SPARK_HOME/jars | grep -E 'hadoop-client|scala-library'` and re-pin.
#
# Pinned to 3.5.3, not 4.1.0/4.2.0: tried both. Iceberg has no
# iceberg-spark-runtime built against Spark 4.2's API (View class ->
# interface change breaks IncompatibleClassChangeError). Spark 4.1 does have
# a matching iceberg-spark-runtime-4.1_2.13 jar, but as of 1.11.0 that
# integration is unfinished upstream (25+ open "Spark 4.1" issues in
# apache/iceberg) — hit a real one: WriteToDataSourceV2Exec.customMetrics NPEs
# on SparkPlan.session() during streaming-write planning (open PR
# apache/iceberg#17820, not yet merged). 3.5.3 is Iceberg 1.11.0's actual
# maintained Spark target. Revisit 4.x once Iceberg's Spark 4.1 support matures.

# Kafka Structured Streaming source. Versions pinned to what
# spark-sql-kafka-0-10_2.12:3.5.3's own POM declares as its compile
# dependency (checked via its published .pom on Maven Central) — using a
# newer kafka-clients/commons-pool2 than the connector expects is what broke
# things under 4.1.0. _2.12 (not _2.13): apache/spark:3.5.3-python3 ships
# Scala 2.12 jars (scala-library-2.12.18.jar) -- a _2.13 connector jar loads
# but throws NoClassDefFoundError (scala/collection/SeqOps etc.) at runtime.
RUN wget -q -P $SPARK_HOME/jars \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Iceberg jars. iceberg-aws-bundle brings S3FileIO (AWS SDK v2), replacing
# the old s3a:// / hadoop-aws connector — same jar works for both the dev
# REST catalog (MinIO) and the aws Glue catalog. _2.12 to match the base
# image's Scala version (see Kafka connector note above).
RUN wget -q -P $SPARK_HOME/jars \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar

# entrypoint.sh renders $SPARK_CONF_DIR/spark-defaults.conf at container
# start from the template matching $ICEBERG_CATALOG_TYPE (rest for dev, glue
# for aws) — one image, no rebuild needed to switch catalogs. $SPARK_HOME/conf
# already exists and is owned by the spark user from the base image.
ENV SPARK_CONF_DIR=$SPARK_HOME/conf

COPY spark-conf/ /opt/spark-conf-templates/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Pre-create as spark:spark so a fresh named volume mounted here (compose's
# spark-checkpoints) inherits this ownership instead of defaulting to root
# (Docker copies the image directory's owner/perms into a new volume on its
# first mount, but only if the directory already exists in the image).
RUN mkdir -p $SPARK_HOME/checkpoints && chown spark:spark $SPARK_HOME/checkpoints

USER spark

ENTRYPOINT ["/entrypoint.sh"]
