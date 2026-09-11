FROM apache/spark:3.5.3-python3

USER root

# wget fetches the connector/Hudi jars below (python3 is already bundled).
# openjdk-17: Hudi 0.15.0 runs fine on Java 8/11/17 -- kept at 17 (rather than
# the base image's default 11) since spark-thrift shares this same image and
# base, no reason to maintain two JDKs.
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
# hudi-spark3.5-bundle_2.12:0.15.0 is Hudi's release matching Spark 3.5.x
# (bundle name pins the Spark line directly). hadoop-aws/aws-java-sdk-bundle
# versions are pinned to what this Spark distribution's bundled Hadoop client
# (3.3.4) expects, to avoid classpath conflicts between hadoop-common and
# hadoop-aws.

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

# Hudi jars. _2.12 to match the base image's Scala version (see Kafka
# connector note above). Hudi has no bundled S3 client of its own (unlike
# Iceberg's S3FileIO) -- it writes through Hadoop's S3A filesystem, so
# hadoop-aws + the AWS SDK bundle are added here too; same jars work for both
# the dev MinIO endpoint and real AWS S3.
RUN wget -q -P $SPARK_HOME/jars \
    https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/0.15.0/hudi-spark3.5-bundle_2.12-0.15.0.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# entrypoint.sh renders $SPARK_CONF_DIR/spark-defaults.conf at container
# start from the template matching $LAKE_ENV (rest for dev, glue for aws) —
# one image, no rebuild needed to switch environments. $SPARK_HOME/conf
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
