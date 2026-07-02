FROM flink:1.18

# The flink:1.18 image ships only a JRE. apache-flink pulls in pemja, whose
# native wheel needs the JDK headers (jni.h), so install a full JDK too.
RUN apt-get update && \
    apt-get install -y python3 python3-pip wget default-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

# Build pemja/apache-flink against the apt JDK (headers included). JAVA_HOME is
# scoped to this RUN only, so Flink's runtime JAVA_HOME (/opt/java/openjdk) is
# left untouched.
RUN export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" && \
    pip3 install apache-flink==1.18.0

# Flink's `-py` runner and the PyFlink UDF workers invoke `python`, but the
# image only provides `python3`. Symlink so both the client and TaskManager
# can launch the interpreter.
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Kafka SQL connector — the shaded fat jar that also bundles kafka-clients.
# (The plain flink-connector-kafka jar omits kafka-clients, which breaks the
# Table/SQL 'kafka' connector with NoClassDefFoundError: OffsetResetStrategy.)
RUN wget -q -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# JDBC connector for Snowflake sink
RUN wget -q -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar

# Snowflake JDBC driver
RUN wget -q -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/net/snowflake/snowflake-jdbc/3.25.1/snowflake-jdbc-3.25.1.jar

# Prometheus metrics reporter (ships in the Flink distribution under plugins/).
# Copying it into lib/ puts the reporter factory on the classpath so Prometheus
# can scrape JVM CPU/memory and per-operator record throughput on port 9249.
RUN cp /opt/flink/plugins/metrics-prometheus/flink-metrics-prometheus-*.jar /opt/flink/lib/
