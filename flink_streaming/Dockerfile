FROM flink:1.18

RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install apache-flink==1.18.0

# Kafka connector + JSON format (bundled in flink-connector-kafka)
RUN wget -q -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.1.0-1.18/flink-connector-kafka-3.1.0-1.18.jar

# JDBC connector for Snowflake sink
RUN wget -q -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar

# Snowflake JDBC driver
RUN wget -q -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/net/snowflake/snowflake-jdbc/3.25.1/snowflake-jdbc-3.25.1.jar
