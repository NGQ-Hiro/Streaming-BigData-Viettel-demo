"""
Spark Structured Streaming job: Kafka -> latin1-decode + derived time columns
-> Iceberg bronze (staging.<topic>, catalog-managed, no raw s3a:// path).
Replaces the old Flink SQL job; keeps the exact same bronze column names
(ts_timestamp, year_, month_, day_, hour_, processed_time) since the
separate dbt-spark batch job depends on this schema.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    DoubleType,
    LongType,
    IntegerType,
    BooleanType,
)

from schema import SCHEMAS

KAFKA_BROKERS = "broker-1:29092,broker-2:29092,broker-3:29092"
CHECKPOINT_ROOT = "/opt/spark/checkpoints"

# Structured Streaming tracks Kafka progress via checkpoint files only -- it
# never joins a consumer group or commits offsets back to Kafka, so
# kafka_exporter's consumergroup_lag metric has nothing to report (confirmed:
# zero kafka_consumergroup_* series on the exporter). Each foreachBatch call
# below commits the batch's max offset per partition to this group so lag
# becomes a real, visible metric -- this commit is for observability only,
# Spark's own checkpoint remains the source of truth for resuming the stream.
CONSUMER_GROUP_PREFIX = "streamify"

# Iceberg's streaming sink doesn't auto-create tables (unlike Delta) --
# these build the CREATE TABLE DDL from SCHEMAS[topic] below.
_SQL_TYPE = {
    StringType: "STRING",
    DoubleType: "DOUBLE",
    LongType: "BIGINT",
    IntegerType: "INT",
    BooleanType: "BOOLEAN",
}

_DERIVED_COLUMNS = [
    ("ts_timestamp", "TIMESTAMP"),
    ("year_", "INT"),
    ("month_", "INT"),
    ("day_", "INT"),
    ("hour_", "INT"),
    ("processed_time", "TIMESTAMP"),
]


def _table_ddl(topic):
    cols = [f"{f.name} {_SQL_TYPE[type(f.dataType)]}" for f in SCHEMAS[topic].fields]
    cols += [f"{name} {sql_type}" for name, sql_type in _DERIVED_COLUMNS]
    return f"CREATE TABLE IF NOT EXISTS staging.{topic} ({', '.join(cols)}) USING iceberg"

# (topic, decode latin1-mojibake on artist/song)
TOPICS = [
    ("listen_events", True),
    ("page_view_events", True),
    ("auth_events", False),
]


def _decode(s):
    """latin1-mojibake fix, ported verbatim from the old Flink UDF."""
    if s:
        return (s.encode("latin1")
                 .decode("unicode-escape")
                 .encode("latin1")
                 .decode("utf-8"))
    return s


decode_udf = F.udf(_decode, StringType())


def build_stream(spark, topic, decode_strings):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), SCHEMAS[topic]).alias("data"),
        "partition",
        "offset",
    ).select("data.*", "partition", "offset")

    if decode_strings:
        parsed = (
            parsed.withColumn("artist", decode_udf(F.col("artist")))
                  .withColumn("song", decode_udf(F.col("song")))
        )

    ts_col = F.timestamp_millis(F.col("ts"))
    return (
        parsed
        .withColumn("ts_timestamp", ts_col)
        .withColumn("year_", F.year(ts_col))
        .withColumn("month_", F.month(ts_col))
        .withColumn("day_", F.dayofmonth(ts_col))
        .withColumn("hour_", F.hour(ts_col))
        .withColumn("processed_time", F.current_timestamp())
    )


def _commit_offsets(spark, group_id, topic, batch_df):
    """Commit this batch's max offset+1 per partition to `group_id` via the
    Kafka AdminClient (kafka-clients jar already on the driver classpath --
    see spark_streaming/Dockerfile). Only touches Kafka's consumer-group
    metadata, not the checkpoint Spark actually resumes from.
    """
    rows = batch_df.groupBy("partition").agg(F.max("offset").alias("max_offset")).collect()
    if not rows:
        return

    jvm = spark._jvm
    props = jvm.java.util.Properties()
    props.put("bootstrap.servers", KAFKA_BROKERS)
    admin = jvm.org.apache.kafka.clients.admin.AdminClient.create(props)
    try:
        offsets = jvm.java.util.HashMap()
        for row in rows:
            tp = jvm.org.apache.kafka.common.TopicPartition(topic, row["partition"])
            offsets.put(tp, jvm.org.apache.kafka.clients.consumer.OffsetAndMetadata(row["max_offset"] + 1))
        admin.alterConsumerGroupOffsets(group_id, offsets).all().get()
    finally:
        admin.close()


def _write_batch(topic, batch_df, batch_id):
    # ponytail: a new AdminClient per microbatch, fine at this demo's batch
    # rate; reuse a driver-side singleton if this ever becomes a bottleneck.
    (
        batch_df.drop("partition", "offset")
        .writeTo(f"spark_catalog.staging.{topic}")
        .append()
    )
    _commit_offsets(batch_df.sparkSession, f"{CONSUMER_GROUP_PREFIX}-{topic}", topic, batch_df)


def main():
    spark = SparkSession.builder.appName("bronze-stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Namespace must be created through the "iceberg" catalog alias, not
    # spark_catalog: SparkSessionCatalog.createNamespace always delegates to
    # Spark's built-in in-memory catalog, never to the real REST/Glue backend
    # it wraps (confirmed in Iceberg's SparkSessionCatalog.java source) — so
    # CREATE DATABASE against spark_catalog silently never reaches REST/Glue,
    # and the later CREATE TABLE ... USING iceberg fails with
    # NoSuchNamespaceException. See spark-conf/rest.conf.template.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.staging")

    for topic, decode_strings in TOPICS:
        spark.sql(_table_ddl(topic))
        df = build_stream(spark, topic, decode_strings)
        (
            df.writeStream
            .foreachBatch(lambda batch_df, batch_id, topic=topic: _write_batch(topic, batch_df, batch_id))
            .outputMode("append")
            # ponytail: no partitionBy — add partitionBy("year_", "month_", "day_")
            # if data volume grows enough that small-file/scan cost matters.
            .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{topic}")
            .start()
        )

    # Three independent queries; awaitAnyTermination so one query
    # failing/stopping doesn't hide the other two.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()


def _self_check():
    """Minimal runnable check for the mojibake decode (needs pyspark installed,
    since this module imports it at load time). Run inside the built image:
    python3 -c "from stream_events import _self_check; _self_check()"
    """
    mojibake = "Beyonc\u00c3\u00a9"  # UTF-8 bytes for 'é' misread as latin1
    fixed = _decode(mojibake)
    assert fixed == "Beyoncé", f"decode broken: got {fixed!r}"

    ddl = _table_ddl("auth_events")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS staging.auth_events ("), ddl
    assert ddl.endswith("success BOOLEAN, ts_timestamp TIMESTAMP, year_ INT, "
                         "month_ INT, day_ INT, hour_ INT, "
                         "processed_time TIMESTAMP) USING iceberg"), ddl

    print("ok")
