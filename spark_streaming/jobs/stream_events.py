"""
Spark Structured Streaming job: Kafka -> latin1-decode + derived time columns
-> Hudi bronze (staging.<topic>, MERGE_ON_READ tables under s3a://lakehouse).
Replaces the old Flink SQL job; keeps the exact same bronze column names
(ts_timestamp, year_, month_, day_, hour_, processed_time) since the
separate dbt-spark batch job depends on this schema.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

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

_WAREHOUSE = "s3a://lakehouse"

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
        .withColumn("_row_key", F.expr("uuid()"))
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


def _hudi_options(topic):
    return {
        "hoodie.table.name": topic,
        "hoodie.datasource.write.table.type": "MERGE_ON_READ",
        "hoodie.datasource.write.recordkey.field": "_row_key",
        "hoodie.datasource.write.precombine.field": "processed_time",
        "hoodie.datasource.write.operation": "insert",
        "hoodie.datasource.write.hive_style_partitioning": "true",
    }


def _write_batch(topic, batch_df, batch_id):
    # ponytail: a new AdminClient per microbatch, fine at this demo's batch
    # rate; reuse a driver-side singleton if this ever becomes a bottleneck.
    path = f"{_WAREHOUSE}/staging/{topic}"
    (
        batch_df.drop("partition", "offset")
        .write.format("hudi")
        .options(**_hudi_options(topic))
        .mode("append")
        .save(path)
    )
    # Hudi's DataSource writer auto-creates the table at `path` on first
    # write (unlike Iceberg, which needed an explicit CREATE TABLE before
    # any data existed) -- this just makes it resolvable by name
    # (staging.<topic>) for dbt/Superset over the Thrift Server. Schema is
    # inferred from the Hudi table's own metadata at `path`, so this only
    # succeeds once data exists there -- safe to repeat every batch since
    # by now the .save() above has already run at least once.
    spark = batch_df.sparkSession
    spark.sql(f"CREATE TABLE IF NOT EXISTS staging.{topic} USING hudi LOCATION '{path}'")
    _commit_offsets(spark, f"{CONSUMER_GROUP_PREFIX}-{topic}", topic, batch_df)


def main():
    # enableHiveSupport: without it, the CREATE TABLE below (staging.<topic>)
    # lands in a private in-memory catalog only this JVM can see -- spark-thrift
    # (hive-site.xml, same shared postgres metastore) wouldn't find the table.
    spark = SparkSession.builder.appName("bronze-stream").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE DATABASE IF NOT EXISTS staging")

    # Warm-up: on a fresh metastore, DataNucleus lazily creates several
    # metastore tables (TBLS, TBL_PRIVS, its FK constraints, ...) on the
    # FIRST CREATE TABLE it ever sees -- CREATE DATABASE above doesn't touch
    # that code path. The 3 streaming queries below each fire their own
    # CREATE TABLE concurrently (one per stream-execution thread) once
    # batches start landing, and racing that lazy DDL from multiple threads
    # deadlocked in postgres (concurrent ALTER TABLE ADD CONSTRAINT). Doing
    # one harmless CREATE+DROP here, single-threaded, forces that one-time
    # DDL to happen before any concurrent writer exists.
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS staging.__metastore_init (id INT) "
        f"USING parquet LOCATION '{_WAREHOUSE}/staging/__metastore_init'"
    )
    spark.sql("DROP TABLE IF EXISTS staging.__metastore_init")

    for topic, decode_strings in TOPICS:
        df = build_stream(spark, topic, decode_strings)
        (
            df.writeStream
            .queryName(topic)
            .foreachBatch(lambda batch_df, batch_id, topic=topic: _write_batch(topic, batch_df, batch_id))
            .outputMode("append")
            # ponytail: no partitioning at this data volume -- add
            # "hoodie.datasource.write.partitionpath.field": "year_,month_,day_"
            # to _hudi_options() if small-file/scan cost ever matters.
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

    opts = _hudi_options("auth_events")
    assert opts["hoodie.table.name"] == "auth_events", opts
    assert opts["hoodie.datasource.write.recordkey.field"] == "_row_key", opts
    assert opts["hoodie.datasource.write.precombine.field"] == "processed_time", opts

    print("ok")
