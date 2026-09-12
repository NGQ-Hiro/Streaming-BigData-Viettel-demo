"""One-shot Hudi compaction for the bronze (staging.<topic>) MERGE_ON_READ
tables -- merges accumulated delta log files into columnar base files.
Run via spark-submit; not part of the always-on stream_events.py job.
"""
from pyspark.sql import SparkSession

TOPICS = ["listen_events", "page_view_events", "auth_events"]
WAREHOUSE = "s3a://lakehouse"


def main():
    spark = SparkSession.builder.appName("bronze-compact").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    for topic in TOPICS:
        path = f"{WAREHOUSE}/staging/{topic}"
        spark.sql(f"CALL run_compaction(op => 'run', path => '{path}')")


if __name__ == "__main__":
    main()
