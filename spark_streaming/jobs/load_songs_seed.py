"""
One-off bulk load of dbt/my_project/seeds/songs.csv (10k rows) straight into
Hudi via a real spark-submit job on the cluster. dbt seed compiles each
row into an inline SQL VALUES literal, and Spark's Catalyst analyzer is
near-quadratic on that -- fine for state_codes.csv (58 rows, still a dbt
seed) but never finishes in practical time for 10k rows, on any driver
(local session or cluster thrift server alike -- analysis is driver-side,
not distributed, so more workers don't help). Same target table
(staging.songs) either way, so models using source('staging', 'songs')
don't care which path filled it.

Run via: docker compose run --rm -v "$(pwd)/dbt/my_project/seeds:/seeds" \
  spark-streaming spark-submit --master spark://spark-master:7077 \
  /opt/spark/jobs/load_songs_seed.py /seeds/songs.csv
"""
import sys
from pyspark.sql import SparkSession

csv_path = sys.argv[1] if len(sys.argv) > 1 else "/seeds/songs.csv"

spark = SparkSession.builder.appName("load-songs-seed").getOrCreate()
spark.sql("CREATE DATABASE IF NOT EXISTS staging")

df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)

path = "s3a://lakehouse/warehouse/staging/songs"
(
    df.write.format("hudi")
    .options(**{
        "hoodie.table.name": "songs",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.recordkey.field": "song_id",
        "hoodie.datasource.write.precombine.field": "song_id",
        "hoodie.datasource.write.operation": "insert_overwrite_table",
    })
    .mode("append")
    .save(path)
)
spark.sql(f"CREATE TABLE IF NOT EXISTS staging.songs USING hudi LOCATION '{path}'")

print(f"loaded {df.count()} rows into staging.songs")
