"""
One-off bulk load of dbt/my_project/seeds/songs.csv (10k rows) straight into
Iceberg via a real spark-submit job on the cluster. dbt seed compiles each
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
spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.staging")

df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)
df.writeTo("spark_catalog.staging.songs").createOrReplace()

print(f"loaded {df.count()} rows into staging.songs")
