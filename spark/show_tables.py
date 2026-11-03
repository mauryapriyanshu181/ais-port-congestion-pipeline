"""Batch-reads the lake tables and prints row counts and the latest congestion window."""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LAKE = os.getenv("LAKE_PATH", "/data/lake")
TABLES = ["bronze/ais_raw", "silver/ship_positions", "gold/port_congestion"]


def table_exists(path):
    return os.path.isdir(os.path.join(path, "_delta_log"))


def main():
    spark = SparkSession.builder.appName("ais-show-tables").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for name in TABLES:
        path = f"{LAKE}/{name}"
        if table_exists(path):
            print(f"{name}: {spark.read.format('delta').load(path).count():,} rows")
        else:
            print(f"{name}: not created yet")

    silver_path = f"{LAKE}/silver/ship_positions"
    if table_exists(silver_path):
        print("\nLatest silver positions:")
        spark.read.format("delta").load(silver_path).orderBy(F.desc("event_time")).show(5, truncate=False)

    gold_path = f"{LAKE}/gold/port_congestion"
    if table_exists(gold_path):
        gold = spark.read.format("delta").load(gold_path)
        latest = gold.agg(F.max("window_start")).first()[0]
        if latest is not None:
            print(f"\nPort congestion for the window starting {latest}:")
            gold.where(F.col("window_start") == latest).orderBy(F.desc("ships_waiting")).show(truncate=False)


if __name__ == "__main__":
    main()
