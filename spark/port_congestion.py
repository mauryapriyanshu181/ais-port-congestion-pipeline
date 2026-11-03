"""Gold layer: how many ships are around each port, and what they are doing.

Streams silver positions, geofences them against the ports in ports.json, and
counts ships per port per time window by activity (set in silver):
waiting = stopped but not moored, which near a port usually means at anchor
waiting for a berth; berthed = moored; moving = 1 knot or faster.

Each ship is counted once per window, by its last report in that window, so the
numbers read as "at the end of this window, N ships were waiting".

Windows are written once the watermark passes them (append mode), so the first
rows show up roughly WINDOW + WATERMARK after data starts flowing.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LAKE = os.getenv("LAKE_PATH", "/data/lake")
CHECKPOINTS = os.getenv("CHECKPOINT_PATH", "/data/checkpoints")
PORTS_FILE = os.getenv("PORTS_FILE", "/config/ports.json")
# anchored ships report only every 3 minutes, so windows must be well above that
WINDOW = os.getenv("CONGESTION_WINDOW", "15 minutes")
WATERMARK = os.getenv("CONGESTION_WATERMARK", "5 minutes")
EARTH_RADIUS_KM = 6371.0

SILVER = f"{LAKE}/silver/ship_positions"
GOLD = f"{LAKE}/gold/port_congestion"


def haversine_km(lat1, lon1, lat2, lon2):
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(dlon / 2), 2)
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(a))


def count_where(condition):
    return F.sum(F.when(condition, 1).otherwise(0))


def main():
    spark = SparkSession.builder.appName("ais-port-congestion").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ports = spark.read.option("multiLine", True).json(PORTS_FILE).select(
        F.col("name").alias("port"),
        "country",
        F.col("lat").alias("port_lat"),
        F.col("lon").alias("port_lon"),
        "radius_km",
    )

    positions = spark.readStream.format("delta").load(SILVER).withWatermark("event_time", WATERMARK)

    # ports is tiny, so broadcast it and test every position against every port
    near_port = positions.join(
        F.broadcast(ports),
        haversine_km(F.col("lat"), F.col("lon"), F.col("port_lat"), F.col("port_lon")) <= F.col("radius_km"),
    )

    # step 1: one row per ship per window, keeping the ship's last report
    per_ship = (
        near_port.groupBy(F.window("event_time", WINDOW), "port", "country", "mmsi")
        .agg(
            F.max_by("activity", "event_time").alias("activity"),
            F.max_by("sog_knots", "event_time").alias("sog_knots"),
            F.count("*").alias("reports"),
        )
    )

    # step 2: count those ships per port; window_time() puts each row back in
    # the same window, which lets Spark chain the two windowed aggregations
    congestion = (
        per_ship.groupBy(F.window(F.window_time("window"), WINDOW), "port", "country")
        .agg(
            F.count("*").alias("ships_in_area"),
            count_where(F.col("activity") == "waiting").alias("ships_waiting"),
            count_where(F.col("activity") == "berthed").alias("ships_berthed"),
            count_where(F.col("activity") == "moving").alias("ships_moving"),
            F.round(F.avg("sog_knots"), 2).alias("avg_sog_knots"),
            F.sum("reports").alias("position_reports"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "port",
            "country",
            "ships_in_area",
            "ships_waiting",
            "ships_berthed",
            "ships_moving",
            "avg_sog_knots",
            "position_reports",
        )
    )

    query = (
        congestion.writeStream.format("delta")
        .queryName("gold_port_congestion")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINTS}/gold_port_congestion")
        .trigger(processingTime="1 minute")
        .start(GOLD)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
