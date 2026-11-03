"""Bronze + silver layers: Kafka -> Delta Lake.

bronze/ais_raw          every Kafka message as-is, plus Kafka metadata (replayable history)
silver/ship_positions   parsed, validated, deduplicated position reports, partitioned by day

Both queries read the Kafka topic directly (each keeps its own offsets in its
checkpoint), so silver doesn't have to wait for bronze to exist.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "ais.raw")
LAKE = os.getenv("LAKE_PATH", "/data/lake")
CHECKPOINTS = os.getenv("CHECKPOINT_PATH", "/data/checkpoints")
TRIGGER = os.getenv("TRIGGER_INTERVAL", "30 seconds")

# Only the fields we use from aisstream.io's PositionReport message
AIS_SCHEMA = T.StructType([
    T.StructField("MessageType", T.StringType()),
    T.StructField("MetaData", T.StructType([
        T.StructField("MMSI", T.LongType()),
        T.StructField("ShipName", T.StringType()),
        T.StructField("latitude", T.DoubleType()),
        T.StructField("longitude", T.DoubleType()),
        T.StructField("time_utc", T.StringType()),
    ])),
    T.StructField("Message", T.StructType([
        T.StructField("PositionReport", T.StructType([
            T.StructField("Sog", T.DoubleType()),
            T.StructField("Cog", T.DoubleType()),
            T.StructField("TrueHeading", T.IntegerType()),
            T.StructField("NavigationalStatus", T.IntegerType()),
        ])),
    ])),
])

# AIS uses sentinel values for "not available"
SOG_NOT_AVAILABLE = 102.3
COG_NOT_AVAILABLE = 360.0
HEADING_NOT_AVAILABLE = 511

# AIS navigational status codes
AT_ANCHOR = 1
MOORED = 5
# below this a ship is stopped; anchored ships still drift and swing a little
STOPPED_MAX_SOG_KNOTS = 1.0


def activity(sog, nav_status):
    """moving / berthed / waiting / unknown, from speed first and the crew-set status second.

    Speed wins because crews often forget to update the status. A ship that is
    tied up but still says "under way" comes out as waiting: a known gap that
    would need exact berth locations to fix. "waiting" only means congestion
    near a port; the gold job does that geofencing.
    """
    return (
        F.when(sog >= STOPPED_MAX_SOG_KNOTS, "moving")
        .when(nav_status == MOORED, "berthed")
        .when(sog.isNotNull() | (nav_status == AT_ANCHOR), "waiting")
        .otherwise("unknown")
    )


def read_kafka(spark):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        # don't crash if retention deleted offsets we haven't read yet
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 200_000)
        .load()
    )


def to_bronze(kafka_df):
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("payload"),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingested_at"),
    )


def to_silver(kafka_df):
    msg = F.from_json(F.col("value").cast("string"), AIS_SCHEMA).alias("m")
    report = "m.Message.PositionReport"
    sog, cog, heading = F.col(f"{report}.Sog"), F.col(f"{report}.Cog"), F.col(f"{report}.TrueHeading")
    return (
        kafka_df.select(msg, F.col("timestamp").alias("kafka_ts"))
        .where(F.col("m.MessageType") == "PositionReport")
        .select(
            F.col("m.MetaData.MMSI").alias("mmsi"),
            F.trim("m.MetaData.ShipName").alias("ship_name"),
            F.col("m.MetaData.latitude").alias("lat"),
            F.col("m.MetaData.longitude").alias("lon"),
            F.when(sog < SOG_NOT_AVAILABLE, sog).alias("sog_knots"),
            F.when(cog < COG_NOT_AVAILABLE, cog).alias("cog_deg"),
            F.when(heading != HEADING_NOT_AVAILABLE, heading).alias("heading_deg"),
            F.col(f"{report}.NavigationalStatus").alias("nav_status"),
            # time_utc looks like "2026-10-28 09:15:02.123456789 +0000 UTC"; keep it to the second
            F.to_timestamp(
                F.regexp_extract("m.MetaData.time_utc", r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", 1)
            ).alias("event_time"),
            "kafka_ts",
        )
        .where(
            F.col("mmsi").isNotNull()
            & F.col("event_time").isNotNull()
            & F.col("lat").between(-90, 90)
            & F.col("lon").between(-180, 180)
        )
        .withColumn("activity", activity(F.col("sog_knots"), F.col("nav_status")))
        .withColumn("event_date", F.to_date("event_time"))
        # the same report can arrive twice (reconnects, overlapping receivers);
        # remember keys for 10 minutes of event time to drop the repeats
        .withWatermark("event_time", "10 minutes")
        .dropDuplicatesWithinWatermark(["mmsi", "event_time"])
    )


def main():
    spark = SparkSession.builder.appName("ais-positions").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = read_kafka(spark)

    (
        to_bronze(kafka_df).writeStream.format("delta")
        .queryName("bronze_ais_raw")
        .option("checkpointLocation", f"{CHECKPOINTS}/bronze_ais_raw")
        .trigger(processingTime=TRIGGER)
        .start(f"{LAKE}/bronze/ais_raw")
    )

    (
        to_silver(kafka_df).writeStream.format("delta")
        .queryName("silver_ship_positions")
        .option("checkpointLocation", f"{CHECKPOINTS}/silver_ship_positions")
        .partitionBy("event_date")
        .trigger(processingTime=TRIGGER)
        .start(f"{LAKE}/silver/ship_positions")
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
