"""
Main Spark Structured Streaming pipeline.
Run as a Databricks Workflow job — not on EC2.

Flow: Kafka (stb-vehicles) → Bronze Delta → Silver (sessions) → Gold (baseline + ETA)
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from schema import BRONZE_SCHEMA

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC      = "stb-vehicles"
BRONZE_PATH      = os.getenv("BRONZE_PATH", "/tmp/bus381/bronze")
SILVER_PATH      = os.getenv("SILVER_PATH", "/tmp/bus381/silver")
GOLD_PATH        = os.getenv("GOLD_PATH",   "/tmp/bus381/gold")
CHECKPOINT_BASE  = os.getenv("CHECKPOINT_PATH", "/tmp/bus381/checkpoints")

spark = SparkSession.builder.appName("bus381-pipeline").getOrCreate()
spark.conf.set("spark.sql.adaptive.enabled", "true")


def start_bronze():
    raw = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .load()
    )
    parsed = (
        raw.select(from_json(col("value").cast("string"), BRONZE_SCHEMA).alias("d"), col("timestamp"))
           .select("d.*", col("timestamp").alias("event_time"))
           .withColumn("ingested_at", current_timestamp())
    )
    return (
        parsed.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze")
            .start(BRONZE_PATH)
    )


def start_silver():
    from sessionizer import Sessionizer
    bronze = spark.readStream.format("delta").load(BRONZE_PATH)
    return Sessionizer(spark, bronze, SILVER_PATH, f"{CHECKPOINT_BASE}/silver").start()


def start_gold():
    from pyspark.sql.functions import avg, stddev, count, hour, dayofweek
    silver = spark.readStream.format("delta").load(SILVER_PATH)
    gold = (
        silver.filter(col("is_complete") == True)
              .withColumn("hour_of_day",  hour(col("run_start_ts")))
              .withColumn("day_of_week",  dayofweek(col("run_start_ts")))
              .groupBy("hour_of_day", "day_of_week")
              .agg(
                  avg("duration_seconds").alias("baseline_duration_s"),
                  stddev("duration_seconds").alias("baseline_stddev_s"),
                  count("*").alias("sample_count"),
              )
              .withColumn("updated_at", current_timestamp())
    )
    return (
        gold.writeStream
            .format("delta")
            .outputMode("complete")
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/gold")
            .start(GOLD_PATH)
    )


if __name__ == "__main__":
    q_bronze = start_bronze()
    q_silver = start_silver()
    q_gold   = start_gold()
    spark.streams.awaitAnyTermination()
