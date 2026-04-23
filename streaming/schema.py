from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType, TimestampType, ArrayType
)

BRONZE_SCHEMA = StructType([
    StructField("vehicle_id",      StringType(),    True),
    StructField("license_plate",   StringType(),    True),
    StructField("latitude",        DoubleType(),    True),
    StructField("longitude",       DoubleType(),    True),
    StructField("line_id",         IntegerType(),   True),
    StructField("direction_id",    IntegerType(),   True),
    StructField("passenger_count", IntegerType(),   True),
    StructField("ingested_at",     TimestampType(), True),
    StructField("event_time",      TimestampType(), True),
])

SILVER_SCHEMA = StructType([
    StructField("session_id",        StringType(),            True),
    StructField("vehicle_id",        StringType(),            True),
    StructField("run_start_ts",      TimestampType(),         True),
    StructField("run_end_ts",        TimestampType(),         True),
    StructField("duration_seconds",  IntegerType(),           True),
    StructField("stop_sequence",     ArrayType(IntegerType()), True),
    StructField("pace_kmh",          DoubleType(),            True),
    StructField("deviation_seconds", IntegerType(),           True),
    StructField("is_complete",       BooleanType(),           True),
])

GOLD_SCHEMA = StructType([
    StructField("hour_of_day",          IntegerType(), True),
    StructField("day_of_week",          IntegerType(), True),
    StructField("baseline_duration_s",  DoubleType(),  True),
    StructField("baseline_stddev_s",    DoubleType(),  True),
    StructField("sample_count",         IntegerType(), True),
    StructField("eta_next_arrival",     TimestampType(), True),
    StructField("confidence_low_s",     IntegerType(), True),
    StructField("confidence_high_s",    IntegerType(), True),
    StructField("updated_at",           TimestampType(), True),
])
