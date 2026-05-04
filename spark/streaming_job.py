import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, sum as _sum,
    count, avg, when, lit, from_unixtime
)
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, IntegerType, DoubleType
)

JARS = ",".join([
    "jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    "jars/kafka-clients-3.4.0.jar",
    "jars/postgresql-42.6.0.jar",
    "jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    "jars/commons-pool2-2.11.1.jar",
])

spark = (SparkSession.builder
    .appName("LLMCostStreaming")
    .master("local[*]")
    .config("spark.jars", JARS)
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

print("Spark session started. Reading from Kafka...")

schema = StructType([
    StructField("event_ts",      LongType()),
    StructField("team",          StringType()),
    StructField("user_id",       StringType()),
    StructField("feature",       StringType()),
    StructField("model",         StringType()),
    StructField("provider",      StringType()),
    StructField("input_tokens",  IntegerType()),
    StructField("output_tokens", IntegerType()),
    StructField("cost_usd",      DoubleType()),
    StructField("latency_ms",    DoubleType()),
    StructField("request_id",    StringType()),
])

raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "llm.inference.raw")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("d"))
    .select("d.*")
    .withColumn("event_at",
        from_unixtime(col("event_ts") / 1000).cast("timestamp")))

POSTGRES_URL  = "jdbc:postgresql://pg-dd4835d-kelly-analysis-254.g.aivencloud.com:12538/defaultdb?ssl=true&sslmode=require"
POSTGRES_PROPS = {
    "user":     "user",
    "password": "password",
    "driver":   "org.postgresql.Driver",
    "ssl":      "true",
    "sslmode":  "require",
}

def write_events(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    print(f"[Batch {batch_id}] Writing {batch_df.count()} raw events...")
    (batch_df.select(
        "event_ts", "event_at", "team", "user_id", "feature",
        "model", "provider", "input_tokens", "output_tokens",
        "cost_usd", "latency_ms", "request_id")
    .write.jdbc(
        url=POSTGRES_URL,
        table="inference_events",
        mode="append",
        properties=POSTGRES_PROPS,
    ))

def write_windows(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    print(f"[Batch {batch_id}] Writing {batch_df.count()} window aggregations...")
    (batch_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "team", "model", "call_count",
        "total_cost_usd", "avg_latency_ms",
        "input_tokens", "output_tokens", "anomaly_flag")
    .write.jdbc(
        url=POSTGRES_URL,
        table="cost_by_team_minute",
        mode="append",
        properties=POSTGRES_PROPS,
    ))

raw_query = (raw.writeStream
    .foreachBatch(write_events)
    .option("checkpointLocation", "/tmp/checkpoints/raw_events")
    .trigger(processingTime="30 seconds")
    .start())

windowed = (raw
    .withWatermark("event_at", "2 minutes")
    .groupBy(window("event_at", "1 minute"), "team", "model")
    .agg(
        count("*").alias("call_count"),
        _sum("cost_usd").alias("total_cost_usd"),
        avg("latency_ms").alias("avg_latency_ms"),
        _sum("input_tokens").alias("input_tokens"),
        _sum("output_tokens").alias("output_tokens"),
    )
    .withColumn("anomaly_flag",
        when(col("total_cost_usd") > 0.01, lit("HIGH_COST"))
        .otherwise(lit(None))))

window_query = (windowed.writeStream
    .foreachBatch(write_windows)
    .option("checkpointLocation", "/tmp/checkpoints/cost_windows")
    .trigger(processingTime="30 seconds")
    .outputMode("append")
    .start())

print("Streaming queries running. Waiting for data...")
print("Press Ctrl+C to stop.\n")

spark.streams.awaitAnyTermination(timeout=180000)
