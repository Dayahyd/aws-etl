from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# Spark session with Kafka support
spark = (SparkSession.builder
.appName("kafka-streaming-app")
.getOrCreate())


# Define schema
schema = StructType([
StructField("user_id", StringType()),
StructField("event_type", StringType()),
StructField("value", IntegerType()),
StructField("event_ts", StringType())
])


# Read stream from Kafka
df = (spark.readStream.format("kafka")
.option("kafka.bootstrap.servers", "kafka:9092")
.option("subscribe", "user_events")
.load())


# Parse value column
value_df = df.selectExpr("CAST(value AS STRING)")
json_df = value_df.select(from_json(col("value"), schema).alias("data")).select("data.*")


# Add ingest timestamp
final_df = json_df.withColumn("ingest_ts", current_timestamp())


# Write to S3 in micro-batches
(output := final_df.writeStream
.format("parquet")
.option("path", "s3://datalake/processed/events/")
.option("checkpointLocation", "s3://datalake/checkpoints/events/")
.partitionBy("event_type")
.outputMode("append")
.start())


output.awaitTermination()
