from pyspark.sql.types import *
from pyspark.sql.functions import *

# Simulating JSON data ingestion instead of Kafka

# Define sample JSON file path (Upload this JSON file manually to DBFS)
json_file_path = "dbfs:/FileStore/customer_updates.json"

# Bronze Layer: Simulating Raw data ingestion from JSON instead of Kafka
bronze_df = (
    spark.read.json(json_file_path)
    .selectExpr("to_json(struct(*)) as json_payload", "current_timestamp() as timestamp")
)

# Write bronze table
(
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/Volumes/checkpoints/bronze_customers")
    .trigger(availableNow=True)
    .start("bronze_customers")
)

# Silver Layer: Parse and merge incrementally
customer_schema = StructType([
    StructField("customerId", StringType(), True),
    StructField("customerName", StringType(), True),
    StructField("isActive", BooleanType(), True),
    StructField("eventTimestamp", TimestampType(), True)
])

bronze_stream_df = spark.readStream.table("bronze_customers")

silver_parsed_df = (
    bronze_stream_df.select(from_json(col("json_payload"), customer_schema).alias("data"))
    .select("data.*")
)

# Explicitly create silver table once
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_customers (
        customerId STRING,
        customerName STRING,
        isActive BOOLEAN,
        eventTimestamp TIMESTAMP
    ) USING DELTA
""")

def upsert_to_silver(microBatchDF, batchId):
    microBatchDF.createOrReplaceTempView("updates")

    spark.sql("""
        MERGE INTO silver_customers tgt
        USING updates src
        ON tgt.customerId = src.customerId
        WHEN MATCHED AND src.eventTimestamp > tgt.eventTimestamp THEN UPDATE SET
          tgt.customerName = coalesce(src.customerName, tgt.customerName),
          tgt.isActive = coalesce(src.isActive, tgt.isActive),
          tgt.eventTimestamp = src.eventTimestamp
        WHEN NOT MATCHED THEN
          INSERT *
    """)

# Apply merge operation to Silver
(
    silver_parsed_df.writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", "dbfs:/Volumes/checkpoints/silver_customers")
    .trigger(availableNow=True)
    .start()
)

# Gold Layer: Business-ready (Active Customers Example)
silver_stream_df = spark.readStream.table("silver_customers")

gold_df = silver_stream_df.filter(col("isActive") == True)

# Write to Gold table
(
    gold_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/Volumes/checkpoints/gold_customers")
    .trigger(availableNow=True)
    .start("gold_customers")
)
