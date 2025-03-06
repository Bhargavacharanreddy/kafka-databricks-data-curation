from pyspark.sql.types import *
from pyspark.sql.functions import *

# Kafka configuration
kafka_bootstrap_servers = "<YOUR_BOOTSTRAP_SERVERS>" # e.g. pkc-xyz.region.cloud.confluent.cloud:9092
kafka_topic_name = "customer_updates"
kafka_api_key = "<YOUR_API_KEY>"
kafka_api_secret = "<YOUR_API_SECRET>"

# Bronze Layer: Raw data ingestion from Kafka
bronze_df = (
  spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
  .option("subscribe", kafka_topic_name)
  .option("startingOffsets", "earliest")
  .option("kafka.security.protocol","SASL_SSL")
  .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_api_key}' password='{kafka_api_secret}';")
  .option("kafka.sasl.mechanism","PLAIN")
  .load()
  .selectExpr("CAST(value AS STRING) as json_payload", "timestamp")
)

# Write bronze table
(
  bronze_df.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/tmp/checkpoints/bronze_customers")
  .table("bronze_customers")
)

# Silver Layer: Parse and merge incrementally
customer_schema = StructType([
    StructField("customerId", StringType(), True),
    StructField("customerName", StringType(), True),
    StructField("isActive", BooleanType(), True),
    StructField("eventTimestamp", TimestampType(), True)
    # Add additional fields as necessary
])

bronze_stream_df = spark.readStream.table("bronze_customers")

silver_parsed_df = bronze_stream_df.select(from_json(col("json_payload"), customer_schema).alias("data"))\
                                  .select("data.*")

def upsert_to_silver(microBatchDF, batchId):
    microBatchDF.createOrReplaceTempView("updates")

    # Create silver table initially (run once)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver_customers (
            customerId STRING,
            customerName STRING,
            isActive BOOLEAN,
            eventTimestamp TIMESTAMP
        ) USING DELTA
    """)

    # MERGE for incremental upsert
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
  .foreachBatch(lambda df, epochId: upsert_to_silver(df=df, batchId=epoch_id))
  .option("checkpointLocation", "/tmp/checkpoints/silver_customers")
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
  .option("checkpointLocation", "/tmp/checkpoints/gold_customers")
  .table("gold_customers")
)
