# kafka-databricks-data-curation
Processing external kafka topic messages, curating and updating them using Databricks medallion architecture


# Databricks Medallion Architecture with Kafka and JSON Simulation

## Overview

This repository provides a simple **Databricks Medallion Architecture pipeline** using **Kafka** as the data source and **Delta Lake** for data curation. If you want to explore how to ingest streaming data from **Kafka into Databricks** and apply **Medallion Architecture (Bronze, Silver, Gold layers)** for data transformation, this repository will be helpful.

Additionally, if you don’t have access to Kafka, you can use the **JSON-based ingestion pipeline** to simulate the data flow.

## Files in This Repository

### 1. `kafka-databricks-pipeline.py`
- This script connects **Databricks to Kafka**.
- It reads messages from a **Kafka topic (`customer_updates`)**.
- It writes the data into a **Bronze Delta Table** and further refines it into **Silver and Gold layers**.
- Uses **Delta Lake** for data storage and **structured streaming** for processing.

### 2. `databricks-pipeline.py`
- This script **simulates data ingestion using a JSON file instead of Kafka**.
- The JSON file (`customer_updates.json`) is used as the source.
- Similar to the Kafka pipeline, it follows the **Medallion Architecture** and writes data to **Bronze, Silver, and Gold tables**.

### 3. `customer_updates.json`
- A simple JSON file containing **sample customer update events**.
- Used by `databricks-pipeline.py` to mimic a **real-time data stream**.

## Medallion Architecture Breakdown

1. **Bronze Layer:**  
   - Raw ingestion from Kafka (`kafka-databricks-pipeline.py`) or JSON (`databricks-pipeline.py`).
   - Data is stored in a Delta Table (`bronze_customers`) as is.

2. **Silver Layer:**  
   - Data is **parsed, cleaned, and structured**.
   - Uses **Delta Lake MERGE** for **incremental updates**.
   - Stored in `silver_customers` table.

3. **Gold Layer:**  
   - Business-ready **aggregations and filtering**.
   - Example: Extracting only **active customers** (`isActive=True`).
   - Stored in `gold_customers` table.

## How to Use

### **Option 1: Using Kafka (Real Streaming)**
1. **Ensure Kafka is running** and messages are being published to `customer_updates` topic.
2. Update `kafka-databricks-pipeline.py` with your **Kafka bootstrap servers, API key, and secret**.
3. Run the script in **Databricks** to stream data into **Delta Tables**.

### **Option 2: Using JSON (Simulation)**
1. **Upload `customer_updates.json`** to **DBFS (`dbfs:/FileStore/customer_updates.json`)**.
2. Run `databricks-pipeline.py` in Databricks.
3. This will **simulate the ingestion process** and transform the data.

## Prerequisites
- Databricks Workspace (Community/Standard/Serverless)
- Apache Kafka (for `kafka-databricks-pipeline.py`)
- AWS S3 (for external storage, optional)
- Delta Lake Enabled in Databricks

## Additional Notes
- If using **Databricks Serverless**, ensure you have the correct permissions to access storage locations.
- If you face issues with **Kafka integration**, try running the JSON-based pipeline first.

## Conclusion
This repo provides a **simple yet effective demonstration** of using **Kafka and Delta Lake in Databricks** following the **Medallion Architecture**. Feel free to experiment and extend the pipelines for your own **streaming analytics and data engineering use cases**.

Happy Learning! 
