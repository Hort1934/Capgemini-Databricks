# NYC Airbnb Data Pipeline

## Overview
This project builds a data pipeline using Databricks to ingest, transform, and load the New York City Airbnb Open Data into Delta Lake tables. The pipeline supports continuous data ingestion and transformation using Databricks Auto Loader and Structured Streaming.

## Prerequisites
- Databricks account (AWS, Azure, or GCP)
- Databricks cluster with Delta Lake library installed
- NYC Airbnb dataset (`AB_NYC_2019.csv`) uploaded to DBFS

## Setup Instructions

### 1. Setting Up the Databricks Environment
- Create a new Databricks cluster.
- Install the following libraries:
  - `delta`
  - `pyspark`

### 2. Upload the Dataset to DBFS
- Upload the `AB_NYC_2019.csv` dataset to DBFS.
- Note down the file path (e.g., `/dbfs/FileStore/tables/AB_NYC_2019.csv`).

### 3. Create Database and Tables
- Run the SQL script (`03_create_tables.sql`) to create the necessary database and tables.

### 4. Running the Pipeline
- Open the Databricks notebooks or scripts:
  - `01_ingestion_bronze_table.py` for ingestion.
  - `02_transformation_silver_table.py` for transformation.
- Run the cells to ingest raw data and create the Bronze table.
- Execute the transformation script to create the Silver table with cleaned data.

### 5. Configuring Streaming and Monitoring
- Set up Databricks Auto Loader to ingest new files added to the input directory (`/mnt/nyc_airbnb/raw_data`).
- Use Structured Streaming to propagate changes from the Bronze table to the Silver table.

### 6. Error Handling and Time Travel
- Use Delta Lake's Time Travel feature to recover from accidental data deletions or modifications.
- Example command to view table history:
```sql
DESCRIBE HISTORY nyc_airbnb.silver;
