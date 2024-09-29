# Delta Lake Configuration Guide

## 1. Bronze Table Configuration
- File Format: Delta Lake
- Storage Location: `/mnt/nyc_airbnb/bronze_table`
- Purpose: Store raw, untransformed data ingested from CSV files.

## 2. Silver Table Configuration
- File Format: Delta Lake
- Storage Location: `/mnt/nyc_airbnb/silver_table`
- Purpose: Store cleaned and transformed data.
- Constraints: 
  - `price_positive`: Ensures price is greater than 0.
  - `minimum_nights_not_null`: Ensures minimum_nights is not null.
  - `availability_365_not_null`: Ensures availability_365 is not null.

## 3. Streaming Configuration
- Use `Databricks Auto Loader` to ingest new files added to the input directory (`/mnt/nyc_airbnb/raw_data`).
- Use `Structured Streaming` to propagate changes from the Bronze table to the Silver table.
