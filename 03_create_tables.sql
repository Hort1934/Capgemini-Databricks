-- Create the database to store the tables
CREATE DATABASE IF NOT EXISTS nyc_airbnb;

-- Create the Bronze table to store raw data
CREATE TABLE IF NOT EXISTS nyc_airbnb.bronze
USING DELTA
LOCATION '/mnt/nyc_airbnb/bronze_table';

-- Create the Silver table to store transformed data
CREATE TABLE IF NOT EXISTS nyc_airbnb.silver
USING DELTA
LOCATION '/mnt/nyc_airbnb/silver_table';

-- Add data quality constraints to the Silver table
ALTER TABLE nyc_airbnb.silver ADD CONSTRAINT price_positive CHECK (price > 0);
ALTER TABLE nyc_airbnb.silver ADD CONSTRAINT minimum_nights_not_null CHECK (minimum_nights IS NOT NULL);
ALTER TABLE nyc_airbnb.silver ADD CONSTRAINT availability_365_not_null CHECK (availability_365 IS NOT NULL);

-- Verify the constraints
SHOW CONSTRAINTS nyc_airbnb.silver;
