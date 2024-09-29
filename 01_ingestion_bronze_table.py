# Import required libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder.appName("NYC Airbnb Bronze Table Ingestion").getOrCreate()

# Define schema for the CSV data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("host_id", IntegerType(), True),
    StructField("host_name", StringType(), True),
    StructField("neighbourhood_group", StringType(), True),
    StructField("neighbourhood", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("room_type", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("minimum_nights", IntegerType(), True),
    StructField("number_of_reviews", IntegerType(), True),
    StructField("last_review", StringType(), True),
    StructField("reviews_per_month", DoubleType(), True),
    StructField("calculated_host_listings_count", IntegerType(), True),
    StructField("availability_365", IntegerType(), True)
])

# Read data from CSV file into a DataFrame
input_path = "/dbfs/FileStore/tables/AB_NYC_2019.csv"
raw_df = spark.read.csv(input_path, header=True, schema=schema)

# Write raw data to Bronze Delta Table
bronze_table_path = "/mnt/nyc_airbnb/bronze_table"
raw_df.write.format("delta").mode("overwrite").save(bronze_table_path)

print("Ingestion to Bronze Table completed.")
