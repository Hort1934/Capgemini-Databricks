# Import required libraries
from pyspark.sql.functions import col, to_date

# Create Spark Session
spark = SparkSession.builder.appName("NYC Airbnb Silver Table Transformation").getOrCreate()

# Load data from Bronze Delta table
bronze_table_path = "/mnt/nyc_airbnb/bronze_table"
bronze_df = spark.read.format("delta").load(bronze_table_path)

# Data Cleaning and Transformation
silver_df = (bronze_df
             .filter("price > 0")  # Remove rows with non-positive prices
             .withColumn("last_review", to_date(col("last_review"), "yyyy-MM-dd"))  # Convert last_review to date
             .fillna({"last_review": "1900-01-01", "reviews_per_month": 0})  # Fill missing values
             .dropna(subset=["latitude", "longitude"]))  # Drop rows with missing lat/lon

# Write transformed data to Silver Delta Table
silver_table_path = "/mnt/nyc_airbnb/silver_table"
silver_df.write.format("delta").mode("overwrite").save(silver_table_path)

print("Transformation to Silver Table completed.")
