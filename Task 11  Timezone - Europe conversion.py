# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_utc_timestamp, from_utc_timestamp, lit, dayofyear, when

# Initialize the Spark session
spark = SparkSession.builder.appName("Timezone Conversion").getOrCreate()

# Schema
schema = """
SalesOrderID INT,
SalesOrderDetailID INT,
CarrierTrackingNumber STRING,
OrderQty INT,
ProductID INT,
SpecialOfferID INT,
UnitPrice DOUBLE,
UnitPriceDiscount DOUBLE,
LineTotal DOUBLE,
rowguid STRING,
ModifiedDate STRING
"""

# Load the CSV data
df = spark.read.format("csv").option("header", "true").schema(schema).load("/FileStore/tables/Sales_SalesOrderDetail.csv")

# Convert 'ModifiedDate' to timestamp
df = df.withColumn("ModifiedDate", col("ModifiedDate").cast("timestamp"))

# Static list of European time zones with DST details
time_zone_table = [
    {"zone_id": "Europe/London", "local_time_with_dst": "BST", "local_time_without_dst": "GMT"},
    {"zone_id": "Europe/Berlin", "local_time_with_dst": "CEST", "local_time_without_dst": "CET"},
    {"zone_id": "Europe/Paris", "local_time_with_dst": "CEST", "local_time_without_dst": "CET"},
    {"zone_id": "Europe/Madrid", "local_time_with_dst": "CEST", "local_time_without_dst": "CET"},
    {"zone_id": "Europe/Rome", "local_time_with_dst": "CEST", "local_time_without_dst": "CET"},
    {"zone_id": "Europe/Athens", "local_time_with_dst": "EEST", "local_time_without_dst": "EET"}
]

# Create a DataFrame for the time zones
zone_df = spark.createDataFrame(time_zone_table)

# Define the conversion logic and calculate local time for each timezone
df_with_timezone = df
for zone in time_zone_table:
    zone_id = zone["zone_id"]
    local_time_with_dst = zone["local_time_with_dst"]
    local_time_without_dst = zone["local_time_without_dst"]

    df_with_timezone = df_with_timezone \
        .withColumn(f"Local_Time_With_DST_{zone_id}", from_utc_timestamp(col("ModifiedDate"), zone_id)) \
        .withColumn(f"Local_Time_Without_DST_{zone_id}", from_utc_timestamp(col("ModifiedDate"), zone_id))

# Determine if daylight saving is on or off (assuming DST between day 60 and day 300 of the year for Europe)
df_with_timezone = df_with_timezone \
    .withColumn("Day_Of_Year", dayofyear(col("ModifiedDate"))) \
    .withColumn("Is_DST_On", when((col("Day_Of_Year") >= 60) & (col("Day_Of_Year") <= 300), lit("Yes")).otherwise(lit("No")))

# Show the final DataFrame with local times and DST information
df_with_timezone.show()



# 1. Timezone Conversion with Daylight Saving in Spark
# Objective: Convert timestamps from UTC to various European time zones, taking into account daylight saving time (DST).

# Steps:

# Initialize Spark Session: Set up the Spark environment to process data.
# Load Data: Read sales data from a CSV file into a DataFrame.
# Convert to Timestamp: Convert the ModifiedDate column from string format to timestamp format for accurate time manipulation.
# Create Time Zone Data: Define a static list of European time zones with details on whether they observe daylight saving time.
# Convert Time Zones:
# Use to_utc_timestamp() to convert timestamps to UTC.
# Use from_utc_timestamp() to convert UTC timestamps to local time based on specific time zones.
# Determine Daylight Saving Status:
# Extract the day of the year from the timestamp.
# Determine if DST is in effect based on whether the day of the year falls within a typical DST range.
# Show Results: Display the DataFrame with the additional columns for local time and DST status.
# Key Considerations:

# Time Zones: European time zones may differ in their DST observance and local time designations.
# DST Calculation: The code assumes a simple range for DST; real-world applications may need a more precise calculation based on specific rules.


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
import os

# Initialize the Spark session
spark = SparkSession.builder.appName("Sales Data by Month").getOrCreate()

# Schema
schema = """
SalesOrderID INT,
SalesOrderDetailID INT,
CarrierTrackingNumber STRING,
OrderQty INT,
ProductID INT,
SpecialOfferID INT,
UnitPrice DOUBLE,
UnitPriceDiscount DOUBLE,
LineTotal DOUBLE,
rowguid STRING,
ModifiedDate STRING
"""

# Load the CSV data
df = spark.read.format("csv").option("header", "true").schema(schema).load("/FileStore/tables/Sales_SalesOrderDetail.csv")

# Convert 'ModifiedDate' to timestamp
df = df.withColumn("ModifiedDate", col("ModifiedDate").cast("timestamp"))

# Extract year and month
df = df.withColumn("Year", year(col("ModifiedDate"))).withColumn("Month", month(col("ModifiedDate")))

# Show the dataframe
df.show()

# Loop through the distinct years and months in the data
distinct_years_months = df.select("Year", "Month").distinct().collect()

for row in distinct_years_months:
    year_val = row["Year"]
    month_val = row["Month"]
    
    # Filter data for the specific year and month
    df_filtered = df.filter((col("Year") == year_val) & (col("Month") == month_val))
    
    # Show the filtered data for each month
    print(f"Data for {year_val}-{month_val}")
    df_filtered.show()
    
    # Define the output path
    output_path = f"/mnt/data/sales_data_{year_val}_{month_val}.csv"
    
    # Check if the file already exists
    if os.path.exists(output_path):
        print(f"File {output_path} already exists. Skipping...")
    else:
        # Save each month's data to a new CSV file
        df_filtered.write.mode("overwrite").csv(output_path, header=True)



# 2. Dividing Sales Data by Month in Spark
# Objective: Partition sales data into separate files or tables based on the month of the ModifiedDate column.

# Steps:

# Initialize Spark Session: Create a Spark session for data processing.
# Load Data: Read the sales data from a CSV file, applying a schema to ensure correct data types.
# Convert to Timestamp: Change the ModifiedDate column to a timestamp format for accurate date operations.
# Extract Year and Month:
# Use the year() and month() functions to create new columns for year and month.
# Filter and Save Data:
# Identify distinct years and months in the data.
# Loop through each unique year-month combination, filter the DataFrame, and save the filtered data to a CSV file.
# Handle Existing Files:
# Check if the file already exists before writing.
# Decide whether to overwrite existing files or skip saving if a file with the same name is already present.
