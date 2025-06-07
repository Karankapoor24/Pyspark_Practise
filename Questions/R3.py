"""
Write a code in SQL *without using windows function* to find out the 2nd level joiners
with initial referrer src_id who still maintain an unbroken chain.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameExample").getOrCreate()

# Define schema
schema = StructType([
    StructField("src_id", StringType(), True),
    StructField("referred_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("event", StringType(), True)
])

# Sample data with timestamp conversion
raw_data = [
    ("A", "B", "2024-01-01 10:00", "add"),
    ("A", "C", "2024-01-01 10:05", "add"),
    ("A", "C", "2024-01-02 09:00", "remove"),
    ("B", "D", "2024-01-01 11:00", "add"),
    ("B", "E", "2024-01-02 13:00", "remove"),
    ("C", "F", "2024-01-01 12:00", "add"),
    ("C", "G", "2024-01-01 13:00", "add"),
    ("C", "G", "2024-01-03 09:00", "remove")
]

# Convert string timestamps to datetime objects
data = [(src, ref, datetime.strptime(ts, "%Y-%m-%d %H:%M"), event) for src, ref, ts, event in raw_data]

# Convert to DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
df.show()
