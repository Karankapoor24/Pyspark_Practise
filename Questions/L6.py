"""
Find the total number of available beds per hosts' nationality.
Output the nationality along with the corresponding total number of available beds.
Sort records by the total available beds in descending order.

Output

+-----------+-------------+
|nationality|total_bed_cnt|
+-----------+-------------+
|      China|            8|
|        USA|            7|
|       Mali|            5|
+-----------+-------------+

"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder.appName("AirbnbData").getOrCreate()

# Define schema for airbnb_apartments
apartments_schema = StructType([
    StructField("host_id", IntegerType(), True),
    StructField("apartment_id", StringType(), True),
    StructField("apartment_type", StringType(), True),
    StructField("n_beds", IntegerType(), True),
    StructField("n_bedrooms", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True)
])

# Create DataFrame for airbnb_apartments
apartments_data = [
    (0, 'A1', 'Room', 1, 1, 'USA', 'NewYork'),
    (0, 'A2', 'Room', 1, 1, 'USA', 'NewJersey'),
    (0, 'A3', 'Room', 1, 1, 'USA', 'NewJersey'),
    (1, 'A4', 'Apartment', 2, 1, 'USA', 'Houston'),
    (1, 'A5', 'Apartment', 2, 1, 'USA', 'LasVegas'),
    (3, 'A7', 'Penthouse', 3, 3, 'China', 'Tianjin'),
    (3, 'A8', 'Penthouse', 5, 5, 'China', 'Beijing'),
    (4, 'A9', 'Apartment', 2, 1, 'Mali', 'Bamako'),
    (5, 'A10', 'Room', 3, 1, 'Mali', 'Segou')
]

apartments_df = spark.createDataFrame(apartments_data, schema=apartments_schema)
apartments_df.show()

# Define schema for airbnb_hosts
hosts_schema = StructType([
    StructField("host_id", IntegerType(), True),
    StructField("nationality", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Create DataFrame for airbnb_hosts
hosts_data = [
    (0, 'USA', 'M', 28),
    (1, 'USA', 'F', 29),
    (2, 'China', 'F', 31),
    (3, 'China', 'M', 24),
    (4, 'Mali', 'M', 30),
    (5, 'Mali', 'F', 30)
]

hosts_df = spark.createDataFrame(hosts_data, schema=hosts_schema)
hosts_df.show()

joined_df = hosts_df.join(apartments_df, hosts_df["host_id"] == apartments_df["host_id"])

ans_df = joined_df.groupby("nationality").agg(F.sum("n_beds").alias("total_bed_cnt"))\
         .orderBy(F.desc("total_bed_cnt"))

ans_df.show()
