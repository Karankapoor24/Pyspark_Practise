"""
LC 1934
The confirmation rate of a user is the number of 'confirmed' messages divided by the total number of requested confirmation messages.
The confirmation rate of a user that did not request any confirmation messages is 0. Round the confirmation rate to two decimal places.

Write a solution to find the confirmation rate of each user.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import to_timestamp, when, col, sum as _sum, count as _count

# Initialize Spark session
spark = SparkSession.builder.appName("SignupsAndConfirmations").getOrCreate()

# Define schemas (keeping timestamps as StringType initially)
signups_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("time_stamp", StringType(), True)
])

confirmations_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("time_stamp", StringType(), True),
    StructField("action", StringType(), True)
])

# Create data
signups_data = [
    (3, "2020-03-21 10:16:13"),
    (7, "2020-01-04 13:57:59"),
    (2, "2020-07-29 23:09:44"),
    (6, "2020-12-09 10:39:37")
]

confirmations_data = [
    (3, "2021-01-06 03:30:46", "timeout"),
    (3, "2021-07-14 14:00:00", "timeout"),
    (7, "2021-06-12 11:57:29", "confirmed"),
    (7, "2021-06-13 12:58:28", "confirmed"),
    (7, "2021-06-14 13:59:27", "confirmed"),
    (2, "2021-01-22 00:00:00", "confirmed"),
    (2, "2021-02-28 23:59:59", "timeout")
]

# Create DataFrames
signups_df = spark.createDataFrame(signups_data, schema=signups_schema)
confirmations_df = spark.createDataFrame(confirmations_data, schema=confirmations_schema)

# Convert string timestamps to actual TimestampType
signups_df = signups_df.withColumn("time_stamp", to_timestamp("time_stamp", "yyyy-MM-dd HH:mm:ss"))
confirmations_df = confirmations_df.withColumn("time_stamp", to_timestamp("time_stamp", "yyyy-MM-dd HH:mm:ss"))

# Show DataFrames
signups_df.show()
confirmations_df.show()

joined_df = signups_df.join(confirmations_df, on="user_id", how="left")
joined_df.show()

ans_df = joined_df.withColumn("pos", when(col("action") == "confirmed", 1)
                                      .otherwise(0))\
          .groupBy("user_id").agg((_sum(col("pos")) / _count(col("user_id"))).alias("confirmation_rate"))
ans_df.show()