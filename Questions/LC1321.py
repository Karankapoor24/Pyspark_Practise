"""
LC1321

You are the restaurant owner and you want to analyze a possible expansion (there will be at least one customer every day).

Compute the moving average of how much the customer paid in a seven days window (i.e., current day + 6 days before).
average_amount should be rounded to two decimal places.

Return the result table ordered by visited_on in ascending order.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerData").getOrCreate()

# Sample data
data = [
    (1, "Jhon", "2019-01-01", 100),
    (2, "Daniel", "2019-01-02", 110),
    (3, "Jade", "2019-01-03", 120),
    (4, "Khaled", "2019-01-04", 130),
    (5, "Winston", "2019-01-05", 110),
    (6, "Elvis", "2019-01-06", 140),
    (7, "Anna", "2019-01-07", 150),
    (8, "Maria", "2019-01-08", 80),
    (9, "Jaze", "2019-01-09", 110),
    (1, "Jhon", "2019-01-10", 130),
    (3, "Jade", "2019-01-10", 150)
]

# Convert date strings to datetime objects
data = [(customer_id, name, datetime.strptime(visited_on, "%Y-%m-%d").date(), amount) for customer_id, name, visited_on, amount in data]

# Define schema
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("visited_on", DateType(), True),
    StructField("amount", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
df.show()

grouped_df = df.groupBy("visited_on").agg(F.sum("amount").alias("amount"))
grouped_df.show()

window_spec = Window.orderBy("visited_on").rowsBetween(-6, Window.currentRow)
moving_avg_df = grouped_df.withColumn("cum_amount", F.sum("amount").over(window_spec))\
                    .withColumn("average_amount", F.round(F.avg("amount").over(window_spec), 2))

moving_avg_df.show()

min_date = moving_avg_df.select(F.min(F.col("visited_on"))).collect()[0][0]
ans_df = moving_avg_df.filter(F.col("visited_on") >= F.date_add(F.lit(min_date), 6))

ans_df.show()

