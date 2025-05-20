"""
IBM is working on a new feature to analyze user purchasing behavior for all Fridays in the first quarter of the year.
For each Friday separately, calculate the average amount users have spent per order.
The output should contain the week number of that Friday and average amount spent.

output


"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType
from pyspark.sql.functions import to_date, col, date_format, weekofyear, avg
from pyspark.sql.window import Window
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# Initialize Spark session
spark = SparkSession.builder.appName("UserPurchases").getOrCreate()

# Define schema for user_purchases
purchases_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("date", StringType(), True),  # Start with StringType
    StructField("amount_spent", FloatType(), True),
    StructField("day_name", StringType(), True)
])

# Create DataFrame with raw string date values
purchases_data = [
    (1047, '2023-01-01', 288.0, 'Sunday'),
    (1099, '2023-01-04', 803.0, 'Wednesday'),
    (1055, '2023-01-07', 546.0, 'Saturday'),
    (1040, '2023-01-10', 680.0, 'Tuesday'),
    (1052, '2023-01-13', 889.0, 'Friday'),
    (1052, '2023-01-13', 596.0, 'Friday'),
    (1016, '2023-01-16', 960.0, 'Monday'),
    (1023, '2023-01-17', 861.0, 'Tuesday'),
    (1010, '2023-01-19', 758.0, 'Thursday'),
    (1013, '2023-01-19', 346.0, 'Thursday'),
    (1069, '2023-01-21', 541.0, 'Saturday'),
    (1030, '2023-01-22', 175.0, 'Sunday'),
    (1034, '2023-01-23', 707.0, 'Monday'),
    (1019, '2023-01-25', 253.0, 'Wednesday'),
    (1052, '2023-01-25', 868.0, 'Wednesday'),
    (1095, '2023-01-27', 424.0, 'Friday'),
    (1017, '2023-01-28', 755.0, 'Saturday'),
    (1010, '2023-01-29', 615.0, 'Sunday'),
    (1063, '2023-01-31', 534.0, 'Tuesday'),
    (1019, '2023-02-03', 185.0, 'Friday'),
    (1019, '2023-02-03', 995.0, 'Friday'),
    (1092, '2023-02-06', 796.0, 'Monday'),
    (1058, '2023-02-09', 384.0, 'Thursday'),
    (1055, '2023-02-12', 319.0, 'Sunday'),
    (1090, '2023-02-15', 168.0, 'Wednesday'),
    (1090, '2023-02-18', 146.0, 'Saturday'),
    (1062, '2023-02-21', 193.0, 'Tuesday'),
    (1023, '2023-02-24', 259.0, 'Friday')
]

purchases_df = spark.createDataFrame(purchases_data, schema=purchases_schema)

# Convert date string to proper DateType
purchases_df = purchases_df.withColumn("date", to_date("date", "yyyy-MM-dd"))

purchases_df.show()

filtered_df = purchases_df.filter(date_format(col("date"), "EEEE") == "Friday")

filtered_df.show()

window_spec =  Window.partitionBy("date").orderBy("date")

ans_df = filtered_df.withColumn("week_number", weekofyear(col("date")))\
            .groupby("date", "week_number").agg(avg("amount_spent").alias("average_spent"))

ans_df.show()
