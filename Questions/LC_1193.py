"""

Write an SQL query to find for each month and country, the number of transactions and
their total amount, the number of approved transactions and their total amount.

Return the result table in any order.

The query result format is in the following example.

Output

+-----------+-------+-----------+--------------+------------------+----------------------+
|order_month|country|trans_count|approved_count|trans_total_amount|approved_total_amount |
+-----------+-------+-----------+--------------+------------------+----------------------+
|    2018-12|     US|          2|             1|              3000|                  1000|
|    2019-01|     US|          1|             1|              2000|                  2000|
|    2019-01|     DE|          1|             1|              2000|                  2000|
+-----------+-------+-----------+--------------+------------------+----------------------+

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql import functions as F
from datetime import datetime
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder.appName("Transactions").getOrCreate()

# Define schema with trans_date as DateType
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("trans_date", DateType(), True)  # Directly as DateType
])

# Sample data with datetime conversion
data = [
    (121, "US", "approved", 1000, datetime.strptime("2018-12-18", "%Y-%m-%d")),
    (122, "US", "declined", 2000, datetime.strptime("2018-12-19", "%Y-%m-%d")),
    (123, "US", "approved", 2000, datetime.strptime("2019-01-01", "%Y-%m-%d")),
    (124, "DE", "approved", 2000, datetime.strptime("2019-01-07", "%Y-%m-%d"))
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
df.show()

monthly_transactions = df.withColumn("order_month",
                        F.date_format(F.col("trans_date"), "yyyy-MM"))\
              .withColumn("approved_flag",
                          F.when(F.col("state") == "approved", 1).otherwise(F.lit(0)))\
              .withColumn("approved_amount",
                          F.when(F.col("state") == "approved", F.col("amount")).otherwise(F.lit(0)))\
              .select("order_month", "country", "amount", "approved_flag", "approved_amount")

monthly_transactions.show()

ans_df = monthly_transactions.groupby("order_month", "country")\
            .agg(
                F.count("order_month").alias("trans_count"),
                F.sum("approved_flag").alias("approved_count"),
                F.sum("amount").alias("trans_total_amount"),
                F.sum("approved_amount").alias("approved_total_amount ")
            )

ans_df.show()