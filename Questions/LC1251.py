"""
Write a solution to find the average selling price for each product. average_price should
be rounded to 2 decimal places. If a product does not have any sold units,
its average selling price is assumed to be 0.

Return the result table in any order.

Output

+----------+-------------+
|product_id|average_price|
+----------+-------------+
|         1|         6.96|
|         2|        16.96|
+----------+-------------+

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType
from pyspark.sql import functions as F
from datetime import datetime
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder.appName("SalesData").getOrCreate()

# Define schema for Prices table
prices_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("price", IntegerType(), True)
])

# Define schema for UnitsSold table
units_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("purchase_date", DateType(), True),
    StructField("units", IntegerType(), True)
])

# Create raw data with datetime conversion
prices_data = [
    (1, datetime.strptime("2019-02-17", "%Y-%m-%d"), datetime.strptime("2019-02-28", "%Y-%m-%d"), 5),
    (1, datetime.strptime("2019-03-01", "%Y-%m-%d"), datetime.strptime("2019-03-22", "%Y-%m-%d"), 20),
    (2, datetime.strptime("2019-02-01", "%Y-%m-%d"), datetime.strptime("2019-02-20", "%Y-%m-%d"), 15),
    (2, datetime.strptime("2019-02-21", "%Y-%m-%d"), datetime.strptime("2019-03-31", "%Y-%m-%d"), 30)
]

units_data = [
    (1, datetime.strptime("2019-02-25", "%Y-%m-%d"), 100),
    (1, datetime.strptime("2019-03-01", "%Y-%m-%d"), 15),
    (2, datetime.strptime("2019-02-10", "%Y-%m-%d"), 200),
    (2, datetime.strptime("2019-03-22", "%Y-%m-%d"), 30)
]

# Create DataFrames
prices_df = spark.createDataFrame(prices_data, schema=prices_schema)
units_df = spark.createDataFrame(units_data, schema=units_schema)

# Show DataFrames
prices_df.show()
units_df.show()

prices_df = prices_df.withColumnRenamed("product_id", "product_id_prices")

joined_df = prices_df.join(units_df,
                           (prices_df["product_id_prices"] == units_df["product_id"]) &
                           ((units_df["purchase_date"] >=prices_df["start_date"]) &
                           (units_df["purchase_date"] <= prices_df["end_date"])),
                           "left"
                           )

joined_df.show()

final_df = joined_df.withColumn("amount", F.col("price") * F.col("units"))\
            .groupby(F.col("product_id_prices"))\
            .agg(F.coalesce(F.round((F.sum("amount") / F.sum("units")), 2), F.lit(0)).alias("average_price"))\
            .withColumnRenamed("product_id_prices", "product_id")

final_df.show()