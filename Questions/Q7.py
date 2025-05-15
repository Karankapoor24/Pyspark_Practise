"""
Write a solution to find all dates' Id with higher
temperatures compared to its previous dates (yesterday).

Input

+---+-----------+----------+
| id|record_date|temprature|
+---+-----------+----------+
|  1| 2015-01-01|        10|
|  2| 2015-01-01|        25|
|  3| 2015-01-01|        20|
|  4| 2015-01-01|        30|
+---+-----------+----------+


"""
from Questions.Q6 import window_spec
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("Q7").master("local").getOrCreate()

weather_data = [
    (1, '2015-01-01', 10),
    (2, '2015-01-02', 25),
    (3, '2015-01-03', 20),
    (4, '2015-01-04', 30)
]

weather_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("record_date", StringType(), True),
    StructField("temprature", IntegerType(), True)
])

df = spark.createDataFrame(weather_data, weather_schema)

df.show()

window_spec = Window.orderBy("record_date")

prev_df = df.withColumn("last_dat_temp",F.lag())

