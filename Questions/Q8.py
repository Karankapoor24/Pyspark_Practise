"""
Write a solution to find the first login date for each player.
Return the result table in any order
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("Q7").master("local").getOrCreate()

activity_schema = StructType([
    StructField("player_id", IntegerType(), True),
    StructField("device_id", IntegerType(), True),
    StructField("event_date", StringType(), True),
    StructField("games_played", IntegerType(), True)
])

activity_data = [
    (1, 2, '2016-03-01', 5),
    (1, 2, '2016-05-02', 6),
    (2, 3, '2017-06-25', 1),
    (3, 1, '2016-03-02', 0),
    (3, 4, '2018-07-03', 5)
]

df = spark.createDataFrame(activity_data, activity_schema)

df.show()

window_spec = Window.partitionBy("player_id").orderBy("event_date")

ans = df.withColumn("rnk", F.rank().over(window_spec))\
        .filter(F.col("rnk") == 1)\
        .select(F.col("player_id"), F.col("event_date").alias("First_Login"))

ans.show()