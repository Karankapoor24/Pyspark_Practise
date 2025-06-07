"""
LC 550
Write a solution to report the fraction of players that logged in again on the day after the day they first logged in, rounded to 2 decimal places.
In other words, you need to count the number of players that logged in for at least two consecutive days starting from their first login date,
then divide that number by the total number of players.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType
import pyspark.sql.functions as f
from datetime import datetime

# Initialize SparkSession
spark = SparkSession.builder.appName("ActivityData").getOrCreate()

# Define Schema
schema = StructType([
    StructField("player_id", IntegerType(), True),
    StructField("device_id", IntegerType(), True),
    StructField("event_date", DateType(), True),
    StructField("games_played", IntegerType(), True)
])

# Define Data
data = [
    (1, 2, datetime.strptime("2016-03-01", "%Y-%m-%d"), 5),
    (1, 2, datetime.strptime("2016-03-02", "%Y-%m-%d"), 6),
    (2, 3, datetime.strptime("2017-06-25", "%Y-%m-%d"), 1),
    (3, 1, datetime.strptime("2016-03-02", "%Y-%m-%d"), 0),
    (3, 4, datetime.strptime("2018-07-03", "%Y-%m-%d"), 5)
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
df.show()

first_login = df.groupBy("player_id").agg(F.min("event_date").alias("first_login_date"))
first_login.show()

consecutive_login = df.join(first_login,
                            (df["player_id"] == first_login["player_id"]) & (df["event_date"] == F.date_add(first_login["first_login_date"], 1)),
                            "inner")
consecutive_login.show()

ans = consecutive_login.count()
ans

distinct_first_login = first_login.select(F.countDistinct("player_id")).collect()[0][0]
distinct_consecutive_login = consecutive_login.select(F.countDistinct("player_id")).collect()[0][0]

ratio = distinct_consecutive_login / distinct_first_login

print("Ratio of consecutive logins to first logins:", ratio)
