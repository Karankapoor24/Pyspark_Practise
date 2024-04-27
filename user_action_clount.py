from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from datetime import datetime

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("unique_words")
    .getOrCreate()
)

# Suppose you have a table called log_data with the following columns:
# log_id, user_id, action, and timestamp.
# Write a SQL query to calculate the number of actions performed by
# each user in the last 7 days.

data = [
 (1, 101, 'login', datetime.strptime('2023-09-05 08:30:00', '%Y-%m-%d %H:%M:%S')),
 (2, 102, 'click', datetime.strptime('2023-09-06 12:45:00', '%Y-%m-%d %H:%M:%S')),
 (3, 101, 'click', datetime.strptime('2023-09-07 14:15:00', '%Y-%m-%d %H:%M:%S')),
 (4, 103, 'login', datetime.strptime('2023-09-08 09:00:00', '%Y-%m-%d %H:%M:%S')),
 (5, 102, 'logout', datetime.strptime('2023-09-09 17:30:00', '%Y-%m-%d %H:%M:%S')),
 (6, 101, 'click', datetime.strptime('2023-09-10 11:20:00', '%Y-%m-%d %H:%M:%S')),
 (7, 103, 'click', datetime.strptime('2023-09-11 10:15:00', '%Y-%m-%d %H:%M:%S')),
 (8, 102, 'click', datetime.strptime('2023-09-12 13:10:00', '%Y-%m-%d %H:%M:%S'))
]

#columns = ["log_id", "user_id", "action", "timestamp"]

sch = T.StructType(
    [
        T.StructField("log_id", T.IntegerType(), False),
        T.StructField("user_id", T.IntegerType(), False),
        T.StructField("action", T.StringType(), False),
        T.StructField("timestamp", T.TimestampType(), False)
    ]
)

df = spark.createDataFrame(data, schema=sch)

df.show()

#df.createOrReplaceTempView("actions")

#spark.sql("select user_id, COUNT(user_id) FROM actions "
#          "WHERE DATEDIFF(CURRENT_DATE(), timestamp) <= 7 GROUP BY user_id").show()

df.select("user_id", "timestamp")\
    .filter(F.datediff(F.current_date(), F.col("timestamp")) <= 7)\
    .groupby("user_id").count()\
    .withColumnRenamed("count", "action_count").show()


spark.stop()