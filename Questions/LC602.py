"""
LC 602
Write a solution to find the people who have the most friends and the most friends number.
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime

# Initialize SparkSession
spark = SparkSession.builder.appName("RequestAccepted").getOrCreate()

# Sample data for RequestAccepted table
data = [
    (1, 2, "2016/06/03"),
    (1, 3, "2016/06/08"),
    (2, 3, "2016/06/08"),
    (3, 4, "2016/06/09")
]

# Preprocess the data to convert accept_date to date-only format
processed_data = [(requester_id, accepter_id, datetime.strptime(accept_date, "%Y/%m/%d").date())
                  for requester_id, accepter_id, accept_date in data]

columns = ["requester_id", "accepter_id", "accept_date"]

# Create DataFrame
df = spark.createDataFrame(processed_data, columns)

# Show the result
df.show()

df_swapped = df.selectExpr("accepter_id as requester_id", "requester_id as accepter_id", "accept_date")

# Perform union of the original DataFrame and the swapped DataFrame
union_df = df.union(df_swapped)

# Show the result
union_df.show()

ans_df = union_df.groupBy("requester_id") \
    .agg(F.count(F.col("accepter_id")).alias("num"))

ans_df.show()

window_spec = Window.orderBy(F.col("num").desc())
final_df = ans_df.withColumn("rnk", F.dense_rank().over(window_spec))\
            .filter(F.col("rnk") == 1)\
            .select(F.col("requester_id").alias("id"), "num")

final_df.show()