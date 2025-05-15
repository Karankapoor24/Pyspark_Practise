"""
A table named “famous” has two columns called user id and follower id. It represents each user ID has a
particular follower ID. These follower IDs are also users. Then, find the famous percentage of each user.
Famous Percentage = number of followers a user has / total number of users on the platform.


Input

user_id	follower_id
1		2
1		3
2		4
5		1
5		3
11		7
12		8
13		5
13		10
14		12
14		3
15		14
15		13


Output

user_id	famous_percent
1		15
2		7
5		15
11		7
12		7
13		15
14		15
15		15
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql import functions as F
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark Session
spark = SparkSession.builder.appName("FollowersDataFrame").getOrCreate()

# Define schema
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("follower_id", IntegerType(), True)
])

# Create data
data = [
    (1, 2), (1, 3), (2, 4), (5, 1), (5, 3),
    (11, 7), (12, 8), (13, 5), (13, 10), (14, 12),
    (14, 3), (15, 14), (15, 13)
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

df.show()

df_users = df.select(F.col("user_id").alias("users"))

df_followers = df.select(F.col("follower_id").alias("users"))

df_union = df_users.union(df_followers).distinct()

row_count = df_union.count()

df_followers = df.groupBy("user_id").agg(F.count("follower_id").alias("followers"))

df_followers.show()

final_df = df_followers\
           .withColumn("%popularity", F.round(100 * F.col("followers")/row_count, 2))\
           .select("user_id", "%popularity")

final_df.show()