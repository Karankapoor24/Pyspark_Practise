"""
Write a solution to find the percentage of the users registered in each contest
rounded to two decimals.

Return the result table ordered by percentage in descending order.
In case of a tie, order it by contest_id in ascending order.

Output

+----------+----------+
|contest_id|percentage|
+----------+----------+
|       208|     100.0|
|       209|     100.0|
|       210|     100.0|
|       215|     66.67|
|       207|     33.33|
+----------+----------+

"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder.appName("ContestData").getOrCreate()

# Define schema for Users table
users_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("user_name", StringType(), True)
])

# Define schema for Register table
register_schema = StructType([
    StructField("contest_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True)
])

# Create data for Users table
users_data = [
    (6, "Alice"),
    (2, "Bob"),
    (7, "Alex")
]

# Create data for Register table
register_data = [
    (215, 6),
    (209, 2),
    (208, 2),
    (210, 6),
    (208, 6),
    (209, 7),
    (209, 6),
    (215, 7),
    (208, 7),
    (210, 2),
    (207, 2),
    (210, 7)
]

# Create DataFrames
users_df = spark.createDataFrame(users_data, schema=users_schema)
register_df = spark.createDataFrame(register_data, schema=register_schema)

# Show DataFrames
users_df.show()
register_df.show()

users_count = users_df.count()
print(users_count)

ans_df = register_df.groupby("contest_id")\
            .agg(F.round((100.0 * F.count("user_id") / users_count), 2).alias("percentage"))\
            .orderBy(F.desc("percentage"), "contest_id")

ans_df.show()