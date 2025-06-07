"""
We define query quality as:
The average of the ratio between query rating and its position.

We also define poor query percentage as:
The percentage of all queries with rating less than 3.

Write a solution to find each query_name, the quality and poor_query_percentage.

Both quality and poor_query_percentage should be rounded to 2 decimal places.

Return the result table in any order.

+----------+-------+----------------------+
|query_name|quality|poor_query_percentage |
+----------+-------+----------------------+
|       Dog|    2.5|                 33.33|
|       Cat|   0.66|                 33.33|
+----------+-------+----------------------+

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import functions as F
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder.appName("QueriesData").getOrCreate()

# Define schema for Queries table
queries_schema = StructType([
    StructField("query_name", StringType(), True),
    StructField("result", StringType(), True),
    StructField("position", IntegerType(), True),
    StructField("rating", IntegerType(), True)
])

# Create data for Queries table
queries_data = [
    ("Dog", "Golden Retriever", 1, 5),
    ("Dog", "German Shepherd", 2, 5),
    ("Dog", "Mule", 200, 1),
    ("Cat", "Shirazi", 5, 2),
    ("Cat", "Siamese", 3, 3),
    ("Cat", "Sphynx", 7, 4)
]

# Create DataFrame
queries_df = spark.createDataFrame(queries_data, schema=queries_schema)
queries_df.show()

df1 = queries_df.withColumn("query_q", F.col("rating")/F.col("position"))\
                .withColumn("per", F.when(F.col("rating") < 3, 1).otherwise(F.lit(0)))
df1.show()

ans_df = df1.groupby("query_name")\
        .agg(
        F.round(F.avg("query_q"), 2).alias("quality"),
        F.round(100.0 * F.sum("per") / F.count("per"), 2).alias("poor_query_percentage ")
    )

ans_df.show()