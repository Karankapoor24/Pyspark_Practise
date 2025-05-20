"""
Write a solution to report the name and bonus amount of
each employee with a bonus less than 1000.
Return the result table in any order

Input

+-----+-----+
|empId|bonus|
+-----+-----+
|    2|  500|
|    4| 2000|
+-----+-----+


Output


"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("Q9").master("local").getOrCreate()

employee_schema = StructType([
    StructField("empId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("supervisor", IntegerType(), True),
    StructField("salary", IntegerType(), True)
])

employee_data = [
    (3, 'Brad', None, 4000),
    (1, 'John', 3, 1000),
    (2, 'Dan', 3, 2000),
    (4, 'Thomas', 3, 4000)
]

bonus_schema = StructType([
    StructField("empId", IntegerType(), True),
    StructField("bonus", IntegerType(), True)
])

bonus_data = [
    (2, 500),
    (4, 2000)
]

emp_df = spark.createDataFrame(employee_data, employee_schema)
emp_df.show()

bonus_df = spark.createDataFrame(bonus_data, bonus_schema)
bonus_df.show()

joined_df = emp_df.join(bonus_df, emp_df["empId"] == bonus_df["empId"], "left")

joined_df.show()

ans = joined_df.filter((F.col("bonus") < 1000) | (F.col("bonus").isNull()))\
        .select("name", F.coalesce("bonus", F.lit(0)).alias("bonus"))

ans.show()
