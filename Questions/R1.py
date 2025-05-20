"""
Find the person_name of the last person that can fit on the bus without exceeding the
weight limit. The limit is 400.

Input

+--------+----------+------------+----+
|personId|personName|personWeight|turn|
+--------+----------+------------+----+
|       5|      john|         120|   2|
|       4|       tom|         100|   1|
|       3|     rahul|          95|   4|
|       6|    bhavna|         100|   5|
|       1|    ankita|          79|   6|
|       2|      Alex|          80|   3|
+--------+----------+------------+----+


Output

+----------+
|personName|
+----------+
|     rahul|
+----------+

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql import Window
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder.appName("BusData").getOrCreate()

# Define schema
schema = StructType([
    StructField("personId", IntegerType(), True),
    StructField("personName", StringType(), True),
    StructField("personWeight", IntegerType(), True),
    StructField("turn", IntegerType(), True)
])

# Create list of tuples representing the data
data = [
    (5, 'john', 120, 2),
    (4, 'tom', 100, 1),
    (3, 'rahul', 95, 4),
    (6, 'bhavna', 100, 5),
    (1, 'ankita', 79, 6),
    (2, 'Alex', 80, 3)
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
df.show()


window_spec = Window.orderBy("turn")
sum_weights = df.withColumn("sum_w", F.sum(F.col("personWeight")).over(window_spec))

sum_weights.show()

window_spec = Window.orderBy(F.col("turn").desc())
rnk_weights = sum_weights.filter(F.col("sum_w") <= 400)\
               .withColumn("rnk", F.rank().over(window_spec))

rnk_weights.show()

ans = rnk_weights.filter(F.col("rnk") == 1).select("personName")
ans.show()
