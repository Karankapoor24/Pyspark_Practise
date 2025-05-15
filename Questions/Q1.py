"""
Q1 - Find the desired Output

Input

+--------+-----------+---------+
|actor_id|director_id|timestamp|
+--------+-----------+---------+
|       1|          1|        0|
|       1|          1|        1|
|       1|          1|        2|
|       1|          2|        3|
|       1|          2|        4|
|       2|          1|        5|
|       2|          1|        6|
+--------+-----------+---------+

Output

+--------+-----------+-----+
|actor_id|director_id|count|
+--------+-----------+-----+
|       1|          1|    3|
+--------+-----------+-----+

"""

#Solution

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
import os
import sys

#os.environ['PYSPARK_PYTHON'] = sys.executable
#os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#print(sys.executable)

spark = SparkSession.builder. \
        master("local"). \
        appName("First Program"). \
        getOrCreate()
       
data = [
      (1, 1, 0),
      (1, 1, 1),
      (1, 1, 2),
      (1, 2, 3),
      (1, 2, 4),
      (2, 1, 5),
      (2, 1, 6)
  ]

schema = StructType(
      [
        StructField("actor_id", IntegerType(), True),
        StructField("director_id", IntegerType(), True),
        StructField("timestamp", IntegerType(), True)
    ]
  )
 
df = spark.createDataFrame(data, schema)

df.show()

df_group = df.groupBy("actor_id", "director_id").count().alias("cc")

df_group.show()

ans = df_group.filter(df_group["count"] >= 3)

ans.show()