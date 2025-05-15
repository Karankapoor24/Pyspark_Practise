"""
Q4 - Find employees earning more than their managers

Input

+---+-----+------+----------+
| id| name|salary|manager_id|
+---+-----+------+----------+
|  1|  Joe| 70000|         3|
|  2|Henry| 80000|         4|
|  3|  Sam| 60000|      NULL|
|  4|  Max| 90000|      NULL|
+---+-----+------+----------+


Output

+----+
|name|
+----+
| Joe|
+----+

"""

#Solution

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local").appName("Q4").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("manager_id", IntegerType(), True),
])

data = [
    (1, "Joe", 70000, 3),
    (2, "Henry", 80000, 4),
    (3, "Sam", 60000, None),
    (4, "Max", 90000, None)
]

employee_df = spark.createDataFrame(data, schema)

employee_df.show()


emp_df = employee_df.alias("e")
man_df = employee_df.alias("m")

joined_df = emp_df.join(man_df,  col("e.manager_id") == col("m.id"))

joined_df.show()

final_df = joined_df.filter(col("e.salary") > col("m.salary")).select(col("e.name"))

final_df.show()
