"""
LC 570
Write a solution to find managers with at least five direct reports.

Output

+---+----+
| id|name|
+---+----+
|101|John|
|111|John|
+---+----+

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("EmployeeData").getOrCreate()

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("managerId", IntegerType(), True)
])

# Create data
data = [
    (101, "John", "A", None),
    (102, "Dan", "A", 101),
    (103, "James", "A", 101),
    (104, "Amy", "A", 101),
    (105, "Anne", "A", 101),
    (106, "Ron", "B", 101),
    (111, "John", "A", None),
    (112, "Dan", "A", 111),
    (113, "James", "A", 111),
    (114, "Amy", "A", 111),
    (115, "Anne", "A", 111),
    (116, "Ron", "B", 111)
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
df.show()

emp_alias = df.alias("e")
man_alias = df.alias("m")

joined_df = emp_alias.join(man_alias, F.col("e.managerId") == F.col("m.id")).select("m.id", "m.name", "e.id")
joined_df.show()

ans_df = joined_df.groupBy("m.id", "m.name").agg(F.count(F.col("e.id")).alias("cnt")).filter(F.col("cnt") >= 5).select("id", "name")
ans_df.show()