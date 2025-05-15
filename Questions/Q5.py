"""
Q5 - Report the duplicate emails.

Input

+---+-------+
| id|  email|
+---+-------+
|  1|a@b.com|
|  2|c@d.com|
|  3|a@b.com|
+---+-------+

Output

+-------+
|  email|
+-------+
|a@b.com|
+-------+
"""

#Solution

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local").appName("Q4").getOrCreate()

emails_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True)
])

emails_data = [
    (1, "a@b.com"),
    (2, "c@d.com"),
    (3, "a@b.com")
]

emails_df = spark.createDataFrame(emails_data, emails_schema)

emails_df.show()


output_df = emails_df.groupBy("email").count().filter(col("count") > 1).select("email")
output_df.show()