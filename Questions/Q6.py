"""
Q6 - You are given a dataset of sales transactions for multiple stores & products.
Calculate  % contribution of each products sale to the total sales of its store

Input

+--------+-------+-----+
|store_id|product|sales|
+--------+-------+-----+
|      S1|     P1|  100|
|      S1|     P2|  200|
|      S1|     P3|  300|
|      S2|     P1|  400|
|      S2|     P2|  100|
|      S2|     P3|  500|
+--------+-------+-----+


Output

+-------+------------+
|product|%_sale_store|
+-------+------------+
|     P1|       16.67|
|     P2|       33.33|
|     P3|        50.0|
|     P1|        40.0|
|     P2|        10.0|
|     P3|        50.0|
+-------+------------+

"""

#Solution

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("Q6").master("local").getOrCreate()

data = [
      ("S1", "P1", 100),
      ("S1", "P2", 200),
    ("S1", "P3", 300),
    ("S2", "P1", 400),
    ("S2", "P2", 100),
    ("S2", "P3", 500)
]

schema = StructType([
    StructField("store_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("sales", StringType(), True)
])

df = spark.createDataFrame(data, schema)

df.show()


window_spec = Window.partitionBy("store_id").orderBy("product").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

sales_df = df.withColumn("store_sales", F.sum("sales").over(window_spec))\
             .withColumn("%_sale_store", F.round(100 * F.col("sales") / F.col("store_sales"), 2))\
            .select("product", "%_sale_store")

sales_df.show() 