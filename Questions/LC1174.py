"""
LC 1174
If the customer's preferred delivery date is the same as the order date, then the order is called immediate; otherwise, it is called scheduled.

The first order of a customer is the order with the earliest order date that the customer made. It is guaranteed that a customer has precisely one first order.

Write a solution to find the percentage of immediate orders in the first orders of all customers, rounded to 2 decimal places.

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize SparkSession
spark = SparkSession.builder.appName("DeliveryData").getOrCreate()

# Define Schema
schema = StructType([
    StructField("delivery_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_pref_delivery_date", StringType(), True)
])

# Define Data
data = [
    (1, 1, "2019-08-01", "2019-08-02"),
    (2, 2, "2019-08-02", "2019-08-02"),
    (3, 1, "2019-08-11", "2019-08-12"),
    (4, 3, "2019-08-24", "2019-08-24"),
    (5, 3, "2019-08-21", "2019-08-22"),
    (6, 2, "2019-08-11", "2019-08-13"),
    (7, 4, "2019-08-09", "2019-08-09")
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
df.show()

window_spec = Window.partitionBy("customer_id").orderBy("order_date")
first_orders = df.withColumn("rnk", F.rank().over(window_spec))\
                .filter(F.col("rnk") == 1)\
                .withColumn("flag", F.when(F.col("order_date") == F.col("customer_pref_delivery_date"), 1).otherwise(F.lit(0)))

first_orders.show()

ans_df = first_orders.groupBy("rnk")\
                .agg(F.round(100.0 * F.sum(first_orders["flag"]) / F.count(first_orders["flag"]), 2).alias("immediate_percentage"))
ans_df.show()