from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from datetime import datetime

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("unique_words")
    .getOrCreate()
)

# order_data with schema ("order_id","order_items","customer_id", "order_date") and
# product_data with schema ("item_id","item_name","item_price")
# how to get total_amount for each customer.

order_data = [
 (1, ["item_1", "item_2", "item_3","item_2","item_2"], 101, "2023-09-01"),
 (2, ["item_2", "item_4","item_4"], 102, "2023-09-02"),
 (3, ["item_1","item_1", "item_3", "item_5"], 103, "2023-09-03"),
 (4, ["item_3", "item_4","item_4"], 104, "2023-09-04"),
 (5, ["item_1","item_1", "item_2"], 105, "2023-09-05")
]

order_schema = "order_id INT, order_items ARRAY<STRING>, customer_id INT, order_date STRING"

product_data = [
 ("item_1", 101, 20),
 ("item_2", 102, 15),
 ("item_3", 103, 25),
 ("item_4", 104, 30),
 ("item_5", 105, 10)
]

product_schema = "item_name STRING, item_id INT, item_price INT"

orderDF = spark.createDataFrame(order_data,order_schema)

productsDF = spark.createDataFrame(product_data, product_schema)

new_orders = orderDF.select(F.col("customer_id"),F.col("order_items"),\
               F.to_date(F.col("order_date"), 'yyyy-mm-dd').alias("order_date"))

new_orders.show(truncate=False)
productsDF.show()

exp_orders = new_orders.select("customer_id","order_date", F.explode(F.col("order_items")).alias("item_name"))

exp_orders.show()

joinDF = exp_orders.join(productsDF, on="item_name", how="inner")

joinDF.show()

joinDF.groupby("customer_id").agg(F.sum(F.col("item_price")).alias("sum_amount")).show()

spark.stop()