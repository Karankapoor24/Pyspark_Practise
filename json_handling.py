from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("Json")
    .getOrCreate()
)

inputDF = (
    spark.read
    .format("json")
    .option("path","C:/Users/karan/OneDrive/Desktop/Codes/Data/sample_json.json")
    .load()
)

flattened_orders = inputDF.select("customer_id", "name", explode("orders").alias("order"))

flattened_items = flattened_orders.select("customer_id", "name", "order.order_id", "order.order_date",explode("order.items").alias("item"))

final_df = flattened_items.select("customer_id", "name", "order_id", "order_date", "item.item_id", "item.product", "item.quantity")

final_df.show()

spark.stop()
