from pyspark.sql import SparkSession

spark = SparkSession.builder\
.appName("DFJoin")\
.master("local[*]")\
.getOrCreate()

orders_data = [
		(1, 101, 100.0),
		(2, 102, 150.0),
		(3, 103, 200.0)
	]

customers_data = [
		(101, "Alice"),
		(102, "Bob"),
		(103, "Charlie")
	]

orders = spark.createDataFrame(orders_data, ["order_id", "customer_id", "amount"])

customers = spark.createDataFrame(customers_data, ["customer_id", "customer_name"])

orders.show()

customers.show()

joined_df = orders.join(customers,
            orders["customer_id"] == customers["customer_id"], "inner")

joined_df.show()

spark.stop()