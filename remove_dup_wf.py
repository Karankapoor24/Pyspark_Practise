from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("unique_words")
    .getOrCreate()
)

data = [
    (1, 101, 201, "2023-01-15"),
    (2, 102, 202, "2023-01-16"),
    (3, 101, 201, "2023-01-17"),
    (4, 103, 203, "2023-01-18")
]
columns = ["OrderID", "CustomerID", "ProductID", "OrderDate"]
orders_df = spark.createDataFrame(data, columns)

orders_df.show()

window_spec = (Window
    .partitionBy(orders_df["CustomerID"], orders_df[ "ProductID"])\
    .orderBy(orders_df["OrderID"]))

dup_df = orders_df.withColumn("rn",row_number().over(window_spec))

dup_df.show()

final_df = dup_df.filter(dup_df.rn == 1).drop("rn")

final_df.show()


spark.stop()