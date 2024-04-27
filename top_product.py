from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("product_analysis")
    .getOrCreate()
)

cols = "product_id INT, product_name STRING, quantity_sold INT, order_date STRING"

data = [
    (1, "Product A", 50, "2023-07-01"),
    (2, "Product B", 75, "2023-07-05"),
    (3, "Product C", 30, "2023-07-12"),
    (4, "Product A", 45, "2023-07-15"),
    (5, "Product D", 60, "2023-07-20"),
    (6, "Product B", 70, "2023-07-25"),
    (7, "Product C", 35, "2023-07-29"),
    (8, "Product D", 55, "2023-07-30"),
    (9, "Product E", 40, "2023-07-01"),
    (10, "Product F", 65, "2023-07-10")
]

products = spark.createDataFrame(data,schema=cols)

products = products.select("product_id","product_name","quantity_sold",\
            F.to_date("order_date", "yyyy-MM-dd").alias("order_date"))

products.printSchema()

products.show()

pr1 = products.withColumn("mon", F.month("order_date"))

pr1.show()

#pr1.printSchema()

target_month = 7

filtered_sal = pr1.where(F.col("mon") == 7)

agg_sal = filtered_sal.groupBy("product_id", "product_name")\
    .agg(sum("quantity_sold").alias("total_quantity_sold"))

window_spec = Window.orderBy(desc("total_quantity_sold"))

top_selling_products = agg_sal.withColumn("rank", \
                    rank().over(window_spec))

N = 10

top_N_products = top_selling_products.filter(top_selling_products.rank <= N)

filtered_sal.show()
spark.stop()