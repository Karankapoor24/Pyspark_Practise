from pyspark.sql import SparkSession
from pyspark import SparkConf

my_conf = SparkConf()
my_conf.set("spark.app.name","RDD To DF")
my_conf.set("spark.master", "local[*]")

spark = (
    SparkSession.builder.
    config(conf=my_conf)
    .getOrCreate()
)

sc = spark.sparkContext

details = [
        (1, "Jonh", 15000),
        (2, "Alice", 21000),
        (3, "Bob", 18000)
    ]

salary_rdd = sc.parallelize(details)

# 1st Way
salary_df = salary_rdd.toDF(["id", "name", "salary"])
salary_df.show()

orders_list = [
    ("John", "Laptop", 1),
    ("Alice", "Smartphone", 2),
    ("Bob", "Tablet", 3)
]

#2nd Way
ordersDF = spark.createDataFrame(orders_list, ["Customer", "Product", "Quantity"])
ordersDF.show()


spark.stop()