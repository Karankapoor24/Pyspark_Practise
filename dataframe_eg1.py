from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name", "My First Application")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

#ordersDF = spark.read.option("header",True) \
#    .option("inferSchema",True) \
#    .csv("C:/Users/karan/OneDrive/Desktop/Codes/Data/orders.csv")

ordersDF = spark.read.format("csv") \
    .option("header",True) \
    .option("inferSchema",True) \
    .option("path","C:/Users/karan/OneDrive/Desktop/Codes/Data/orders.csv") \
    .load()


#ordersDF.show()
#ordersDF.printSchema()

grouped_df = ordersDF.where("order_customer_id > 10000") \
.select("order_id","order_customer_id") \
.groupBy("order_customer_id") \
.count()

grouped_df.show()
spark.stop()
