from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("product_analysis")
    .getOrCreate()
)

data = [
    (1, "2023-11-10"),
    (2, "2023-11-15"),
    (3, "2023-11-02"),
    (4, "2023-11-28")
]

df = spark.createDataFrame(data, ["id", "date_string"])

df = df.withColumn("date_column", \
                   F.to_date("date_string", "yyyy-MM-dd"))

df = df.withColumn("future_date", F.date_add("date_column", 7))

df = df.withColumn("past_date", F.date_sub("date_column", 3))

df = df.withColumn("date_difference", \
                   F.datediff("future_date", "past_date"))

df = df.withColumn("formatted_date", \
                   F.date_format("date_column", "dd-MM-yyyy"))

df = df.withColumn("year", F.trunc("date_column", "yyyy"))

df = df.withColumn("month", F.trunc("date_column", "MM"))

df.show()

spark.stop()