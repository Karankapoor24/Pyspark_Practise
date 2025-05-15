"""
Q2 - Find CTR of ads. Round to 2 decimal points. Order result by ctr in descending order & ad_id by ascending order in case of tie.
CTR = clicked / (clicked + viewed)

Input

+-----+-------+-------+
|ad_id|user_id| action|
+-----+-------+-------+
|    1|      1|Clicked|
|    2|      2|Clicked|
|    3|      3| Viewed|
|    5|      5|Ignored|
|    1|      7|Ignored|
|    2|      7| Viewed|
|    3|      5|Clicked|
|    1|      4| Viewed|
|    2|     11| Viewed|
|    1|      2|Clicked|
+-----+-------+-------+

Output

+-----+----+----+
|ad_id| ctr|rank|
+-----+----+----+
|    1|0.67|   1|
|    3| 0.5|   2|
|    2|0.33|   3|
|    5| 0.0|   4|
+-----+----+----+

"""

#Solution

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import when, col, sum as spark_sum, round, rank
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Q2").master("local").getOrCreate()

data = [
    (1, 1, 'Clicked'),
    (2, 2, 'Clicked'),
    (3, 3, 'Viewed'),
    (5, 5, 'Ignored'),
    (1, 7, 'Ignored'),
    (2, 7, 'Viewed'),
    (3, 5, 'Clicked'),
    (1, 4, 'Viewed'),
    (2, 11, 'Viewed'),
    (1, 2, 'Clicked'),
]

schema = StructType([
    StructField("ad_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("action", StringType(), True)
])

df = spark.createDataFrame(data, schema)

df.show()

df_cnt = df.withColumn("click_cnt", when(col("action") == "Clicked", 1).otherwise(0) ). \
            withColumn("view_cnt", when(col("action") == "Viewed", 1).otherwise(0) )

df_cnt.show()

df_group = df_cnt.groupBy("ad_id").agg(
                                        spark_sum("click_cnt").alias("sum_click"),
                                        spark_sum("view_cnt").alias("sum_view")
                                )
df_group.show()

ctr_df = df_group.select("ad_id", round(col("sum_click") / (col("sum_click") + col("sum_view")), 2).alias("ctr"))
ctr_df.show()

window_spec = Window.orderBy(col("ctr").desc_nulls_last(), col("ad_id").asc())

ans_df = ctr_df.withColumn("rank", rank().over(window_spec)).fillna(0)
#ans_df = ans_df.select("ad_id", "ctr")
ans_df.show()
