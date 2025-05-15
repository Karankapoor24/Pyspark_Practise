"""
Write a pyspark query to find the prices of all products on 2019-08-16.
Assume the price of all products before any change is 10.
Products table:
+------------+-----------+-------------+
| product_id | new_price | change_date |
+------------+-----------+-------------+
| 1     | 20    | 2019-08-14 |
| 2     | 50    | 2019-08-14 |
| 1     | 30    | 2019-08-15 |
| 1     | 35    | 2019-08-16 |
| 2     | 65    | 2019-08-17 |
| 3     | 20    | 2019-08-18 |
+------------+-----------+-------------+
Result table:
+------------+-------+
| product_id | price |
+------------+-------+
| 2     | 50  |
| 1     | 35  |
| 3     | 10  |
+------------+-------+
"""

from  datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, IntegerType, DateType
import pyspark.sql.functions as F
spark = SparkSession.builder\
.appName("DFJoin")\
.master("local[*]")\
.getOrCreate()

products_data = [
    (1, 20, datetime.strptime('2019-08-14', '%Y-%m-%d')),
    (2, 50, datetime.strptime('2019-08-14', '%Y-%m-%d')),
    (1, 30, datetime.strptime('2019-08-15', '%Y-%m-%d')),
    (1, 35, datetime.strptime('2019-08-16', '%Y-%m-%d')),
    (2, 65, datetime.strptime('2019-08-17', '%Y-%m-%d')),
    (3, 20, datetime.strptime('2019-08-18', '%Y-%m-%d')),
]

products_schema = StructType(
    [
        StructField('product_id', IntegerType()),
        StructField('new_price', IntegerType()),
        StructField('change_date', DateType())
    ]
)

products = spark.createDataFrame(products_data, products_schema)

products.show()

result_df = products.withColumn(
    'price',
    F.when(F.col('change_date') <= '2019-08-16', F.col('new_price'))
    .otherwise(10)
)
result_df.show()


spark.stop()

