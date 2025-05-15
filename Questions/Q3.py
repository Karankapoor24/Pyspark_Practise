"""
Q3 - Report first_name, last_name, city, state of person given 2 dataframes person and address. If address is not there report null

Input

person_df

+---------+----------+---------+
|person_id|first_name|last_name|
+---------+----------+---------+
|        1|      Wang|    Allen|
|        2|     Alice|      Bob|
+---------+----------+---------+

address_df

+----------+---------+-------------+----------+
|address_id|person_id|         city|     state|
+----------+---------+-------------+----------+
|         1|        2|New York City|  New York|
|         2|        3|           CA|California|
+----------+---------+-------------+----------+


Output

+----------+---------+-------------+--------+
|first_name|last_name|         city|   state|
+----------+---------+-------------+--------+
|      Wang|    Allen|         NULL|    NULL|
|     Alice|      Bob|New York City|New York|
+----------+---------+-------------+--------+

"""

#Solution


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.master("local").appName("Q3").getOrCreate()

person_data = [
    (1, "Wang", "Allen"),
    (2, "Alice", "Bob")
]

person_schema = StructType([
    StructField("person_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True)
])

person_df = spark.createDataFrame(person_data, person_schema)

address_data = [
    (1, 2, "New York City", "New York"),
    (2, 3, "CA", "California")
]

address_schema = StructType([
    StructField("address_id", IntegerType(), True),
    StructField("person_id", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
])

address_df = spark.createDataFrame(address_data, address_schema)

person_df.show()
address_df.show()


final_df = person_df.join(address_df, person_df["person_id"] == address_df["person_id"], "left")

final_df = final_df.select("first_name", "last_name", "city", "state")
final_df.show()