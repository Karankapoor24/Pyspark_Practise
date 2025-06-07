"""
You are given a table of product launches by company by year.
Write a query to count the net difference between the number of products companies launched in 2020 with
the number of products companies launched in the previous year. Output the name of the companies and a net difference
of net products released for 2020 compared to the previous year.

Output

+------------+----+
|company_name|diff|
+------------+----+
|      Toyota|  -1|
|       Honda|  -3|
|   Chevrolet|   2|
|        Ford|  -1|
+------------+----+

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder.appName("CarLaunches").getOrCreate()

# Define schema for car_launches
car_schema = StructType([
    StructField("year", IntegerType(), True),
    StructField("company_name", StringType(), True),
    StructField("product_name", StringType(), True)
])

# Create DataFrame for car_launches
car_data = [
    (2019, 'Toyota', 'Avalon'),
    (2019, 'Toyota', 'Camry'),
    (2020, 'Toyota', 'Corolla'),
    (2019, 'Honda', 'Accord'),
    (2019, 'Honda', 'Passport'),
    (2019, 'Honda', 'CR-V'),
    (2020, 'Honda', 'Pilot'),
    (2019, 'Honda', 'Civic'),
    (2020, 'Chevrolet', 'Trailblazer'),
    (2020, 'Chevrolet', 'Trax'),
    (2019, 'Chevrolet', 'Traverse'),
    (2020, 'Chevrolet', 'Blazer'),
    (2019, 'Ford', 'Figo'),
    (2020, 'Ford', 'Aspire'),
    (2019, 'Ford', 'Endeavour'),
    (2020, 'Jeep', 'Wrangler')
]

car_df = spark.createDataFrame(car_data, schema=car_schema)

# Show the result
car_df.show()

df_2020 = car_df.filter(F.col("year") == 2020)\
            .groupby("company_name").agg(F.count(F.col("product_name")).alias("count_2020"))

df_2020.show()

df_2019 = car_df.filter(F.col("year") == 2019)\
            .groupby("company_name").agg(F.count(F.col("product_name")).alias("count_2019"))

df_2019.show()

joined_df = df_2020.join(df_2019, on="company_name")\
               .withColumn("diff", F.col("count_2020") - F.col("count_2019"))\
               .select("company_name", "diff")

joined_df.show()
