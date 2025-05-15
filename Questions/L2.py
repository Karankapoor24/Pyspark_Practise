"""

Given a table 'sf_transactions' of purchases by date, calculate the month-over-month percentage change in revenue.
The output should include the year-month date (YYYY-MM) and percentage change, rounded to the 2nd decimal point, and sorted from the beginning of the
year to the end of the year. The percentage change column will be populated from the 2nd month forward and calculated as
((this month’s revenue — last month’s revenue) / last month’s revenue)*100.

Input
id	created_at				value	purchase_id
1	2019-01-01 00:00:00.000	172692	43
2	2019-01-05 00:00:00.000	177194	36
3	2019-01-09 00:00:00.000	109513	30
4	2019-01-13 00:00:00.000	164911	30
5	2019-01-17 00:00:00.000	198872	39
6	2019-01-21 00:00:00.000	184853	31
7	2019-01-25 00:00:00.000	186817	26
8	2019-01-29 00:00:00.000	137784	22
9	2019-02-02 00:00:00.000	140032	25
10	2019-02-06 00:00:00.000	116948	43
11	2019-02-10 00:00:00.000	162515	25
12	2019-02-14 00:00:00.000	114256	12
13	2019-02-18 00:00:00.000	197465	48
14	2019-02-22 00:00:00.000	120741	20
15	2019-02-26 00:00:00.000	100074	49
16	2019-03-02 00:00:00.000	157548	19
17	2019-03-06 00:00:00.000	105506	16
18	2019-03-10 00:00:00.000	189351	46
19	2019-03-14 00:00:00.000	191231	29
20	2019-03-18 00:00:00.000	120575	44
21	2019-03-22 00:00:00.000	151688	47
22	2019-03-26 00:00:00.000	102327	18
23	2019-03-30 00:00:00.000	156147	25


Output

yearMonth	total_revenue	percentage_change
2019-01		1332636			NULL
2019-02		952031			-28.560000000000
2019-03		1174373			23.350000000000

"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder.appName("SQL_to_PySpark").getOrCreate()

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("value", IntegerType(), True),
    StructField("purchase_id", IntegerType(), True)
])

# Define data with datetime conversion
data = [
    (1, datetime.strptime("2019-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"), 172692, 43),
    (2, datetime.strptime("2019-01-05 00:00:00", "%Y-%m-%d %H:%M:%S"), 177194, 36),
    (3, datetime.strptime("2019-01-09 00:00:00", "%Y-%m-%d %H:%M:%S"), 109513, 30),
    (4, datetime.strptime("2019-01-13 00:00:00", "%Y-%m-%d %H:%M:%S"), 164911, 30),
    (5, datetime.strptime("2019-01-17 00:00:00", "%Y-%m-%d %H:%M:%S"), 198872, 39),
    (6, datetime.strptime("2019-01-21 00:00:00", "%Y-%m-%d %H:%M:%S"), 184853, 31),
    (7, datetime.strptime("2019-01-25 00:00:00", "%Y-%m-%d %H:%M:%S"), 186817, 26),
    (8, datetime.strptime("2019-01-29 00:00:00", "%Y-%m-%d %H:%M:%S"), 137784, 22),
    (9, datetime.strptime("2019-02-02 00:00:00", "%Y-%m-%d %H:%M:%S"), 140032, 25),
    (10, datetime.strptime("2019-02-06 00:00:00", "%Y-%m-%d %H:%M:%S"), 116948, 43),
    (11, datetime.strptime("2019-02-10 00:00:00", "%Y-%m-%d %H:%M:%S"), 162515, 25),
    (12, datetime.strptime("2019-02-14 00:00:00", "%Y-%m-%d %H:%M:%S"), 114256, 12),
    (13, datetime.strptime("2019-02-18 00:00:00", "%Y-%m-%d %H:%M:%S"), 197465, 48),
    (14, datetime.strptime("2019-02-22 00:00:00", "%Y-%m-%d %H:%M:%S"), 120741, 20),
    (15, datetime.strptime("2019-02-26 00:00:00", "%Y-%m-%d %H:%M:%S"), 100074, 49),
    (16, datetime.strptime("2019-03-02 00:00:00", "%Y-%m-%d %H:%M:%S"), 157548, 19),
    (17, datetime.strptime("2019-03-06 00:00:00", "%Y-%m-%d %H:%M:%S"), 105506, 16),
    (18, datetime.strptime("2019-03-10 00:00:00", "%Y-%m-%d %H:%M:%S"), 189351, 46),
    (19, datetime.strptime("2019-03-14 00:00:00", "%Y-%m-%d %H:%M:%S"), 191231, 29),
    (20, datetime.strptime("2019-03-18 00:00:00", "%Y-%m-%d %H:%M:%S"), 120575, 44),
    (21, datetime.strptime("2019-03-22 00:00:00", "%Y-%m-%d %H:%M:%S"), 151688, 47),
    (22, datetime.strptime("2019-03-26 00:00:00", "%Y-%m-%d %H:%M:%S"), 102327, 18),
    (23, datetime.strptime("2019-03-30 00:00:00", "%Y-%m-%d %H:%M:%S"), 156147, 25)
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
df.show()

monthlyRevenue = df.withColumn(
    "yearMonth", F.date_format(F.col("created_at"), "yyyy-MM"))\
    .groupBy("yearMonth").agg(F.sum("value").alias("total_revenue"))

monthlyRevenue.show()

win_spec = Window.orderBy("yearMonth")
revenueChange = monthlyRevenue.withColumn(
    "previous_month_revenue", F.lag(F.col("total_revenue")).over(win_spec)
        )
revenueChange.show()

final = revenueChange.withColumn(
        "percentage_change",
         F.round(F.when(F.col("previous_month_revenue").isNotNull(),
                100.0 * (F.col("total_revenue") - F.col("previous_month_revenue")) / F.col("previous_month_revenue")
                ), 2)
        ).select("yearMonth", "total_revenue", "percentage_change")

final.show()

