from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType,StructField,
                               IntegerType,StringType,FloatType)


def parse_line(line):
    fields = line.split(",")
    return (fields(0),int(fields(1)),int(fields(2)),int(fields(3)),float(fields(4)))

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("P2")
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("ERROR")

inpRdd = sc.textFile("C:/Users/karan/OneDrive/Desktop/Codes/Data/W11.csv")

print(inpRdd.getNumPartitions())

mappedRDD1 = inpRdd.map(parse_line)
repRDD = mappedRDD1.repartition(8)

print(repRDD.getNumPartitions())

df_schema = StructType(
    [
        StructField("Country",StringType(), False),
        StructField("Weeknum", IntegerType(), False),
        StructField("num_invoices",IntegerType(),False),
        StructField("total_quantity",IntegerType(), False),
        StructField("invoice_value",FloatType(), False)
    ]
)

inpDF = spark.createDataFrame(repRDD, ["Country","Weeknum","num_invoices","total_quantity","invoice_value"])

inpDF.show()

spark.stop()