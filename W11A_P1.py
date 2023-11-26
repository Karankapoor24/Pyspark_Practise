from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType,StructField,
                               IntegerType,StringType,FloatType)

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("Assign11")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("Error")

df_schema = StructType(
    [
        StructField("Country",StringType(), False),
        StructField("Weeknum", IntegerType(), False),
        StructField("num_invoices",IntegerType(),False),
        StructField("total_quantity",IntegerType(), False),
        StructField("invoice_value",FloatType(), False)
    ]
)

inputDF = (
    spark.read
    .format("csv")
    .option("path","C:/Users/karan/OneDrive/Desktop/Codes/Data/W11.csv")
    .schema(df_schema)
    .load()
)

inputDF.write\
.partitionBy("Country","Weeknum")\
.mode("overwrite")\
.option("path", "C:/Users/karan/OneDrive/Desktop/Codes/Data/Outputs/W11P1")\
.save()

#result = inputDF.collect()

#for a in result:
    #print(a)

spark.stop()