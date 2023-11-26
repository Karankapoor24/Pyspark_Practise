from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, countDistinct

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("unique_words")
    .getOrCreate()
)

data = [("Hello world and hello PySpark",),
 ("This is a PySpark example",),
 ("Let's count unique words",)]

schema = ["text"]
df = spark.createDataFrame(data, schema)

df.show()

words_df = df.withColumn("word", explode(split(df.text, " ")))

words_df.show()

unique_word_count = words_df.select(countDistinct("word")).collect()[0][0]

print(unique_word_count)