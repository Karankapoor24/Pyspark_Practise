"""

Write a solution to report the movies with an odd-numbered ID and a description that is not "boring".

Return the result table ordered by rating in descending order.

Output

+---+----------+-----------+------+
| id|     movie|description|rating|
+---+----------+-----------+------+
|  5|House card|Interesting|   9.1|
|  1|       War|   great 3D|   8.9|
+---+----------+-----------+------+

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import functions as F
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder.appName("MoviesDataFrame").getOrCreate()

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("movie", StringType(), True),
    StructField("description", StringType(), True),
    StructField("rating", FloatType(), True)
])

# Create data
data = [
    (1, "War", "great 3D", 8.9),
    (2, "Science", "fiction", 8.5),
    (3, "irish", "boring", 6.2),
    (4, "Ice song", "Fantasy", 8.6),
    (5, "House card", "Interesting", 9.1)
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
df.show()

filtered_df = df.filter((F.col("id") % 2 != 0) & ~(F.col("description").contains("boring")))\
                .orderBy(F.desc("rating"))

filtered_df.show()
