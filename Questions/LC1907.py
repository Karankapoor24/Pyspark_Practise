"""
LC 1907
Write a solution to calculate the number of bank accounts for each salary category. The salary categories are:

"Low Salary": All the salaries strictly less than $20000.
"Average Salary": All the salaries in the inclusive range [$20000, $50000].
"High Salary": All the salaries strictly greater than $50000.
The result table must contain all three categories. If there are no accounts in a category, return 0.

Return the result table in any order.
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("AccountsTable").getOrCreate()

# Define data and schema
data = [(3, 108939), (2, 12747), (8, 87709), (6, 91796)]
columns = ["account_id", "income"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

all_categories = spark.createDataFrame([("Low Salary",), ("Average Salary",), ("High Salary",)], ["category"])
all_categories.show()


cat_sal = df.withColumn("category", F.when(F.col("income") < 20000, "Low Salary")
                                    .when(F.col("income") > 50000, "High Salary")
                                    .otherwise("Average Salary")
                       )\
            .groupBy("category").agg(F.count("account_id").alias("cnt"))
cat_sal.show()

ans_df = all_categories.join(cat_sal, on="category", how="left")\
            .selectExpr("category", "coalesce(cnt, 0) AS accounts_count")

ans_df.show()