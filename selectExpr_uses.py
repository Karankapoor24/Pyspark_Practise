from pyspark.sql import SparkSession
spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("unique_words")
    .getOrCreate()
)

data = [
    (1, "Alice", "2021-01-15", 16000),
    (2, "Bob", "2021-03-20", 21000),
    (3, "Charlie", "2021-02-10", 15000)
]

columns = ["id", "name", "birthdate", "salary"]

df = spark.createDataFrame(data, columns)

#Selecting Columns with Alias:
df.selectExpr("name AS full_name", "salary * 1.1 AS updated_salary").show()

#Mathematical Transformations:
df.selectExpr("salary", "salary * 1.5 AS increased_salary").show()

#String Manipulation:
df.selectExpr("name", "substring(birthdate, 1, 4) AS birth_year", "concat(name, ' - ', birth_year) AS name_year").show()

#Conditional Expressions:
df.selectExpr("name", "CASE WHEN salary > 15000 THEN 'High Salary' ELSE 'Low Salary' END AS salary_category").show()

#Type Casting:
df.selectExpr("name", "cast(salary AS double) AS double_salary").show()

spark.stop()