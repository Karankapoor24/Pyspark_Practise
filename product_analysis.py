from pyspark.sql import SparkSession


spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("product_analysis")
    .getOrCreate()
)

purchase_data = spark.createDataFrame([
 (1, "A"),
 (1, "B"),
 (2, "A"),
 (2, "B"),
 (3, "A"),
 (3, "B"),
 (1, "C"),
 (1, "D"),
 (1, "E"),
 (3, "E"),
 (4, "A"),
 (5, "B")
], ["customer", "product_model"])

product_data = spark.createDataFrame([
 ("A",),
 ("B",),
 ("C",),
 ("D",),
 ("E",)
], ["product_model"])

purchase_data.show()
product_data.show()

# Find customers who have bought only product A.

purchase_data.createOrReplaceTempView("orders")
product_data.createOrReplaceTempView("prod")

spark.sql(""" 
WITH cte1 AS (
SELECT customer FROM orders GROUP BY customer HAVING 
COUNT(DISTINCT product_model) = 1
)
SELECT 
    o.customer
FROM cte1
INNER JOIN orders o
ON cte1.customer = o.customer
WHERE o.product_model = 'A'
""").show()

# Find customers who upgraded from product B to
# product E (they might have bought other products as well)

spark.sql(
"""
SELECT 
o1.customer 
FROM orders o1 
INNER JOIN orders o2 
ON o1.customer = o2.customer
WHERE o1.product_model='B' AND o2.product_model='E'
"""
).show()

# Find customers who have bought all models in the new Product Data
spark.sql("""
 SELECT customer
 FROM orders
 GROUP BY customer
 HAVING COUNT(DISTINCT product_model) = (SELECT COUNT(*) FROM prod)
""").show()

spark.stop()