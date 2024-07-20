from pyspark.sql import SparkSession
import os


spark = SparkSession.builder.appName("PySpark_task").getOrCreate()

products = spark.createDataFrame([
    {"id": 1, "name": "Milk"},
    {"id": 2, "name": "Potato"},
    {"id": 3, "name": "Ketchup"},
    {"id": 4, "name": "Tomato"},
])

categories = spark.createDataFrame([
    {"id": 1, "name": "Drinks"},
    {"id": 2, "name": "Vegetables"},
    {"id": 3, "name": "Milk products"},
])

product_categories = spark.createDataFrame([
    {"product_id": 1, "category_id": 3},
    {"product_id": 2, "category_id": 2},
    {"product_id": 4, "category_id": 2},
])

products.createOrReplaceTempView("products")
products.show()
categories.createOrReplaceTempView("categories")
categories.show()
product_categories.createOrReplaceTempView("product_categories")
product_categories.show()

result_df = spark.sql("""
    SELECT p.name
    FROM products p
    LEFT JOIN product_categories pc ON p.id = pc.productId
    LEFT JOIN categories c ON pc.categoryId = c.id
""")

result_df.show()
