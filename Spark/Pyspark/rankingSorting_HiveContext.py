
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

conf = SparkConf()
conf.setAppName("Sorting Ranking pyspark sql")

sc = SparkContext(conf = conf)
sqlContext = HiveContext(sc)

sqlContext.sql("set spark.sql.shuffle.partitions = 10")

# Global Sorting and Ranking use ORDER BY AND LIMIT 
#for x in sqlContext.sql("Select * from retail_db.products ORDER BY product_price DESC LIMIT 10").collect():
#    print(x)

# Per key sorting using DISTRIBUTE BY AND SORT BY

#for x in sqlContext.sql("Select * from retail_db.products DISTRIBUTE BY product_category_id SORT BY product_category_id,product_price DESC").collect():
#   print(x.product_category_id, x.product_id, x.product_name, x.product_price)


# FOR RANKING EITHER SPARSE OR DENSE its better to use Normal RDD transformation instead of using SQLContext or HiveContext
# since not all functions are supported by both.

# Example of Dense Ranking using HiveContext
query = """SELECT product_category_id, product_id, product_name, product_price FROM
	(
	  SELECT p.*, dense_rank() OVER(PARTITION BY p.product_category_id ORDER BY p.product_price DESC) dr
	  FROM retail_db.products p
	) t
	WHERE dr <= 3""" 

for x in sqlContext.sql(query).collect() : print(x)
	
