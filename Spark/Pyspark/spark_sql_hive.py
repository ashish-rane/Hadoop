from pyspark import SparkContext,SparkConf
from pyspark.sql import HiveContext

conf = SparkConf()
conf.setAppName("ReadWrite Hive")

sc = SparkContext(conf=conf)

# Create HiveContext
sqlContext = HiveContext(sc)

# here depRDD is a DataFrame RDD which is and RDD with schema (Row objects)
depRDD = sqlContext.sql("Select * from retail_db.departments")

for x in depRDD.collect():
	print(x.department_id)

# We can also specify CREATE TABLE and other hive commands and in the sqlContext.sql() function.
# Then we can use RDD.saveAsXXX() functions to save our transformed data that matches the table schema into the correspondind table location in HDFS
