from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext

conf = SparkConf()
conf.setAppName("Avro Pyspark")

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

path = "hdfs://ip-172-31-53-48.ec2.internal:8020/apps/hive/warehouse/retail_db.db/departments_avro/000000_0"

depRDD =sqlContext.read.format("com.databricks.spark.avro").load(path)


for x in depRDD.collect() : print(x)
