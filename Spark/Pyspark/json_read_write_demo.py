
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext

conf = SparkConf()
conf.setAppName("Json Pyspark")

sc = SparkContext(conf=conf)

# Create SQL Context
sqlContext = SQLContext(sc)

# Read Json file using sqlcontext
depJSON = sqlContext.jsonFile("/user/cloudera/output/itversity/pig/departments_data.json/part-m-00000")

for x in depJSON.collect():
	print(x)

# Create A Structure on top of the raw json data
depJSON.registerTempTable("deps")

# Now query the table
depRDD = sqlContext.sql("Select * from deps")
for x in depRDD.collect():
	print(x)

# Another method to convert the raw file into a structure. This is removed Spark 1.3.0 onwards since Dataframe were introduced.
# Also all the methods on SQLContext,HiveContext used to return SchemaRDD in 1.2.1 but from 1.3.0 onwards these functions return DataFrame object.
# From 1.3.0 onwards the SchemaRDD class is completely removed from Spark.
#sqlContext.registerRDDAsTable(depJSON, "departments")

#depRDD1 = sqlContext.sql("Select * from departments")
#for x in depRDD1.collect():
#	print(x)

# get the schem or print the schema
#scheme = depJSON.schema()

#print(depJSON.schemaString())
#print(depRDD.schemaString())


# Writing data to json file from RDD
# depRDD.toJSON().saveAsTextFile("/user/cloudera/output/itversity/python/departmentsJson")
