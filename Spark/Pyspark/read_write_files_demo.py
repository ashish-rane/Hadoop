
from pyspark import SparkConf,SparkContext

conf = SparkConf()
conf.setAppName("readandwritedemo")

sc = SparkContext(conf=conf)

#read a text file
ordersRDD = sc.textFile("/user/hive/warehouse/retail_db.db/orders")

# Generate K,V pairs required for Sequence file
# Note the quick way of converting the splitted list into a tuple
#orders = ordersRDD.map(lambda  x : tuple(x.split("," , 1)))

def parseOrder(rec):
	parts = rec.split(",", 1)
	return (int(parts[0]), parts[1])

orders = ordersRDD.map(lambda x: parseOrder(x))

# Save a sequence file.
#orders.saveAsSequenceFile("/user/cloudera/output/itversity/spark/pyspark/orders_seq01")

# explicity specify custom output format and k,v types
orders.saveAsNewAPIHadoopFile("/user/cloudera/output/itversity/spark/pyspark/orders_seq01", \
				"org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat", \
				keyClass = "org.apache.hadoop.io.IntWritable", \
				valueClass = "org.apache.hadoop.io.Text")

			


# Now read the same sequence file
#ordersSeq = sc.sequenceFile("/user/cloudera/output/itversity/spark/pyspark/orders_seq01")

# explicity specify k,v types
#ordersSeq = sc.sequenceFile("/user/cloudera/output/itversity/spark/pyspark/orders_seq01", "org.apache.hadoop.io.IntWritable", "org.apache.hadoop.io.Text")

# explicity specify any custom input format  and k,v types
ordersSeq = sc.newAPIHadoopFile("/user/cloudera/output/itversity/spark/pyspark/orders_seq01", \
				"org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat", \
				"org.apache.hadoop.io.IntWritable", \
				"org.apache.hadoop.io.Text")



# do some transformations like filter completed orders only
completed = ordersSeq.map(lambda x: (x[0], tuple(x[1].split(",")))).filter(lambda x: x[1][2] == 'COMPLETE' )


for x in completed.collect():
	print(x)


