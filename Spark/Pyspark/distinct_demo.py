from pyspark import SparkConf, SparkContext

conf = SparkConf()
conf.setAppName("Distinct Demo pyspark")

sc = SparkContext(conf=conf)

def parseOrders(rec):
	parts = rec.split(",")
	return (int(parts[0]), parts[1], int(parts[2]), parts[3])

orders = sc.textFile("/user/hive/warehouse/retail_db.db/orders")

orderStatus = orders.map(lambda x : parseOrders(x)).map(lambda x: x[3]).distinct()

for x in orderStatus.collect(): print(x)
