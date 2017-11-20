from pyspark import SparkConf, SparkContext

conf = SparkConf()
conf.setAppName("Cancelled OrderOver1000 Pyspark")

sc = SparkContext(conf=conf)

def parseOrders(rec):
        parts = rec.split(",")
        return (int(parts[0]), parts[1], int(parts[2]), parts[3])

orders = sc.textFile("/user/hive/warehouse/retail_db.db/orders").map(lambda x : parseOrders(x))

ordersKV = orders.keyBy(lambda x: x[0])

for x in ordersKV.collect(): print(x)
