from pyspark import SparkConf, SparkContext

conf = SparkConf()
conf.setAppName("Cancelled OrderOver1000 Pyspark")

sc = SparkContext(conf=conf)

def parseOrders(rec):
	parts = rec.split(",")
	return (int(parts[0]), parts[1], int(parts[2]), parts[3])


def parseOrderItems(rec):
	parts = rec.split(",")
	return (int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3]), float(parts[4]), float(parts[5]))

path = "/user/hive/warehouse/retail_db.db"

orders = sc.textFile(path + "/orders").map(lambda x : parseOrders(x)).map(lambda x : (x[0], (x[1], x[2], x[3])))


orderItems = sc.textFile(path + "/order_items").map(lambda x : parseOrderItems(x)).map(lambda x: (x[1], x[4]))

# always filter as early as possible before doing joins, sort or aggregates
ordersCancelled = orders.filter(lambda x : x[1][2].upper() == "CANCELED")


ordersJoin = ordersCancelled.join(orderItems).map(lambda x: (x[0], (x[1][0][0], x[1][0][2], x[1][1])))

# use combine by key so that we can get the first item as zero value. the acc and val will be a tuple and hence from
# combineOp and  reduceOp should return a tuple with the last field added up for totals
orderTotals = ordersJoin.combineByKey(lambda x: x, lambda acc, val: (acc[0], acc[1], acc[2] + val[2]), lambda acc,val: (acc[0], acc[1], acc[2] + val[2]))
#for x in orderTotals.take(3): print(x)


ordersOver1000 = orderTotals.filter(lambda x: x[1][2] > 1000).sortByKey()

for x in ordersOver1000.collect(): print(x)

print(ordersOver1000.count())

