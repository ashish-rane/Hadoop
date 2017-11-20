from pyspark import SparkConf, SparkContext

conf = SparkConf()
conf.setMaster("local")
conf.setAppName("custWithMaxOrderPerDay Pyspark")

sc = SparkContext(conf=conf)


def parseOrders(rec):
    parts = rec.split(",")
    return (int(parts[0]), parts[1], int(parts[2]), parts[3])  
def parseOrderItems(rec):
    parts = rec.split(",")
    return (int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3]), float(parts[4]), float(parts[5])  )

path = "hdfs://ip-172-31-53-48.ec2.internal:8020/apps/hive/warehouse/retail_db.db"
orders = sc.textFile(path + "/orders").map(lambda x: parseOrders(x))
orderItems = sc.textFile(path + "/order_items").map(lambda x: parseOrderItems(x))

ordersKV = orders.map(lambda x: ( x[0], (x[1], x[2])))
orderItemsKV = orderItems.map(lambda x : (x[1], x[4]))

ordersJoin = ordersKV.join(orderItemsKV)
ordersByCustDate = ordersJoin.map(lambda x : ((x[1][0][0], x[1][0][1]), x[1][1]))
orderTotalsByCustDate = ordersByCustDate.reduceByKey(lambda acc,val : acc + val)

ordersByDate = orderTotalsByCustDate.map(lambda x : (x[0][0], (x[0][1], x[1])))

# First Method reduceByKey - PUSH MODEL
#maxCustOrderPerDate = ordersByDate.reduceByKey(lambda acc, val : acc if(acc[1] > val[1]) else val)

# Second Method groupByKey followed by mapValues - PULL MODEL
maxCustOrderPerDate  = ordersByDate.groupByKey().mapValues(lambda v : sorted(list(v), key= lambda i:  i[1], reverse=True )[0]).take(3)

for x in maxCustOrderPerDate.collect(): print(x)
