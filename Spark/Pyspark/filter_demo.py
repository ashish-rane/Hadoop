from pyspark import SparkConf, SparkContext
conf = SparkConf()
conf.setMaster("local")
conf.setAppName("filter demo Pyspark")
sc = SparkContext(conf=conf)

def parseOrders(rec):
    parts = rec.split(",")
    return (int(parts[0]), parts[1], int(parts[2]), parts[3])

def parseOrderItems(rec):
    parts = rec.split(",")
    return (int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3]), float(parts[4]), float(parts[5])  )

# Find all cancelled orders that have total of more than 1000
path = "hdfs://ip-172-31-53-48.ec2.internal:8020/apps/hive/warehouse/retail_db.db"
orders = sc.textFile(path + "/orders").map(lambda x: parseOrders(x))
orderItems = sc.textFile(path + "/order_items").map(lambda x: parseOrderItems(x))

# Always filter as early as possible
cancelledOrders = orders.filter(lambda x: x[3].find("CANCELED") != -1)
cancelledOrdersKV = cancelledOrders.map(lambda x: (x[0], (x[1], x[2], x[3])))

# Always perform aggregations if possible prior to join. 
# In this case since we need totals per order we can do this before joining on order id
orderItemsKV = orderItems.map(lambda x : ( x[1], x[4]))

orderItemTotals = orderItemsKV.reduceByKey(lambda acc,val: acc + val)

ordersJoin = cancelledOrdersKV.join(orderItemTotals);

ordersGreaterThan1000 = ordersJoin.filter(lambda x : x[1][1] > = 1000.0)

for x in ordersGreaterThan1000.take(10) : print(x)
