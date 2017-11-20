from pyspark import SparkConf, SparkContext
conf = SparkConf()
conf.setMaster("local")
conf.setAppName("avgRevPerCustPerDay Pyspark")
sc = SparkContext(conf=conf)

def parseOrders(rec):
    parts = rec.split(",")
    return (int(parts[0]), parts[1], int(parts[2]), parts[3])

def parseOrderItems(rec):
    parts = rec.split(",")
    return (int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3]), float(parts[4]), float(parts[5])  )

# Find Average Revenue of Customer Per Order.

path = "hdfs://ip-172-31-53-48.ec2.internal:8020/apps/hive/warehouse/retail_db.db"
orders = sc.textFile(path + "/orders").map(lambda x: parseOrders(x))
orderItems = sc.textFile(path + "/order_items").map(lambda x: parseOrderItems(x))

# find orders that are not cancelled
ordersNotCancelled = orders.filter(lambda x: x[3].find("CANCELED") == -1)

ordersKV = orders.map(lambda x : (x[0], x[2]))

orderItemsKV = orderItems.map(lambda x : (x[1], x[4]))

# FIRST METHOD : First Join and then Aggregate
#orderItemsTotalKV = orderItemsKV.reduceByKey(lambda acc, val : acc + val)
'''
ordersJoin = ordersKV.join(orderItemsKV)

orderTotalsByCustOrder = ordersJoin.map(lambda x : ( (x[1][0], x[0]), x[1][1]))

orderAggByCustOrder = orderTotalsByCustOrder.aggregateByKey( (0.0, 0), lambda acc, val: (acc[0] + val, acc[1] + 1), lambda acc, val : (acc[0] + val[0], acc[1] + val[1]))


avgRevPerCustPerOrder = orderAggByCustOrder.mapValues( lambda x: x[0]/x[1])

for x in avgRevPerCustPerOrder.take(10): print(x)
'''
# SECOND METHOD: Do intermediate aggregate and then join and do final aggregation. The join becomes optimized. 
# However teh intermediate aggregation for average should tranform the subtotal which is single value to return tuple with total and count.
orderItemsTotalsKV = orderItemsKV.aggregateByKey((0.0,0), lambda acc,val : (acc[0] + val, acc[1] + 1), lambda acc,val : (acc[0] + val[0], acc[1] + val[1]))

ordersJoin = ordersKV.join(orderItemsTotalsKV)

orderTotalsByCustOrder = ordersJoin.map(lambda x : ((x[1][0], x[0]), x[1][1]))
orderAggByCustOrder = orderTotalsByCustOrder.reduceByKey(lambda acc, val: (acc[0] + val[0], acc[1] + val[1]))

avgRevPerCustPerOrder = orderAggByCustOrder.mapValues( lambda x: x[0]/x[1])

for x in avgRevPerCustPerOrder.take(10): print(x)



