from pyspark import SparkContext, SparkConf

def parseOrders(rec):
    parts = rec.split(",")
    return (int(parts[0]), parts[1], int(parts[2]), parts[3]  )


def parseOrderItems(rec):
    parts = rec.split(",")
    return (int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3]), float(parts[4]), float(parts[5])  )

conf = SparkConf()
conf.setMaster("local")
conf.setAppName("OrderRevenueCountPerDay")

sc = SparkContext(conf=conf)

path = "/user/hive/warehouse/retail_db.db"

orders = sc.textFile(path + "/orders").map(lambda x: parseOrders(x))
orderItems = sc.textFile(path + "/order_items").map(lambda x: parseOrderItems(x))

# We cannot join the tuples together even if we wanted to join on first column because 
# spark will only take first column as key and the 2nd column from each dataset as value 
# and ignore the remaining fields from both datasets

ordersKV = orders.map(lambda x: (x[0], (x[1], x[2], x[3])))
orderItemsKV = orderItems.map(lambda x: (x[1], (x[0], x[2], x[3], x[4], x[5])))

# Join Demo.
ordersJoin = ordersKV.join(orderItemsKV)

# Generate a flat structure with order_date and orderId  as key and order_subtotal as value.
ordersByOrderIdDate = ordersJoin.map(lambda x : ( (x[0],x[1][0][0]), x[1][1][3]))

# Calculated totals of orders
orderTotalsPerOrder = ordersByOrderIdDate.reduceByKey( lambda acc,val: acc +val)

# Generate a flat structure with Date as key and subtotal as value
ordersByDate = orderTotalsPerOrder.map(lambda x: (x[0][1], x[1]))

# now again reduce by key (orderdate) to find total revenure and ordercount per day
# However since there is change in the input type (float) and output type (float, int) we need to use aggregate by key
#revenueAndOrderCountPerDay = ordersByDate.aggregateByKey( (0.0, 0)  ,  lambda acc, val : (acc[0] + val, acc[1] + 1), lambda acc,val: (acc[0] + val[0], acc[1] + val[1]) )

# OR we can use combineByKey
# The first parameter is a function which takes in a record (the first record from each partition is passed to this function.
# the Second parameter is the combiner function which takes in the tuple returned by first parameter and the 2nd Value, 3rd value and so on...
revenueAndOrderCountPerDay = ordersByDate.combineByKey( lambda t: (t, 1), lambda acc, val: (acc[0] + val, acc[1] + 1), lambda acc, val: (acc[0] + val[0], acc[1] + val[1]) )

temp = revenueAndOrderCountPerDay.take(100)
for x in temp: print(x)

