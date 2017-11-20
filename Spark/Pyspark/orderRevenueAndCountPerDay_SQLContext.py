
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row

conf = SparkConf()
conf.setAppName("OrderCountAndRevPerDay SQLContext")

sc = SparkContext(conf=conf)

# Create SQLContext object, NOT HiveContext
sqlContext = SQLContext(sc)

path = "/user/hive/warehouse/retail_db.db"

orders = sc.textFile(path + "/orders").map(lambda x: x.split(","))

# Create a Row Schema from the split values
ordersRDD = orders.map(lambda x: Row(order_id=int(x[0]), order_date=x[1], order_cust_id=int(x[2]), order_status=x[3]))
# Create an RDD with schema based on the schema provided
ordersSchema = sqlContext.inferSchema(ordersRDD)
ordersSchema.registerTempTable("orders")

orderItems = sc.textFile(path + "/order_items").map(lambda x: x.split(","))
# Create Row Schema from split columns
orderItemsRDD = orderItems.map(lambda x: Row(order_item_id=int(x[0]), order_item_order_id=int(x[1]), order_item_prod_id=int(x[2]), order_item_quantity=int(x[3]), order_item_subtotal=float(x[4]), order_item_prod_price=float(x[5])))
# Create a Schema RDD based on the schema provided
orderItemsSchema = sqlContext.inferSchema(orderItemsRDD)
orderItemsSchema.registerTempTable("order_items")

sqlContext.sql("set spark.sql.shuffle.partitions=10")

#NOTE: Not all Hive queries will work in the SQL Context. For example, Hive Built-in functions like round,ceil,floor, explode, toDate, toString

revenueAndOrderCountPerDay = sqlContext.sql( \
        "SELECT order_date, SUM(order_total) as revenue, count(*) FROM \
                ( select o.order_id as order_id, o.order_date AS order_date, sum(oi.order_item_subtotal) as order_total \
                         from orders o JOIN order_items oi ON o.order_id = oi.order_item_order_id \
                         GROUP BY o.order_id, o.order_date \
                ) ordersJoin \
                GROUP BY order_date")

temp = revenueAndOrderCountPerDay.take(100)
for x in temp: print(x)


