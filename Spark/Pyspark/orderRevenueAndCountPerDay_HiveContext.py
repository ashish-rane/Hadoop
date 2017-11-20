from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

conf = SparkConf()
conf.setAppName("OrderRevenueAndCountPerDay HiveContext")

sc = SparkContext(conf=conf)

# Create Hive Context
sqlContext = HiveContext(sc)

# use Hive Query. By default the shuffle partitions are set to 200 which means 200 tasks will be run. to set this to different value
sqlContext.sql("set spark.sql.shuffle.partitions=10")
revenueAndOrderCountPerDay = sqlContext.sql( \
        "SELECT order_date, SUM(order_total) as revenue, count(*) FROM \
                ( select o.order_id as order_id, o.order_date AS order_date, sum(oi.order_item_subtotal) as order_total \
                         from retail_db.orders o JOIN retail_db.order_items oi ON o.order_id = oi.order_item_order_id \
                         GROUP BY o.order_id, o.order_date \
                ) ordersJoin \
                GROUP BY order_date")

temp = revenueAndOrderCountPerDay.take(100)
for x in temp: print(x)


