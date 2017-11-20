import org.apache.spark._;
import org.apache.spark.rdd._;
import org.apache.spark.sql.hive._;

val conf = new SparkConf().setMaster("local").setAppName("OrderRevenueAndCountPerDay HiveContext Scala");

val sc = new SparkContext(conf);

val sqlContext = new HiveContext(sc);

// Set the number of shuffle partitions. By default it 200
sqlContext.sql("set spark.sql.shuffle.partitions = 5");

val orderRevAndCountPerDay = sqlContext.sql(
	"""SELECT order_date, SUM(order_total) as revenue, count(*) FROM 
	( 
		 select o.order_id as order_id, o.order_date AS order_date, sum(oi.order_item_subtotal) as order_total
		 from retail_db.orders o JOIN retail_db.order_items oi ON o.order_id = oi.order_item_order_id 
		 GROUP BY o.order_id, o.order_date
	) ordersJoin 
	GROUP BY order_date""");

orderRevAndCountPerDay.take(10).foreach(println)

