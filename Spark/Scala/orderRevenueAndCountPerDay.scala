import org.apache.spark._;
import org.apache.spark.rdd._;

val conf = new SparkConf().setMaster("local").setAppName("OrderRevenueAndCountPerDay Scala");

val sc = new SparkContext(conf)


val path = "/user/hive/warehouse/retail_db.db";


case class Order(order_id:Int, order_date:String, order_cust_id: Int, order_status:String)
def parseOrder(rec:String): Order ={
	
	val parts = rec.split(",");
	return Order(parts(0).toInt, parts(1), parts(2).toInt, parts(3))

}

case class OrderItem(order_item_id:Int, order_item_order_id: Int, order_item_prod_id: Int, order_item_quantity:Int, order_item_subtotal:Float, order_item_prod_price:Float);
def parseOrderItem(rec:String) : OrderItem = {

	val parts = rec.split(",");
	return OrderItem(parts(0).toInt, parts(1).toInt, parts(2).toInt, parts(3).toInt, parts(4).toFloat, parts(5).toFloat);
}

// Parse the records into corresponding objects
val orders = sc.textFile(path + "/orders").map(x => parseOrder(x));
val orderItems = sc.textFile(path + "/order_items").map(x => parseOrderItem(x));

// Generate KV pairs where K- order id, V = object
val ordersKV = orders.map(x => (x.order_id, x));
val orderItemsKV = orderItems.map(x => (x.order_item_order_id, x));

// Join on key
val ordersJoin = ordersKV.join(orderItemsKV);

// Generate a flat structure which contains K = order_id, order_date and V= subtotal
val ordersByOrderIdDate = ordersJoin.map(x => ((x._2._1.order_id,x._2._1.order_date), x._2._2.order_item_subtotal));

// Reduce by key to calculate orders total
val ordersTotal = ordersByOrderIdDate.reduceByKey( (acc,value) => acc + value);

// Generate the flat structure where K= order_date, V = totals
val ordersByDate = ordersTotal.map(x => (x._1._2, x._2))

// Again reduce by key to find total revenue and count per day. Since the input is just float (total) and output is revenue and count tuple(float, int) 
// we need to use aggregateByKey
val revenueAndCountPerDay = ordersByDate.aggregateByKey((0.0, 0) )( (acc, value) => (acc._1 + value, acc._2 + 1) , (acc, value) => (acc._1 + value._1, acc._2 + value._2));

revenueAndCountPerDay.take(10).foreach(println)


 
