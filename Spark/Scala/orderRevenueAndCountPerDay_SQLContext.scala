import org.apache.spark._;
import org.apache.spark.sql._;

val conf = new SparkConf().setMaster("local").setAppName("OrderRevenueAndCountPerDay SQLContext Scala");
val sc = new SparkContext(conf);

val sqlContext = new SQLContext(sc);
sqlContext.sql("set spark.sql.shuffle.partitions = 5");

// NOTE: The following import is very important to implicity convert any standard RDD containing case classes to SchemaRDD
// which is required to create Temp tables for the RDD.
import sqlContext.createSchemaRDD;

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


// Register the RDDs as Temp Table
orders.registerTempTable("orders");
orderItems.registerTempTable("order_items");

val orderRevAndCountPerDay = sqlContext.sql(
        """SELECT order_date, SUM(order_total) as revenue, count(*) FROM
        (
                 select o.order_id as order_id, o.order_date AS order_date, sum(oi.order_item_subtotal) as order_total
                 from orders o JOIN order_items oi ON o.order_id = oi.order_item_order_id
                 GROUP BY o.order_id, o.order_date
        ) ordersJoin
        GROUP BY order_date""");

orderRevAndCountPerDay.take(10).foreach(println)
