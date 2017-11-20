import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object Top3ProductsByPricePerCat {

	case class Product(id:Int, cat_id:Int, name:String, desc:String, price:Float, url:String = "");

	def main(args : Array[String]) {

		val conf = new SparkConf().setAppName("Top3ProductsByPricePerCat").setMaster("local");
		val sc = new SparkContext(conf);

		val productsRDD = sc.textFile("/user/hive/warehouse/retail_db.db/products");
		val productsRec:RDD[(Product)] = productsRDD.map(x => parseRec(x));

		val groupedByCat = productsRec.groupBy(x => x.cat_id);

		val top3ProductByPricePerCat = groupedByCat.flatMap(x => x._2.toList.sortBy(p => -p.price).take(3));

		top3ProductByPricePerCat.collect().foreach(println);
		
	}

	def parseRec(rec : String): Product = {

		val parts = rec.split(",");
		Product(parts(0).toInt, parts(1).toInt, parts(2), parts(3), parts(4).toFloat);
	}
}
