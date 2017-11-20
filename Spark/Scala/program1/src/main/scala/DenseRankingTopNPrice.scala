import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext._;
import org.apache.spark.SparkConf;
import org.apache.spark.rdd._;

object DenseRankingTopNPrice {

	case class Product (id:Int, cat_id:Int, name:String, desc:String, price:Float, url:String = "");

	def main(args : Array[String] ){

		
		val conf = new SparkConf().setAppName("DenseRanking").setMaster("local");
		val sc = new SparkContext(conf);

	
		val products = sc.textFile("/user/hive/warehouse/retail_db.db/products");
		
		val productsKV = products.map(x => parseRec(x));

		val groupedByCat = productsKV.groupByKey();

		val topNProductPricePerCat = groupedByCat.flatMapValues(v => getDenseTopN(v, 2));

		topNProductPricePerCat.collect().foreach(println);

	}

	def parseRec(rec: String): (Int, Product) = {
	
		val parts = rec.split(",");
		(parts(1).toInt, Product(parts(0).toInt, parts(1).toInt, parts(2), parts(3),parts(4).toFloat));

	}


	def getDenseTopN(values :Iterable[Product], topN: Int) : Iterable[Product] = {
		
		val listProducts = values.toList;
		val topNPrices = listProducts.map(x => x.price).distinct.sortBy(x => -x).take(topN);
		var result:List[Product] = List();

		for(p <- listProducts){

			if(topNPrices.contains(p.price)){

				result = result :+ p;
			}

		}	
		
		return result.sortBy(-_.price);	
	}

}
