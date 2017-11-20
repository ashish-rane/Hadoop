import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SimpleApp
{

	def main(args : Array[String]){

		val conf = new SparkConf().setAppName("Scala Spark")
		val sc = new SparkContext(conf)

		val dataRDD = sc.textFile("/user/hive/warehouse/retail_db.db/departments")

		dataRDD.saveAsTextFile("/user/cloudera/output/itversity/spark/simpleapp01")
	}

}
