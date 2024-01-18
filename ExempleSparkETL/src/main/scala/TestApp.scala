import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SimpleApp {
	def main(args: Array[String]) {
		// Initialisation de Spark
		val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()
		
		val businessFile = "files/yelp_academic_dataset_business.json"
		
		// Chargement du fichier JSON
		var business = spark.read.json(businessFile).cache()

		business = business.drop(col(""))
		// Changement du type d'une colonne
		// explode? 

		business.printSchema()
		business.show(5)

		spark.stop()
	}
}
