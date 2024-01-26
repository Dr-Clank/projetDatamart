import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TestApp {
	def main(args: Array[String]) {
		// Initialisation de Spark
		val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()
		
		// //Paramètres de la connexion BD PostgreSQL
        // Class.forName("org.postgresql.Driver")
        // val url = "jdbc:postgresql://stendhal:5432/tpid2020"
        // import java.util.Properties
        // val connectionProperties = new Properties()
        // connectionProperties.setProperty("driver", "org.postgresql.Driver")
        // connectionProperties.setProperty("user", "tpid")
        // connectionProperties.setProperty("password","tpid")
        
        // // Enregistrement du DataFrame reviews dans la table "review"
        // var req = "(select review_id, business_id, date, stars from yelp.review) as q1"
        // var reviews = spark.read.jdbc(url, req, connectionProperties)
        // reviews = reviews.withColumn("date", col("date").cast(DateType))
        

		val businessFile = "files/yelp_academic_dataset_business.json"
		
		// Chargement du fichier JSON
		var business = spark.read.json(businessFile).cache()
		
        var categories = business.select("business_id","categories")
        var splitedCate = business.sql("select split(categories, ',') as category")

        categories.printSchema(20)
        categories.show(20)
        //splitedCate.show(20)
		
		// Suppression de la colonne "friends" dans le DataFrame users
		// users = users.drop(col("friends"))

		spark.stop()
	}
}
