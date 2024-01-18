import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object DataApp {
	def main(args: Array[String]) {
		// Initialisation de Spark
		val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()

		/* PostgresSQL Table User */

		// Paramètres de la connexion BD
		Class.forName("org.postgresql.Driver")
		val url = "jdbc:postgresql://stendhal:5432/tpid2020"
		import java.util.Properties
		val connectionProperties = new Properties()
		connectionProperties.setProperty("driver", "org.postgresql.Driver")
		connectionProperties.setProperty("user", "tpid")
		connectionProperties.setProperty("password","tpid")
				

		// Enregistrement du DataFrame users dans la table "user"
		var req = "(select user_id,name,useful,cool,fans,funny,review_count,average_stars from yelp.user) as q1"
		var dim_users = spark.read.jdbc(url, req, connectionProperties)


		// Enregistrement du DataFrame reviews dans la table "review"
		var req1 = "(select review_id,business_id,user_id,date,stars,useful,cool from yelp.review) as q2"
		var fait_reviews = spark.read.jdbc(url, req1, connectionProperties)
        fait_reviews = fait_reviews.withColumn("date", col("date").cast(DateType))

		fait_reviews.printSchema()
		fait_reviews.show(5)


		spark.stop()
		
	}
}
