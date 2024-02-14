import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object DataApp {
	def main(args: Array[String]) {
		// Initialisation de Spark
		val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()


/*******************************************************************
* Postgresql
*******************************************************************/


		// Paramètres de la connexion BD
		Class.forName("org.postgresql.Driver")
		val url = "jdbc:postgresql://stendhal:5432/tpid2020"
		import java.util.Properties
		val connectionProperties = new Properties()
		connectionProperties.setProperty("driver", "org.postgresql.Driver")
		connectionProperties.setProperty("user", "tpid")
		connectionProperties.setProperty("password","tpid")

	/* PostgresSQL Table Dim User */			

		// Enregistrement du DataFrame users dans la table "user"
		var req = "(select user_id,name,useful,cool,fans,funny,review_count,average_stars from yelp.user) as q1"
		var dim_users = spark.read.jdbc(url, req, connectionProperties)

		dim_users = dim_users.withColumn("unique_id", monotonically_increasing_id()+1)

	/* PostgresSQL Table Fait Review */

		// Enregistrement du DataFrame reviews dans la table "review"
		var req1 = "(select review_id,business_id,user_id,date,stars,useful,cool from yelp.review) as q2"
		var fait_reviews = spark.read.jdbc(url, req1, connectionProperties)
        fait_reviews = fait_reviews.withColumn("date", col("date").cast(DateType))

		fait_reviews = fait_reviews.withColumn("unique_id", monotonically_increasing_id()+1)

/*******************************************************************
* JSON
*******************************************************************/

		// Chargement du fichier JSON
		val businessFile = "files/yelp_academic_dataset_business.json"
		val checkinFile = "files/yelp_academic_dataset_checkin.json"

		var json_business = spark.read.json(businessFile).cache()
		var dim_checkin = spark.read.json(checkinFile).cache()

	/* JSON Table Dim Localisation */

		var location = json_business.select("business_id","address","city","state","postal_code","latitude","longitude")

		location = location.withColumn("unique_id", monotonically_increasing_id()+1)

	/* JSON Table Dim Checkin */

		dim_checkin = dim_checkin.withColumn("date", explode(org.apache.spark.sql.functions.split(col("date"), ",")))
		dim_checkin.withColumn("date", col("date").cast(DateType))

		dim_checkin = dim_checkin.withColumn("unique_id", monotonically_increasing_id()+1)

	/* JSON Table Dim Categorie */

		var dim_category = json_business.select("business_id","categories")
		dim_category = dim_category.withColumn("categories", explode(org.apache.spark.sql.functions.split(col("categories"), ",")))
		dim_category = dim_category.withColumnRenamed("categories", "category")

		dim_category = dim_category.withColumn("unique_id", monotonically_increasing_id()+1)

	/* JSON Table Dim business */

		var dim_business = json_business.select("business_id","name","stars","review_count","is_open")
	
		dim_business = dim_business.withColumn("unique_id", monotonically_increasing_id()+1)

/******************************************************************
* CSV	
*******************************************************************/
		val businessCsvFile = "files/yelp_academic_dataset_tip.csv"
		//dim_users.show(30)
		//|_c0|_c1|_c2|_C3|_c4|
		// |business_id|compliment_count|date|text|user_id|
		var csv_business = spark.read.option("header","true").csv(businessCsvFile).cache()

		var dim_csv_business = csv_business.select("business_id","compliment_count","date","user_id")

		dim_csv_business = dim_csv_business.withColumn("unique_id", monotonically_increasing_id()+1)

		dim_csv_business.show(5)
		spark.stop()
		
	}
}
