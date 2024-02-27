import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object DataApp {
	def main(args: Array[String]) {
		// Initialisation de Spark
		val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()


// /*******************************************************************
// * Postgresql
// *******************************************************************/


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
		var dim_users = spark.read.jdbc(url, req, connectionProperties).toDF("USER_ID","NAME", "USEFUL","COOL","FANS", "FUNNY", "REVIEW_COUNT", "AVERAGE_STAR")

		//dim_users = dim_users.withColumn("unique_id", monotonically_increasing_id()+1)

		//dim_users.show(1)

	/* PostgresSQL Table Fait Review */

		// Enregistrement du DataFrame reviews dans la table "review"
		var req1 = "(select review_id,business_id,user_id,date,stars,useful,cool from yelp.review) as q2"
		var fait_reviews = spark.read.jdbc(url, req1, connectionProperties).toDF("REVIEW_ID", "BUSINESS_ID", "USER_ID", "DATE", "STARS", "USEFUL", "COOL")
        fait_reviews = fait_reviews.withColumn("DATE", col("DATE").cast(DateType))

		//fait_reviews.show(1)

/*******************************************************************
* JSON
*******************************************************************/

		// Chargement du fichier JSON
		val businessFile = "files/yelp_academic_dataset_business.json"
		val checkinFile = "files/yelp_academic_dataset_checkin.json"

		var json_business = spark.read.json(businessFile).cache()
		var dim_checkin = spark.read.json(checkinFile).cache()

	// /* JSON Table Dim Localisation */

	 	var location = json_business.select("business_id","address","city","state","postal_code","latitude","longitude").toDF("BUSINESS_ID", "ADRESSE", "CITY", "STATE", "POSTAL_CODE", "LATITUDE", "LONGITUDE")

	// 	location = location.withColumn("unique_id", monotonically_increasing_id()+1)

	// 	location.show(1)

	// /* JSON Table Dim Checkin */

		dim_checkin = dim_checkin.withColumn("date", explode(org.apache.spark.sql.functions.split(col("date"), ","))).toDF("BUSINESS_ID","DATE")
		dim_checkin.withColumn("DATE_VALUE", col("DATE").cast(DateType))

	// 	dim_checkin = dim_checkin.withColumn("unique_id", monotonically_increasing_id()+1)

	// 	dim_checkin.show(1)

	// /* JSON Table Dim Categorie */

		var dim_category = json_business.select("business_id","categories").toDF("BUSINESS_ID", "CATEGORIES")
		dim_category = dim_category.withColumn("CATEGORIES", explode(org.apache.spark.sql.functions.split(col("CATEGORIES"), ",")))
		dim_category = dim_category.withColumnRenamed("CATEGORIES", "CATEGORY")
		dim_category = dim_category.withColumn("CATEGORY",lower(trim(col("CATEGORY"))))

	// 	dim_category = dim_category.withColumn("unique_id", monotonically_increasing_id()+1)

	// 	dim_category.show(1)

	/* JSON Table Dim business */

		var dim_business = json_business.select("business_id","name","stars","review_count","is_open").toDF("BUSINESS_ID","NAME", "STARS", "REVIEW_COUNT", "IS_OPEN")

		//dim_business.show(1)

// /******************************************************************
// * CSV	
// *******************************************************************/
		val foodCategoCsvFile = "files/category.csv"
		var food_filtered_catego = spark.read.option("header","true").csv(foodCategoCsvFile).cache()

		var dim_food_category = food_filtered_catego.select("Category").toDF("FOOD_CATEGORY")

		//dim_csv_business = dim_csv_business.withColumn("unique_id", monotonically_increasing_id()+1)


		import java.util.Properties
		// // Paramètres de la connexion BD
		Class.forName("oracle.jdbc.driver.OracleDriver")
        val url2 = "jdbc:oracle:thin:@stendhal:1521:enss2023"
        val connectionProperties2 = new Properties()
        connectionProperties2.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
        connectionProperties2.setProperty("user", "mo407000")
        connectionProperties2.setProperty("password","mo407000")
		
		dim_business.write.mode(SaveMode.Overwrite).jdbc(url2, "BUSINESS", connectionProperties2)

		fait_reviews.write.mode(SaveMode.Overwrite).jdbc(url2, "FACT_REVIEW", connectionProperties2)

		dim_users.write.mode(SaveMode.Overwrite).jdbc(url2, "USER_YELP", connectionProperties2)

		dim_category.write.mode(SaveMode.Overwrite).jdbc(url2, "CATEGORY", connectionProperties2)

		location.write.mode(SaveMode.Overwrite).jdbc(url2, "LOCATION", connectionProperties2)

		dim_checkin.write.mode(SaveMode.Overwrite).jdbc(url2, "CHECKIN", connectionProperties2)

		dim_food_category.write.mode(SaveMode.Overwrite).jdbc(url2, "FOOD_CATEGORY", connectionProperties2)


		spark.stop()
		
	}
}
