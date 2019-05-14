

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object movieWithSparkSql {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils");

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("Movie Rating").master("local").getOrCreate()

    val df = spark.read.format("csv").option("header","true").load("C:\\Users\\sunita.yadav\\Desktop\\Proj\\RampUp\\excercise_files\\excercise_files\\ratings.csv")

    df.show()

    df.createOrReplaceTempView("movies")

    println("1. List all the movies and the number of ratings")
    val query1 = spark.sql("select moviedid,count(ratings) from movies group by moviedid")
    query1.show()



    println("2. List all the users and the number of ratings they have done for a movie")
    val query2 = spark.sql("select Userid,count(ratings) from movies group by Userid")
    query2.show()

    println("3. List all the Movie IDs which have been rated (Movie Id with atleast one user rating it)")
    val query3 = spark.sql("select moviedid from (select moviedid,count(ratings) from movies group by moviedid having count(ratings) > 0 )abc")
    query3.show()

    println("4. List all the Users who have rated the movies (Users who have rated atleast one movie)")
    val query4 = spark.sql("select Userid from(select Userid,count(ratings) from movies group by Userid having count(ratings) > 0 )abc")
    query4.show()


    println("5. List of all the User with the max,min,average ratings they have given against any movie")
    val query5 = spark.sql("select Userid,max(ratings),min(ratings),avg(ratings) from movies group by Userid")
    query5.show()

    println("6. List all the Movies with the max,min,average ratings given by any user")
    val query6 = spark.sql("select moviedid,max(ratings),min(ratings),avg(ratings) from movies group by moviedid")
    query6.show()
  }

}
