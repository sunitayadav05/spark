import com.sun.jdi.LongType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object MovieWithCore {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils");

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("Movie Rating using core API").master("local").getOrCreate()

    val ownSchema = StructType(
      StructField("Userid",IntegerType,true) ::
        StructField("moviedid",IntegerType,true) ::
        StructField("ratings",DoubleType,true) ::
        StructField("timestamp",StringType,true) :: Nil )

    val df = spark.read.format("csv").schema(ownSchema).option("header","true").load("C:\\Users\\sunita.yadav\\Desktop\\Proj\\RampUp\\excercise_files\\excercise_files\\ratings.csv")



    //df.head(10).foreach(println)






    println("1. List all the movies and the number of ratings")
    val df1 = df.select("moviedid").groupBy("moviedid").count()
    df1.show()

    println("2. List all the users and the number of ratings they have done for a movie")
    val df2 = df.select("Userid").groupBy("Userid").count()
    df2.show()

    println("3. List all the Movie IDs which have been rated (Movie Id with atleast one user rating it)")
    val df3 = df1.select("moviedid").where("count > 0")
    df3.show()

    println("4. List all the Users who have rated the movies (Users who have rated atleast one movie)")
    val df4 = df2.select("Userid").where("count > 0")
    df4.show()

    println("5. List of all the User with the max,min,average ratings they have given against any movie")
    df.printSchema()
    val df5 = df.select("Userid","ratings").groupBy("Userid").agg(avg(df("ratings")),max(df("ratings")),min(df("ratings")))
    df5.show()

   println("6. List all the Movies with the max,min,average ratings given by any user")
    val df6 = df.select("moviedid","ratings").groupBy("moviedid").agg(avg(df("ratings")),max(df("ratings")),min(df("ratings")));
    df6.show()




  }


}
