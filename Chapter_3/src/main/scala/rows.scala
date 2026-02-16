//importing packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

//defining a spark singleton object which contains the main method

object rows_concept{
  def main(args: Array[String]) {

    //instantiating a spark object

    val spark = SparkSession.builder.appName("rows_concept").getOrCreate()

    //imporing spark implicits as it is not static package
    //implicits belong to a SparkSession instance
    
    import spark.implicits._

    // create a row relative to blogs.json dataset residing in data directory

    val sample_blogs_row = Row(10,"Jeona","morhh","https://tinyurl.1.7","2/16/2026","34000",Array("twitter","linkedin"))

    //accessing the item in row using index
    // row index starts from 0
     println(sample_blogs_row(2))
    
    // creating a row object

    val df_rows = Seq(("James","California","fiction"),("Clarke","Washington","science"))

    // creating dataframe from row object

    val df = df_rows.toDF("Name","State","Genre")

    //display the dataframe
    df.show()
     //stop the spark session

     spark.stop()
  }
}
