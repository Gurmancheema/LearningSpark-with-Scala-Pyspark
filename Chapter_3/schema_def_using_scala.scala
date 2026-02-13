// importing packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

// defining a Scala singleton object with the main method serving as the JVM entry point

object schema_def {
  def main(args: Array[String]) {

    // creating a spark object

    val spark = SparkSession.builder.appName("schema_def").getOrCreate()

    // defining schema

    val schema = StructType(Array (StructField("id",IntegerType,false),
                                  StructField("first",StringType,false),
                                  StructField("last",StringType,false),
                                  StructField("url",StringType,false),
                                  StructField("published",StringType,false),
                                  StructField("hits",IntegerType,false),
                                  StructField("campaign",ArrayType(StringType),false)
                                  )
    )

    // static data
     val data =Seq(Row(1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, Seq("twitter","LinkedIn")),
                Row(2, "Brooke","Wenig", "https://tinyurl.2", "5/5/2018", 8908, Seq("twitter","LinkedIn")),
                Row(3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, Seq("web","twitter", "FB", "LinkedIn")),       Row(4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568,
  Seq("twitter", "FB")),
              Row(5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, Seq("web",
  "twitter", "FB", "LinkedIn")),
                Row(6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568,
  Seq("twitter", "LinkedIn"))
     )

    //putting together in a dataframe
    //  but first we need to parallelize the static data

    val rdd = spark.sparkContext.parallelize(data)

    // pass the rows to createDataFrame method
    val df = spark.createDataFrame(rdd,schema)

    // display dataframe
    df.show()

    //display schema

    df.printSchema()

    //stop the spark session

    spark.stop()
  }
}
