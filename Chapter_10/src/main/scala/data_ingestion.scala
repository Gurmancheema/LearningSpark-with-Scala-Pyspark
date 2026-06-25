//import packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// creating a singleton scala object containing the main method

object data_ingestion{
  def main(args:Array[String]) {

    // creating a spark session
    val spark = SparkSession.builder().appName("data_ingestion").getOrCreate()

    // readding data from the source
    
    val data_source = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_10/data"

    // display the data & schema to verify

    val df = spark.read.format("parquet").option("inferschema","true").load(data_source)

    df.select("neighbourhood_cleansed","room_type","bedrooms","bathrooms","number_of_reviews","price").show(5)
    println(s"Total no. of rows: ${df.count()}")
    df.printSchema()

    // split the data into train / test set

    val Array(train, test) = df.randomSplit(Array(.8,.2), seed = 42)

    //verify
    println(s"No. of rows in training set: ${train.count()} and No. of rows in test set: ${test.count()}")
    
    // saving the data split for further operations
    val train_data_path = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_10/data/train/"
    val test_data_path = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_10/data/test/"
  
    try {
      train.write.mode("overwrite").parquet(train_data_path)
      test.write.mode("overwrite").parquet(test_data_path)
      println("data split saved successfully")
    }
    catch {
      case e:Exception => 
        println(s"failed ===>, ${e.getMessage}")
      }
    // stop the spark sessionnn
    spark.stop()
  }
}
