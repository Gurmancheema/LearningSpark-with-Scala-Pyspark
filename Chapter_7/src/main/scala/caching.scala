// import packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// defining a singleton scala object containing the main method

object caching{
  def main(args:Array[String]){

    // instantiating a spark session

    val spark = SparkSession.build.appName("caching").getOrCreate()

    // creating a static dataframe of 1 million records
    // having another column as square of primary columns 
    // ranging from 1 to  million
    
    // importing spark implicits

    import spark.implicits._

    val df = spark.range(1,1000000).toDF("id").withColumn("squared_ids", col("id") * col("id"))

    // verify the dataframe

    df.show()
    df.printSchema()

    // caching the dataframe
    df.cache()

    // as caching is a lazy operation, therefore to materialize it, let's call a trigger operation
    // also taking a note on time take to count the dataframe

    val start_first_time_count = System.currentTimeMillis()
    df.count()
    val end_first_time_count =  System.currentTimeMillis()

    println("Total time taken for initial count (ms) :" + end_first_time_count - start_first_time_count)

    // now the dataframe is cached
    // let's perform the action on cached dataframe again

    val start_second_time_count = System.currentTimeMillis()
    df.count()
    val end_second_time_count = System.currentTimeMillis()

    println(s"Total time taken for final count from cached dataframe (ms) : ($end_second_time_count - $start_second_time_count)")
    // creating a timer to stop the spark session after 2 mins

    Thread.sleep(120000)

    // stop the spark session
    spark.stop()
  }
}


