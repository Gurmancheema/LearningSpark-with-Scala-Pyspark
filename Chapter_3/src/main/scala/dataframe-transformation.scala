//importing packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//  defining a singleton object containing the main method

object dataframe_practice{
  def main(args: Array[String]) {

    //check the datafile passed
    if (args.length < 1) {
      println("Please pass the correct datafile")
      sys.exit(1)
    }

    //loading the data file path

    val data_file = args(0)

    // instantiating spark object

    val spark = SparkSession.builder.appName("dataframe_prac").getOrCreate()

    // letting spark infer the schema this time
    // will check the spark integrity & performance of inferring schema
    // therefore loading the file staright to dataframe

    val df = spark.read.format("csv").option("header","true").option("inferschema","true").load(data_file)

    //display the dataframe & print schema
    df.show(5,truncate =true)
    println(df.printSchema)

    // since spark inferred most of the datatypes as StringType
    // transformation of datatype of some columns are required
    // transformed dataframe will be saved as parquet file & frther used to perform queries

    val new_df = df.withColumn("OnCallDate", to_date(col("CallDate"),"MM/dd/yyyy")).drop("CallDate")
                    .withColumn("OnWatchDate", to_date(col("WatchDate"),"MM/dd/yyyy")).drop("WatchDate")
                    .withColumn("AvailableDtTm", to_timestamp(col("AvailableDtTm"),"MM/dd/yyyy hh:mm:ss a"))
    new_df.printSchema
    
    new_df.write.mode("overwrite").parquet("results/cleaned-sf-fire-calls")
    //stop the spark session
    spark.stop()
  }
}
