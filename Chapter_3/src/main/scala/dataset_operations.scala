//importing packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

    // defining schema for dataset
    // unlike dataframes, dataset schema is defined using case class
    // pass all the columns & their respective datatypes in the form of key value pair
    // case class is not defined under main method, & should be defined before use
    // Spark cannot generate encoder for case classes defined inside methods.


case class DeviceIOTData ( device_id : Long, device_name : String, ip : String, cca2 : String,
                              cca3 : String, cn : String, latitude : Double, longitude : Double,
                              scale : String, temp : Long, humidity :Long, battery_level : Long,
                              c02_level : Long, lcd : String, timestamp : Long )

// defining a singleton spark object containing the main method

object dataset_exercise {
  def main(args:Array[String]) {

    //check file integrity
    if( args.length !=1) {
      print("Please pass the correct datafile")
      sys.exit(1)
    }
    
    //loading the datafile path
    val data_file = args(0)

    //instantiating spark object
    val spark = SparkSession.builder.appName("dataset_exercise").getOrCreate()
    
    // loading schema & json datafile into a dataset
    // importing implicits 

    import spark.implicits._

    val ds = spark.read.format("json").load(data_file).as[DeviceIOTData]

    // display dataframe & schema
     ds.show(5)
     ds.printSchema

     // stop the spark session
     spark.stop()

  }
}
