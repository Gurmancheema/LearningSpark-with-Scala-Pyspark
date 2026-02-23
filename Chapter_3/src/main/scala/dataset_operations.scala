//import packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// since we are working on datasets, dataset schema is defined using case class
// case class is defined outside the main method & before reference
// defining schema for the dataset

case class DeviceIotData (device_id : Long, device_name : String, ip : String, cca2 : String,
                              cca3 : String, cn : String, latitude : Double, longitude : Double,
                              scale : String, temp : Long, humidity :Long, battery_level : Long,
                              c02_level : Long, lcd : String, timestamp : Long ) 
case class Devicetempbycountry ( temp : Long, device_name : String, device_id : Long, cca3 : String)

// defining a singleton spark object containing the main method

object dataset_operations{
  def main(args:Array[String]){
    
    //check the datafile passed
    if (args.length != 1) {
      println("Please pass the correct datafile")
      sys.exit(1)
    }

    // load the data file path
    val data_file = args(0)

    // instantiating a spark object

    val spark = SparkSession.builder.appName("dataset_operations").getOrCreate()
    
    //import implicits
    import spark.implicits._
    // creating a dataset from defined schema class & loaded json datafile path

    val ds = spark.read.format("json").load(data_file).as[DeviceIotData]

    //print schema
    ds.printSchema()

    // **********************************DATASET OPERATIONS****************************
    //
    // 1. Return all rows where temp > 30 & humidity > 70

    val dstemphumidity = ds.filter((d:DeviceIotData) => (d.temp > 30 && d.humidity > 70) )

    dstemphumidity.show(5)  

    // verify the datatype of dataset

    println(dstemphumidity.getClass.getName)

    // 2. Mention alternative way to derive a dataset from a dataset

    

    val dstemp = ds.filter(d => d.temp > 25)
      .map( d=> (d.temp, d.device_name, d.device_id, d.cca3))
      .toDF("temp","device_name","device_id","cca3")
      .as[Devicetempbycountry]

      //print the dataset & schema
      dstemp.show(5)
      println(dstemp.printSchema)

    // 3. Semantically, select() is like map()
    // defining same dataset using select()

    val dstemp_2 = ds.select($"temp",$"device_name",$"device_id",$"cca3")
                     .where("temp >25")
                     .as[Devicetempbycountry]

    dstemp_2.show(5)
    println(dstemp_2.printSchema)
    
    // 4. Detect failing devices with battery levels below a threshold
    
    val battery_level_distinct_values = ds.select("battery_level")
                            .distinct()
                            .show()

    // taking mean of distinct values of battery levels to define a threshold

    val threshold_battery_level = ds.select("battery_level")
                                .distinct()
                                .agg(avg("battery_level").alias("threshold"))
                                .first()
                                .getDouble(0)

    // now using filter of threshold_battery_level to whole dataset to detect failing devices
  
    val failing_devices = ds.select("device_id","device_name","battery_level")
                            .where(col("battery_level") < threshold_battery_level)
                            .show(5)

    // 5. Identify offending countries with high levels of CO2 emissions.

    val disinct_co2_emissions = ds.select("c02_level").distinct().show()

    // taking a mean of distinct values of co2 level to define a threshold

    val avg_co2_level = ds.select("c02_level").distinct().agg(avg("c02_level")
                          .alias("threshold_c02_level"))
                          .first()
                          .getDouble(0)

    // now using filter of average co2 level to identify offending countries with high co2 levels

    val offending_countries = ds.select("cca3","c02_level")
                                .where(col("c02_level") > avg_co2_level)
                                .show()

    // 6. Compute the min and max values for temperature, battery level, CO2, and humidity.
    
    val min_temperature = ds.agg(min("temp").alias("minimum_temperature"),
                                 max("temp").alias("maximum_temperature"),
                                  min("c02_level").alias("minimum_c02_level"),
                                  max("c02_level").alias("maximum_c02_level"),
                                  min("humidity").alias("minimum_humidity_level"),
                                  max("humidity").alias("maximum_humidity_level"))
                            .show()
    //stop the spark session
    spark.stop()
  }
}
