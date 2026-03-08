// import packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// defining a singleton scala object containing the main method

object df_operations{
  def main(args:Array[String]){

    // instantiating a sparksession
    val spark = SparkSession.builder.appName("df_operations").getOrCreate()

    // since we are loading two seperate datasets to work on 
    // therefore, defining schema & loading the data into a dataframes

    // **************************** PART-1 LOADING DATAFILES INTO DATAFRAMES ***********************************
    
    // defining a schema for airport information data

    val airport_schema = StructType(Array(StructField("city",StringType,true),
                                          StructField("state",StringType,true),
                                          StructField("country",StringType,true),
                                          StructField("IATA",StringType,true)
                                          ))
    // reading the data file path into a variable
    val airports_data_path = "data/airportsna.txt"

    // forming a dataframe 
    val airport_df = spark.read.format("csv").option("header",true).option("delimiter","\t").schema(airport_schema)
                                              .load(airports_data_path)

    // display dataframe & printschema to verify

    airport_df.show()
    airport_df.printSchema()

    // ---------------------------------------------------------------------------------------------------------------
    
    // defining schema for the departureflight delays dataset\
    
    val delayed_flights_schema = StructType(Array(StructField("date",IntegerType,false),
                                                  StructField("delay",IntegerType,false),
                                                  StructField("distance",StringType,false),
                                                  StructField("origin",StringType,false),
                                                  StructField("destination",StringType,false)
                                                  ))

    // loading the data file path into a variable

    val delayed_flights_data_path = "data/departuredelays.csv"

    // loading the data into a dataframe using the defined schema

    val delayed_flights_df = spark.read.format("csv").option("header","true").schema(delayed_flights_schema)
                                                      .load(delayed_flights_data_path)

    // display the dataframe & schema to verify

    delayed_flights_df.show()
    delayed_flights_df.printSchema()


    // stop the spark session
    spark.stop()
  }
}
