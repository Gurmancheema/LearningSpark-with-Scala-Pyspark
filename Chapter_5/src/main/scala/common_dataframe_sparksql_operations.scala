// import packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


//***************************** EXERCISE OBJECTIVE OVERVIEW ***************************************
//
//  1. Import two files and create two DataFrames, one for airport (airportsna) information
//      and one for US flight delays (departureDelays).
//
//  2. Using expr(), convert the delay and distance columns from STRING to INT.
//
//  3. Create a smaller table, foo, that we can focus on for our demo examples; it contains
//      only information on three flights originating from Seattle (SEA) to the destination
//      of San Francisco (SFO) for a small time range.

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

    // column datatype conversion
    // "distance" column from STRING to INT

    val updated_flights_df = delayed_flights_df.withColumn("distance",col("distance").cast("int"))


    // ***************************** PART-2 CREATING TEMPORARY VIEWS FROM THE DATASETS ****************************
    
    val foo = updated_flights_df.filter((col("origin") === "SEA")
                                        && (col("destination") === "SFO")
                                        && (col("date").like("1010%"))
                                        && (col("delay") > 0))

    foo.show()
    println(s"Shape: (${foo.count()}, ${foo.columns.length})")


    // ******************************* PART-3 COMMON OPERATIONS *****************************************
    
    //  1. ----------------------- UNION OPERATION --------------------------------------
    
    // union two dataframes with same schema together into one dataframe

    val union_flight_dfs = updated_flights_df.union(foo)
    union_flight_dfs.show()

    println(s"Shape of resulting dataframe after union: ,(${union_flight_dfs.count()}, ${union_flight_dfs.columns.length})")


   // verifying the union of dataframes by other way
   // creating a new subset dataframe following the same conditions of foo dataframe

   val verify_df =  union_flight_dfs.filter((col("origin") === "SEA") &&
                                     (col("destination") === "SFO") &&
                                     (col("date").like("1010%")) &&
                                     (col("delay") > 0))
                                    .distinct()

   verify_df.show()









    // stop the spark session
    spark.stop()
  }
}
