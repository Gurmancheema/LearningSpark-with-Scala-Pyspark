// import packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// defining a singleton scala object containing the main method

object views_and_catalogs{
  def main(args:Array[String]) {

    // verify the datafile passed
    if (args.length < 1){
      println("Please pass the data file")
      sys.exit(1)
    }

    // load the datafile passed
    val data_file = args(0)

    // instantiating spark session
    val spark = SparkSession.builder.appName("views_catalogs").enableHiveSupport().getOrCreate()

    // define a schema for the dataframe
    // today defining schema using spark's structured datatype API
    
    val schema = StructType(Array (StructField("date", StringType, false),
                                    StructField("delay", IntegerType, false),
                                    StructField("distance",IntegerType, false),
                                    StructField("origin", StringType, false),
                                    StructField("destination", StringType, false))
                                )
    // load the data into a dataframe

    val df = spark.read.format("csv").option("header","true").schema(schema).load(data_file)

    // display the dataframe & schema to verify

    df.show(10)
    df.printSchema()

    // i can directly create a view from the dataframe
    // but since i am practicing numerous methods of reading in data & creating tables & views
    // i prefer to create a View from an existing table

    // list the current databases in use & then the list of tables

    spark.sql("""SHOW DATABASES""").show()
    spark.sql("""USE DATABASE learn_spark_db""").show()
    spark.sql("""SHOW TABLES""").show(false)



    // *********************** CREATING & QUERYING GLOBAL TEMP VIEWS *************************************

    // i have a managed table in my database created from last spark application
    // i will use that table to create a subset of data in the form of a VIEW

    spark.sql(""" CREATE OR REPLACE GLOBAL TEMP VIEW global_temp_view_flight_delays AS
                  SELECT date, delay, origin, destination 
                  FROM df_api_managed_us_flight_delays
                  WHERE origin = 'SFO'""")

    // run SELECT query on the view to verify
    // since i created a "GLOBAL TEMP VIEW", i have to specify the view with prefix "global_temp"
    // while querying the global temp view as it resides in global_temp directory in Spark

    spark.sql("""SELECT * FROM global_temp.global_temp_view_flight_delays""").show(10,false)

    // let's print the schema of view as we would in table
    spark.sql("""DESCRIBE global_temp.global_temp_view_flight_delays""").show()

    // querying the same view using SCALA/PYTHON (PYSPARK) will be like
    // it will return a dataframe

    spark.read.table("global_temp.global_temp_view_flight_delays").show(12)



    // ************************** CREATING & QUERYING TEMP VIEWS **********************************

    // Quite similar to global temp views, no need to write prefix while creation or querying

    spark.sql(""" CREATE OR REPLACE TEMP VIEW temp_view_flight_delays AS
                  SELECT date, delay, origin, destination
                  FROM df_api_managed_us_flight_delays
                  WHERE origin = 'JFK'""")

    spark.sql("""SELECT * FROM temp_view_flight_delays""").show(10,false)
    spark.sql("""DESCRIBE temp_view_flight_delays""").show()

    // querying the same view in SCALA OR PYTHON(PYSPARK) will be like
    // it will return a dataframe

    spark.read.table("temp_view_flight_delays").show(10)


    // ******************************* DROPPING VIEWS **********************************

    spark.sql(""" DROP VIEW IF EXISTS global_temp.global_temp_view_flight_delays""")
    spark.sql(""" SHOW TABLES""").show()

    // dropping view using SCALA/PYTHON (PYSPARK) 

    spark.catalog.dropTempView("temp_view_flight_delays")
    spark.sql("""SHOW TABLES""").show()

    // ************************* CATALOGS FOR METADATA ******************************************
    
    // Catalog is a high-level abstraction in Spark SQL for storing the metadata

    spark.catalog.listDatabases().show(false)
    spark.catalog.listTables().show(false)
    spark.catalog.listColumns("df_api_managed_us_flight_delays").show(false)





    // stop the spark session
    spark.stop()
  }
}
