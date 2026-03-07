// import packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// defining a singleton scala object containing the main method

object creating_db_tables {
  def main (args:Array[String]) {
    if (args.length < 1) {
      println("Please pass the correct datafile")
      sys.exit(1)
    }

    // load the datafile path which will be further saved as table in DB

    val data_file = args(0)

    // instantiating the spark session with HIVE support enabled
    // as i want spark to use HIVE libraries support to create database & store metadata
    
    val spark =  SparkSession.builder.appName("creating_db_tables").enableHiveSupport().getOrCreate()

    // creating a database using SparkSQL

    spark.sql("""CREATE DATABASE IF NOT EXISTS learn_spark_db""")
    
    // selecting the database to use for saving our progress

    spark.sql(""" USE DATABASE learn_spark_db""")

    // 1. Creating managed table using sparksql

    spark.sql(""" CREATE TABLE IF NOT EXISTS managed_us_flights_delays(date STRING, delay INT, distance INT,
                  origin STRING, destination STRING)""")

    // 2. Verify the database & table creation
    
    spark.sql("SHOW DATABASES").show()
    spark.sql("SHOW TABLES").show()

    println("Everything ran successfully")

    // 3. Creating a managed table using Dataframe API
    // loading the datafile into a dataframe first with a defined schema
    // defining schema using DDL statement

    val schema = (" date STRING, delay INT, distance INT, origin STRING, destination STRING")

    val df = spark.read.format("csv").option("header","true").schema(schema).load(data_file)

    // display the dataframe along with schema
    df.show(10)
    df.printSchema()

    // saving this dataframe as a managed table using Dataframe API
    df.write.mode("overwrite").saveAsTable("df_api_managed_us_flight_delays")

    // now again check the databases & tables inside it
    // it should have 2 tables by now
    // One created from SparkSQL, other created from Dataframe API
  
    spark.sql("SHOW DATABASES").show()
    spark.sql("SHOW TABLES").show()

    //query the empty table
    spark.sql("""SELECT * FROM managed_us_flights_delays""").show()

    // 4. Creating an unmanaged/external table from datasource
    // creating using sparksql
    // since i am passing the data file path's variable, SparkSQL needs a SQL string to parse the path
    // therefore, i will interpolate the path stored in "data_file" variable

    println(new java.io.File(".").getAbsolutePath)

    spark.sql(s""" CREATE TABLE IF NOT EXISTS unmanaged_us_flight_delays(date STRING, delay INT, distance INT, origin                                                                           STRING, destination STRING)
                  USING csv OPTIONS ('PATH' '$data_file') """)

    // verify table creating in db
    spark.sql("""SHOW TABLES""")

    // creating unmanaged/external table using Dataframe API
    // since we are giving the path of table storage;
    // it falls in unmanaged/external table category

    val df_2 = spark.read.format("csv").option("header","true").schema(schema).load(data_file)

    df_2.write.mode("overwrite").option("path","tmp/data/us_flights_delays")
                                .saveAsTable("df_api_unmanaged_us_flights_delays")

    // verify table existence in database
    // verify the type of table
  
   spark.sql("DESCRIBE EXTENDED df_api_unmanaged_us_flights_delays").show(false) 
   spark.sql("""SHOW TABLES""").show()
    


    

  


    // stop the spark session
    spark.stop()
  }
}

