//   import packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//defining a singleton scala object containing the main method

object sparksql_operations {
  def main(args:Array[String]) {

    // check the files passed
    if (args.length != 1) {
      println("Please pass the correct files")
      sys.exit(1)
    }

    // loading the file path 
    val data_file = args(0)

    // instantiating a spark session
    val spark = SparkSession.builder.appName("sparksql").getOrCreate()

    //defining schema using DDL

    val my_schema = "date DATE, delay INT, distance INT, origin STRING, destination STRING"

    // ********************* METADATA******************

    // The US flight delays data set has five columns:
    // The date column contains a string like 02190925. When converted, this maps to
    // 02-19 09:25 am.
    // The delay column gives the delay in minutes between the scheduled and actual
    //  departure times. Early departures show negative numbers.
    //  The distance column gives the distance in miles from the origin airport to the
    //  destination airport.
    //  The origin column contains the origin IATA airport code.
    //  The destination column contains the destination IATA airport code.

    // loading the datafile into a dataframe

    // val df = spark.read.format("csv").schema(my_schema).load(data_file)

    // since the date column is loading NULL values, spark is not able to parse the data correctly
    // therefore, loading the date column as "STRING" first then transform to DATE
    
    val updated_schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"

    val df = spark.read.format("csv").option("header","true").schema(updated_schema).load(data_file)
    
     //display the dataframe using sparksql

    df.show()
    df.printSchema()

    // converting the datatype of "date" column from STRING to DATE format

    val updated_df = df.withColumn("new_date", to_timestamp(col("date"), "MMddHHmm"))
    updated_df.printSchema()

    // Converting dataframe to SQL view
    
    updated_df.createOrReplaceTempView("us_flight_departure_delays")

    // ************************perform SPARKSQL operations on view*************************

    // 1. find all flights whose distance is greater than 1,000 miles

    val flights   = spark.sql("""SELECT * 
                                FROM us_flight_departure_delays
                                WHERE distance >1000
                                ORDER BY distance DESC""").show(10)


    // 2 .find all flights between San Francisco (SFO) and Chicago (ORD) with at least a two-hour delay:
    
    val flights_bw_sfo_ord = spark.sql("""SELECT * 
                                          FROM us_flight_departure_delays
                                          WHERE origin = 'SFO' 
                                          AND destination = 'ORD'
                                          AND delay >= 120
                                          ORDER BY delay DESC""").show(10)


    // 3. find the months when the delays were most common

    val months_with_most_delays = spark.sql(""" SELECT MONTH(new_date) AS delayed_months,
                                                COUNT("*") AS total_delays
                                                FROM us_flight_departure_delays
                                                WHERE origin = 'SFO' 
                                                AND destination = 'ORD'
                                                AND delay >= 120
                                                GROUP BY MONTH(new_date)
                                                ORDER BY total_delays DESC """).show(10)












    //stop the spark session

    spark.stop()
  }
}
