// importing packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// I'll be using the sf-fire-calls.csv datafile for all the dataframe operations
// data file residing in data directory

// defining a singleton scala object containing the main method

object dataframe_operations{
  def main(args: Array[String]){

    //check the datafile
    if (args.length <1) {
      println("Please pass the datafile")
      sys.exit(1)
    }

    //instantiating the spark object
    val spark = SparkSession.builder.appName("dataframe_operation").getOrCreate()
    
    //loading the datafile
    val data_file = args(0)

    //since the datafile is large, it's better to define the schema beforehand
    val my_schema = StructType(Array(StructField("CallNumber",IntegerType,false),
                            StructField("UnitID",StringType,false),
                            StructField("IncidentNumber",IntegerType,false),
                            StructField("CallType",StringType,false),
                            StructField("CallDate",StringType,false),
                            StructField("WatchDate",StringType,false),
                            StructField("CallFinalDisposition",StringType,false),
                            StructField("AvailableDtTm",StringType,false),
                            StructField("Address",StringType,false),
                            StructField("City",StringType,false),
                            StructField("Zipcode",StringType,false),
                            StructField("Battalion",StringType,false),
                            StructField("StationArea",StringType,false),
                            StructField("Box",StringType,false),
                            StructField("OriginalPriority",StringType,false),
                            StructField("Priority",StringType,false),
                            StructField("FinalPriority",StringType,false),
                            StructField("ALSUnit",BooleanType,false),
                            StructField("CallTypeGroup",StringType,false),
                            StructField("NumAlarms",IntegerType,false),
                            StructField("UnitType",StringType,false),
                            StructField("UnitSequenceInCallDispatch",IntegerType,false),
                            StructField("FirePreventionDistrict",StringType,false),
                            StructField("SupervisorDistrict",StringType,false),
                            StructField("Neighborhood",StringType,false),
                            StructField("Location",StringType,false),
                            StructField("RowID",StringType,false),
                            StructField("Delay",FloatType,false)
                            ))
    //creating dataframe from defined schema & loaded datafile

    val df = spark.read.format("csv").option("header","true").schema(my_schema).load(data_file)

    //display the dataframe
    //df.show(20,truncate=true)


    // *********************** COMMON DATAFRAME OPERATIONS ****************************
    
    // 1. Projections & Filters
    // In Spark, projections are done with the select() method, 
    // while filters can be expressed using the filter() or where() method.
    
    // fetching all rows where the "Calltype is a Medical Incident"

    val df_medical_incident = df.select ("IncidentNumber","AvailableDtTm","CallType")
                                .where (col("CallType") =!= "Medical Incident")
                                .show (5)

    // 2. What if we want to know how many distinct CallTypes were recorded as the causes
    //    of the fire calls?

    val number_of_fire_calls = df.select (countDistinct("CallType").alias("Distinct_calls")).show()

    val alternative = df.select("CallType")
                        .where(col("CallType").isNotNull)
                        .agg(countDistinct("CallType") as ("Distinct_calls"))
                        .show()

    // 3. list the distinct call types
    
    val distinct_call_types = df.select("CallType")
                                .where(col("CallType").isNotNull)
                                .distinct()
                                .show()

    // 4. Renaming columns using "withColumnRenamed"
    // renaming "Delay" column to "ResponseDelayedInMins"
    
    val renamed_col = df.withColumnRenamed("Delay","ResponseDelayedInMins")
                        .select("ResponseDelayedInMins")
                        .where(col("ResponseDelayedInMins") > 5)
                        .show()



    // 5. Changing the datatype of columns
    // using "to_date" to change the datatype of a StringType column to DateType
    // using "to_timestamp" to change the datatype of a StringType column to DateType
    // drop the original column after converting the datatype
    
    val changed_datatype_cols = df
                                  .withColumn("IncidentDate", to_date(col("CallDate"), "MM/dd/yyyy"))
                                  .drop("CallDate")
                                  .withColumn("OnWatchDate", to_date(col("WatchDate"), "MM/dd/yyyy"))
                                  .drop("WatchDate")
                                  .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTM"), "MM/dd/yyyy hh:mm                                   :ss a"))
                                  .drop("AvailableDtTM")

    // select the converted columns
     changed_datatype_cols.select("IncidentDate","OnWatchDate","AvailableDtTS").show(5)


    // 6. Aggregations
    // Most common type of fire call
    
    val common_fire_calls = df.select("CallType")
                              .where(col("CallType").isNotNull)
                              .groupBy(col("CallType"))
                              .agg(count("*").alias("Total_Count"))
                              .orderBy(desc("Total_Count"))
                              .show()

    // What ZipCodes accounted for most calls?

    val zip_codes_calls = df.select("Zipcode")
                            .where(col("Zipcode").isNotNull)
                            .groupBy(col("Zipcode"))
                            .agg(count("*").alias("Total_Calls"))
                            .orderBy(desc("Total_Calls"))
                            .show()

    // 7. Statistical Methods
    // Compute the sum of alarms in the dataset
    // Compute the average response time
    // Minimum & maximum response time

    val stats_df = df.agg(sum("NumAlarms").alias("Number_of_alarms"),avg("Delay").alias("Average_response_time"),
                      max("Delay").alias("max_response_time"),min("Delay").alias("min_response_time"))

    stats_df.show()

    // 8. DataFrameWriter
    // saving the dataframe as table or parquet file format
    
   val stats_df_results = stats_df.write.mode("overwrite").format("parquet").save("../../../results")
    //stop the spark session
    spark.stop()
  }
}
