#importing packages
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

print("all imports are successful")

# creating the entry point of the application

if __name__ == "__main__":

    #check if the datafile is passed or not
    if len(sys.argv) != 2:
        print("Please pass the correct datafile",file = sys.stderr)
        sys.exit(1)

    # load the datafile path
    data_file = sys.argv[1]

    # instantiating spark object
    spark = SparkSession.builder.appName("dataframe_operation").getOrCreate()

    # since the data file to be worked upon is large
    # best practice is to define the schema rather have spark infer it
    # saving spark jobs & resources

    # defining schema

    my_schema = StructType([ StructField("CallNumber",IntegerType(),True),
                            StructField('UnitID', StringType(), True),
                            StructField('IncidentNumber', IntegerType(), True),
                            StructField('CallType', StringType(), True),
                            StructField('CallDate', StringType(), True),
                            StructField('WatchDate', StringType(), True),
                            StructField('CallFinalDisposition', StringType(), True),
                            StructField('AvailableDtTm', StringType(), True),
                            StructField('Address', StringType(), True),
                            StructField('City', StringType(), True),
                            StructField('Zipcode', IntegerType(), True),
                            StructField('Battalion', StringType(), True),
                            StructField('StationArea', StringType(), True),
                            StructField('Box', StringType(), True),
                            StructField('OriginalPriority', StringType(), True),
                            StructField('Priority', StringType(), True),
                            StructField('FinalPriority', IntegerType(), True),
                            StructField('ALSUnit', BooleanType(), True),
                            StructField('CallTypeGroup', StringType(), True),
                            StructField('NumAlarms', IntegerType(), True),
                            StructField('UnitType', StringType(), True),
                            StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                            StructField('FirePreventionDistrict', StringType(), True),
                            StructField('SupervisorDistrict', StringType(), True),
                            StructField('Neighborhood', StringType(), True),
                            StructField('Location', StringType(), True),
                            StructField('RowID', StringType(), True),
                            StructField('Delay', FloatType(), True)
                            ])
    
    # creating a dataframe from the defined schema & loaded datafile path
    # since we are loading the datafile from it's path, we can't use the following:
    
    # df = spark.createDataFrame(data_file,my_schema)

    # we need to read the file from the path of the datafile passed, therefore

    df = spark.read.format("csv").option("header","true").schema(my_schema).load(data_file)

    df.show(10)

    # ********************************* COMMON DATAFRAME OPERTAIONS *********************************

    # 1. Projections & Filters

    # using select() method for projections
    # using where() method to filter out on some conditions

    # fetching all rows where the "Calltype is NOT a Medical Incident"

    df.select("IncidentNumber","AvailableDtTM","CallType").where(col("CallType") != "Medical Incident").show(10)

    # 2. What if we want to know how many distinct CallTypes were recorded as the causes
    #    of the fire calls?

    number_of_distinct_call_types = (df.select("CallType")
                                     .where(col("CallType").isNotNull())
                                     .agg(countDistinct("CallType").alias("Number_of_distinct_calls"))
                                     .show()
                                     )

    # 3. list the distinct call types

    distinct_call_types = (df.select("CallType")
                            .where(col("CallType").isNotNull())
                            .groupBy(col("CallType"))
                            .agg(count("CallType").alias("Total_calls"))
                            .orderBy(desc("Total_calls"))
                            .show()
                           )

    # 4. Renaming columns using "withColumnRenamed"
    # renaming "Delay" column to "ResponseDelayedInMins"

    renamed_df = df.withColumnRenamed("Delay","ResponseDelayedInMins")

    (renamed_df.select("ResponseDelayedInMins")
                .where(col("ResponseDelayedInMins") > 5)
                .show(10)
     )

    # 5. Changing the datatype of columns

    # using "to_date" to change the datatype of a StringType column to DateType
    # using "to_timestamp" to change the datatype of a StringType column to DateType
    # drop the original column after converting the datatype

    changed_datatype_df = (df.withColumn("Incident_date", to_date(col("CallDate"), "MM/dd/yyyy"))
                            .drop("CallDate")
                            .withColumn("OnWatchDate", to_date(col("WatchDate"), "MM/dd/yyyy"))
                            .drop("WatchDate")
                            .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTM"), "MM/dd/yyyy hh:mm:ss a"                             )).drop("AvailableDtTM")
                            )
    changed_datatype_df.select("Incident_date","OnWatchDate","AvailableDtTS").show(10)

    # 6. Statistical methods
    # compute the sum of alarms
    # compute the average of response time/delay
    # compute the max & min response time/delay

    stats_df = (df.agg(sum("NumAlarms").alias("Total_alarms"),
                      avg("Delay").alias("Avg_response_time"),
                      max("Delay").alias("Max_response_time"),
                      min("Delay").alias("Min_response_time")
                      )
                )
    stats_df.show()
    # 7. Saving the results
    # can be saved as table or parquet format file

    stats_df_results = stats_df.write.mode("overwrite").format("parquet").save("../../results")
    #stop the spark session

    spark.stop()
