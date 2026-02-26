#import packages

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# creating the entry point of the application

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Please pass the script file & datafile",file = sys.stderr)
        sys.exit(1)

    # load the datafile's path
    data_file = sys.argv[1]

    # instantiating spark session

    spark =  SparkSession.builder.appName("sparksql_operations").getOrCreate()

    # defining the schema using DDL statement

    schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"

    # load the datafile into a dataframe with reference to defined schema

    df = spark.read.format("csv").option("header","true").schema(schema).load(data_file)

    # display the dataframe & schema
    df.show(10)
    df.printSchema()

    # creating a temp view to perform SparkSQL operations

    df.createOrReplaceTempView("us_flight_departure_delays")

    # query the temp view, NOTE=> querying a temp view using sparksql returns a dataframe

    new_df = spark.sql("""SELECT *
                            FROM us_flight_departure_delays""")
    new_df.show(10)
    # **************************SPARK SQL OPERATIONS ON TEMPORARY VIEW****************************  

    # 1.find all flights whose distance is greater than 1,000 miles 
    
    long_distance_flights = spark.sql("""SELECT *
                                         FROM us_flight_departure_delays
                                         WHERE distance > 1000
                                         ORDER BY distance DESC""").show(10)

    # 2. find all flights between San Francisco (SFO) and Chicago (ORD) with at least a two-hour delay

    flights_with_delays = spark.sql("""SELECT *
                                       FROM us_flight_departure_delays
                                       WHERE origin = 'SFO' AND
                                       destination = 'ORD' AND
                                       delay >= 120
                                       ORDER BY delay DESC""").show(20)

    # 3. find the months when the delays were most common

    # changing the datatype of "date" column from STRING to TIMESTAMP
    # since, the datatype of a temporary view cannot be changed as temp view is just a logical pointer
    # to a dataframe
    # therefore changing the datatype in dataframe & then forming a new temp view

    updated_df = df.withColumn("updated_date", to_timestamp(col("date"),"MMddHHmm"))

    updated_df.createOrReplaceTempView("us_flight_departure_delays")

    # print schema to verify addition of new column with changed datatype
    updated_df.printSchema()
    
    months_with_most_delays = spark.sql("""SELECT MONTH(updated_date) AS months_with_most_delays,
                                           COUNT ("*") AS total_months
                                           FROM us_flight_departure_delays
                                           WHERE origin = 'SFO' AND
                                           destination = 'ORD' AND
                                           delay >= 120
                                           GROUP BY MONTH(updated_date)
                                           ORDER BY total_months DESC""").show()

    # 4. Label all flights regardless of their origin or destination with an indication
    # of delays they experienced. For example:; Very Long delay > 6 hours, Long delay 2-6 hours 

    delayed_flight_label = spark.sql("""SELECT *,
                                        CASE
                                            WHEN delay > 360 THEN 'Very Long Delay'
                                            WHEN delay > 120 AND delay < 360 THEN 'Long Delay'
                                            WHEN delay > 60 AND delay < 120 THEN 'Delay'
                                            WHEN delay > 0 AND delay < 60 THEN 'Tolerable Delay'
                                            WHEN delay = 0 THEN 'No Delay'
                                        ELSE
                                            'Early'
                                        END AS Flight_delays
                                        FROM us_flight_departure_delays
                                        ORDER BY delay DESC""").show(15)










    
    #stop spark session
    spark.stop()

