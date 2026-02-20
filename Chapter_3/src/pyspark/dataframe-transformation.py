#import packages

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# this transformation spark job's main objective is to load, transform & store the datafile
# in a parquet format, as the CSV file is large in size & utilizes more spark's resources
# each time spark job is run to load, read & perform operations.
# The parquet file stored will be used in further operations in next pyspark scripts.

# creating an entry point of the application
if __name__ == "__main__":
    if len(sys.argv) !=2:
        print("Please pass the pyspark & datafile")
        sys.exit(1)

    #load the path of datafile
    data_file = sys.argv[1]

    #instantiating spark session
    spark = SparkSession.builder.appName("dataframe-transformation").getOrCreate()

    # load the file into a dataframe & letting spark infer the schema
    df = spark.read.format("csv").option("header","true").option("inferschema","true").load(data_file)

    #display dataframe & schema

    df.show(10)
    df.printSchema()

    # since some columns are inferred as StringType by spark
    # changing the datatypes to correct ones

    new_df = df.withColumn("CallDate", to_date(col("CallDate"), "MM/dd/yyyy")) \
                .withColumn("WatchDate", to_date(col("WatchDate"), "MM/dd/yyyy")) \
                .withColumn("AvailableDtTm", to_timestamp("AvailableDtTm","MM/dd/yyyy hh:mm:ss a"))

    #print new dataframe schema

    new_df.printSchema()

    #saving this new transformed dataframe as parquet file to use further

    new_df.write.mode("overwrite").parquet("../../results/pyspark-cleaned-sf-fire-calls")


    #stop the spark session

    spark.stop()

