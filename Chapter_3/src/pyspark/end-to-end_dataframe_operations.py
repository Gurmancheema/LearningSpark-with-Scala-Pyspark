#import libraries

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# creating an entry point of the application

if __name__ == "__main__":

    #check the correct files submitted to spark
    if len(sys.argv)!=2:
        print("Please submit the correct files")
        sys.exit(1)

    # load the datafile
    data_file = sys.argv[1]

    # instantiate the sparksession

    spark = SparkSession.builder.appName("dataframe_exercise").getOrCreate()

    # load the file into a dataframe
    df = spark.read.parquet(data_file)

    #Note: i haven't passed any schema neither asked spark to infer it 
    # as paruqet files have their schema stored as a metadata & spark picks it up itself

    # display the dataframe & schema

    # df.show()
    df.printSchema()

    # ************************END TO END DATAFRAME OPERATIONS ******************************
  
    # 1. What were all the different type of calls in 2018?

    types_of_calls = (df.select(col("CallType").alias("Distinct Call Types"))
                        .where(year(col("CallDate")) == 2018)
                        .distinct()
                        .show() )


    # 2 . What month within the year 2018 saw the highest number of fire calls?

    month_with_highest_calls = (df.where(
                                (year(col("CallDate")) == 2018) &
                                (col("CallType") == "Structure Fire")
                                )
                                 .groupBy(month(col("CallDate")).alias("Month_with_highest_calls"))
                                 .agg(count("*").alias("Number_of_fire_calls"))
                                 .orderBy(desc("Number_of_fire_calls"))
                                 .limit (1)
                                 .show() )

    # 3 . Which neighbourhood in SanFrancisco generated the most fire calls in 2018? 

    SF_neighborhood_with_most_fire_calls =  (df.where((year(col("CallDate")) == 2018) &
                                                     (col("CallType") == "Structure Fire") &
                                                     (lower(col("City")) == "san francisco")
                                                     )
                                              .groupBy(col("Neighborhood").alias("Neighborhood_most_calls"))
                                              .agg(count("*").alias("Number_of_fire_calls"))
                                              .orderBy(desc("Neighborhood_most_calls"))
                                              .limit(1)
                                              .show() )

    # 4. Which neighbourhood had the worst response time to fire calls in 2018?

    neighborhood_with_worst_response_time = (df.where((year(col("CallDate")) == 2018) &
                                                    (col("CallType") == "Structure Fire")
                                                    )
                                              .groupBy(col("Neighborhood").alias("Neighborhood_with_worst_response_time"))
                                              .agg(max(col("Delay")).alias("Worst_response_time(secs)"))
                                              .orderBy(desc("Worst_response_time(secs)"))
                                              .limit(1)
                                              .show()
                                              )
    # 5. Which week in the year 2018 had most fire calls??

    week_with_most_fire_calls = (df.where((year(col("CallDate")) == 2018) &
                                         (col("CallType") == "Structure Fire")
                                         )
                                    .groupBy(weekofyear(col("CallDate")).alias("Week_with_most_fire_calls"))
                                    .agg(count("*").alias("Number_of_fire_calls"))
                                    .orderBy(desc("Number_of_fire_calls"))
                                    .limit(1)
                                    .show()
                                    )
    #stop the spark session
    spark.stop()

