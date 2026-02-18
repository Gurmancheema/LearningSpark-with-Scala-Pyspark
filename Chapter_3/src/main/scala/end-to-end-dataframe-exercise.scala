// import packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//defining a singleton scala object containing the main method

object dataframe_exercise{
  def main(args: Array[String]){

    //check the data file passed
    if (args.length < 1){
      println("Please pass the correct datafile")
      sys.exit(1)
    }
    
    val data_file = args(0)

    // instantiating the spark object
    
    val spark = SparkSession.builder.appName("dataframe-exercise").getOrCreate()

    // load the parquet datafile into a dataframe
    
    val df = spark.read.parquet(data_file)
    // get schema of datafile

    df.printSchema


    // ************************END TO END DATAFRAME OPERATIONS ******************************
  
    // 1. What were all the different type of calls in 2018?
    //
      val diff_calls =   df.select("CallType")
                            .where(year(col("OnCallDate")) === 2018)
                            .distinct()
                            .show()
    //stop the spark sesion

    // 2 . What month within the year 2018 saw the highest number of fire calls?

      val max_calls_month = df.where(year(col("OnCallDate")) === 2018 && col("CallType") === "Structure Fire")
                              .groupBy(month(col("OnCallDate")).alias("month"))
                              .agg(count("*").alias("total_fire_calls"))
                              .orderBy(desc("total_fire_calls"))
                              .limit (1)
                              .show()

    // 3 . Which neighbourhood in SanFrancisco generated the most fire calls in 2018?

      val max_calls_neighbourhood = df.select("Neighborhood")
                                      .where(
                                        lower(col("City")) === "san francisco" &&
                                        year(col("OnCallDate")) === 2018 &&
                                      col("CallType") === "Structure Fire"
                                    )
                                      .groupBy(col("Neighborhood"))
                                      .agg(count("*").alias("Total_fire_calls"))
                                      .orderBy(desc("Total_fire_calls"))
                                      .limit (1)
                                      .show()

    // 4. Which neighbourhood had the worst response time to fire calls in 2018?

      val neighborhood_most_delay = df.where(
                                        col("CallType") === "Structure Fire" &&
                                        year(col("OnCallDate")) === 2018
                                      )
                                      .groupBy(col("Neighborhood"))
                                      .agg(max(col("Delay")).alias("worst_response_time(secs)"))
                                      .orderBy(desc("worst_response_time(secs)"))
                                      .limit(1)
                                      .show()

    // 5. Which week in the year 2018 had most fire calls??
    
    val week_with_most_fire_calls =   df.where(col("CallType") === "Structure Fire" &&
                                              year(col("OnWatchDate")) === 2018
                                              )
                                        .groupBy(weekofyear(col("OnWatchDate")).alias("Week"))
                                        .agg(count("*").alias("Total_fire_calls"))
                                        .orderBy(desc("Total_fire_calls"))
                                        .limit (1)
                                        .show()

  // stop the spark session
    spark.stop()
  }
}
