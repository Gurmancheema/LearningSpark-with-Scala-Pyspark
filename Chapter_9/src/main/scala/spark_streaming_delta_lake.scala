// import packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._

// creating a singleton scala object containing the main method
object spark_streaming_delta_lake{
  def main(args:Array[String]) {
    
    // creating a spark session & configuring it for delta lake functionality

    val spark = SparkSession.builder().appName("streaming_to_delta_lake")
                .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate()

    // reading from data stream source

    val newdatastream = spark.readStream.format("socket")
                                        .option("host","localhost")
                                        .option("port",9999)
                                        .load()

    // specifying write location
    val save_stream_to_delta_lake = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_9/delta_lake_streaming"
    // writing to delta lake

    val writing_to_delta_lake = newdatastream.writeStream.format("delta")
                                     .option("checkpointLocation","/tmp/cp_delta_lake")
                                     .trigger(Trigger.ProcessingTime("10 seconds"))
                                     .start(save_stream_to_delta_lake)

    // await query termination before closing spark session
    writing_to_delta_lake.awaitTermination()

    // stop the spark session
    spark.stop()
  }
}
