// import packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// creating a singleton scala object containing the main method
object spark_delta_lake{
  def main (args:Array[String]) {

    // creating a spark session
    // since i need to connect with delta lake packages, so i will configure the spark session accordingly

    val spark = SparkSession.builder().appName("spark_delta_lake")
                        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") 
                        //Extend Spark SQL with Delta-specific commands.
                        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                  //When interacting with tables, use Delta's catalog implementation instead of Spark's default one.
                        .getOrCreate()

    // creating a delta lake path

    val delta_lake = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_9/delta_lake"

    // defining source path
    val data_source = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_9/data"

    // now simply reading from the data source and further writing it as delta lake
    spark.read.format("parquet").load(data_source).write.format("delta").save(delta_lake)

    // create a view on the delta lake table created
    spark.read.format("delta").load(delta_lake).createOrReplaceTempView("delta_loans_table")

    // now simply query the table using SQL API

    spark.sql(""" SELECT * FROM delta_loans_table LIMIT 10 """).show()

    // stop the spark session
    spark.stop()
  }
}
