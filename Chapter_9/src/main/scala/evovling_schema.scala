// import packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// creating a singleton scala object containing the main method
object evolving_schema{
  def main(args:Array[String]) {

    // creating a spark session with delta lake configs
    
    val spark = SparkSession.builder().appName("evolved_schema")
              .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
              .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
              .getOrCreate()

    // reading the data from source
    val data = spark.read.format("parquet").load("data/loan-risks.snappy.parquet")

    // sink to console for verification
    data.show(5)
    data.printSchema()

    // adding a new column to this dataframe
    val updated_data = data.withColumn("closed", col("paid_amnt") >= col("funded_amnt"))

    // sink this new dataframe to console for verification
    updated_data.show(10)
    updated_data.printSchema()

    // now trying writing this updated dataframe to same delta lake location
    // i am expecting "schema mismatch error" here as new column has been added
    
    // delta lake file path
    val delta_lake_path = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_9/delta_lake"

    // updated_data.write.format("delta").mode("append").save(delta_lake_path)

    // schema mismatch was found, therefore it proves that delta lake unlike common data formats like
    // JSON, Parquet & ORC which store the datalayout of individual files and not of the entire table;
    // delta lake format records the schema as table-level metadata

    // therefore, in order to enforce the concept of "Evolving Schemas" in delta lake, we need to do:

    updated_data.write.format("delta").mode("append").option("mergeSchema","true").save(delta_lake_path)

    // print latest schema to console for verification
    val latest_read = spark.read.format("delta").load(delta_lake_path).printSchema()

    // stop the spark session
    spark.stop()
  }
}
