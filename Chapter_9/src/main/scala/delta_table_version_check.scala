// import packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable

// creating a singleton scala object containing the main method
object fetch_version{
  def main(args:Array[String]) {

    // creating a spark session
    val spark = SparkSession.builder().appName("transform_data")
                    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .getOrCreate()

  // defining delta lake path 
  val delta_lake_path = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_9/delta_lake/"

  // load the data & display
  val data = spark.read.format("delta").load(delta_lake_path)
  data.show(10)
  data.printSchema()
  println(s"No. of rows: ${data.count()}")
  println(s"No. of columns: ${(data.columns.length)}")

  // check version history

  DeltaTable.forPath(spark, delta_lake_path)
            .history()
            .select("version",
                   "timestamp",
                   "operation",
                   "operationMetrics")
            .show(false)
  // found duplicate writes
  // therefore deleting the whole table for clean start again
  
  import org.apache.hadoop.fs._

  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  fs.delete(new Path(delta_lake_path), true)

  //stop the spark session
  spark.stop()
  }
}


