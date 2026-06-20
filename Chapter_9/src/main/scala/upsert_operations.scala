// import packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable

// creating a singleton scala object containing the main method
object upsert_data{
  def main(args:Array[String]) {

    // creating a spark session
    val spark = SparkSession.builder().appName("transform_data")
                    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .getOrCreate()

  // defining delta lake path 
  val delta_lake_path = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_9/delta_lake/"
  // load the source data file into the delta lake first
  // since i cleared all versions of delta lake as the data was heavily duplicated by multiple writes

  spark.read.format("parquet").load("data/loan-risks.snappy.parquet").write.format("delta").save(delta_lake_path)

  // load the data & display
  val data = spark.read.format("delta").load(delta_lake_path)
  data.show(10)
  data.printSchema()
  println(s"No. of rows: ${data.count()}")
  println(s"No. of columns: ${(data.columns.length)}")
  // creating another sample dataframe to perform merge

  import spark.implicits._
  val sample_df = Seq(( 12, 23000, 12000, "WA", "true"),
                      (1, 10000, 8000, "OR", "true"),   // existing loan -> update
                      (3, 20000, 15000, "TA", "true"),  // existing loan -> update
                      (4, 25000, 0, "PB", "false"))       // new loan -> insert
                      .toDF("loan_id", "funded_amnt", "paid_amnt","addr_state","closed")

  sample_df.show()
  sample_df.printSchema()

  // PERFORM UPSERT OPERATION USING MERGE()
  // this operation is between a delta lake table & a dataframe

  val deltatable = DeltaTable.forPath(spark,delta_lake_path)

  deltatable.alias("dt")
            .merge(sample_df.alias("s_df"), "dt.loan_id = s_df.loan_id")
            .whenMatched.updateAll()
            .whenNotMatched.insertAll()
            .execute()

  // verify the row count after upsert
  // using shortcut way
  deltatable.toDF.show()
  deltatable.toDF.printSchema()
  deltatable.toDF.count()



  //stop the spark session
  spark.stop()
  }
}


