// import packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable

// creating a singleton scala object containing the main method
object transform_data{
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

  // Perform UPDATE transformation
  // all of the loans assigned to addr_state = 'OR'
  // should have been assigned to addr_state = 'WA'

  val delta_table_transformation = DeltaTable.forPath(spark,delta_lake_path)

  //delta_table_transformation.update(col("addr_state") === "OR", Map("addr_state" -> lit("WA")))

  // Delta operations are actions that modify the table itself.
  // After the update, the same deltaTable object still points to the table
  // therefore, in order to display the changes made, we need to load the delta lake table into a dataframe again

  //val updated_table = spark.read.format("delta").load(delta_lake_path)
  //updated_table.show(10)
  //updated_table.printSchema()

  // let's display the history of delta table too
  delta_table_transformation.history().show(false)

  //also note this returns a dataframe of all transformations 
  
  
  // PERFORM DELETE transformation

   val delete_from_table = DeltaTable.forPath(spark,delta_lake_path)

   delete_from_table.delete("funded_amnt >= paid_amnt")

   // shortcut way to display the results
   // convert the table to dataframe and use .show() 

   import spark.implicits._
   delete_from_table.toDF.show()



  //stop the spark session
  spark.stop()
  }
}


