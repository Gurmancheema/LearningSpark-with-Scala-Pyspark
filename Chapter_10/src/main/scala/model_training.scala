// import packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.VectorAssembler

// creating a singleton scala object containing the main method

object ml_flow{
  def main(args:Array[String]) {

    // instantiating a spark session
    val spark = SparkSession.builder().appName("model_training").getOrCreate()

    // training data ingestion 
    val train_df_path = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_10/data/train"
    val traindf = spark.read.format("parquet").load(train_df_path)

    // verify schema & data shape
    traindf.printSchema()
    println(s"No of rows of training data: ${traindf.count()} with no. of columns: ${traindf.columns.length}")

    // STEP 1: PASS THE READY TRAINING DATA TO A TRANSFORMER => VECTORASSEMBLER
    // IT IS A TRANFORMER WHICH TAKES ALL THE INPUT FEATURES AS PARAMETERS AND FEEDS INTO A SINGLE VECTOR
    // CALLED "features" WHICH IS APPENDED INTO THIS NEW DATAFRAME
    
    val vecAssembler = new VectorAssembler().setInputCols(Array("bedrooms")).setOutputCol("features")
    
    // after instantiating VectorAssembler object with setter methods defining the input and output columns
    // now we pass the training dataframe to this transformer

    val vecTraindf = vecAssembler.transform(traindf)

    // to verfiy the new transformed dataframe,let's print it
    vecTraindf.select("bedrooms","features","price").show()

    // stop the spark session
    spark.stop()
  }
}
