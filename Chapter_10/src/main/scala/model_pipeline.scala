// import packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.Pipeline

// creating a singleton scala object containing the main method

object ml_pipeline{
  def main(args:Array[String]) {

    // instantiating a spark session
    val spark = SparkSession.builder().appName("model_training").getOrCreate()

    // training data ingestion 
    val train_df_path = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_10/data/train"
    val traindf = spark.read.format("parquet").load(train_df_path)

    // verify schema & data shape
    traindf.printSchema()
    println(s"No of rows of training data: ${traindf.count()} with no. of columns: ${traindf.columns.length}")

    // *********STEP 1: INSTANTIATING A TRANSFORMER => VECTORASSEMBLER ***************

    // IT IS A TRANFORMER WHICH TAKES ALL THE INPUT FEATURES AS PARAMETERS AND FEEDS INTO A SINGLE VECTOR
    // CALLED "features" WHICH IS APPENDED INTO THIS NEW DATAFRAME
    // For this example, just providing a single feature as Inputcolumn
    
    val vecAssembler = new VectorAssembler().setInputCols(Array("bedrooms")).setOutputCol("features")


    // ***************** STEP 2: INSTANTIATING AN ESTIMATOR TO BUILD MODELS *************************

    // Linear Regression is part of spark.ml.regression.LinearRegression module
    // instantiating an object of Linear Regression class with setter methods

    val lr = new LinearRegression().setFeaturesCol("features").setLabelCol("price")

    // NOTE: the "setFeaturesCol" & "setLabelCol" are setter methods pre-defined in SparkML
    // they simply means “use this column as input features”, “use this column as target label”
    // here lr is just configured object yet, learning didn't take place

    // **** STEP 3: CREATING A PIPELINE TO TRAIN THE ESTIMATOR ON TRAIN DATA & PASS TEST DATA TO PERFORM PREDICTIONS *****
   
    // In PIPELINE API , you simply specify the stages you want your data to
    // pass through, in order, and Spark takes care of the processing. It provides
    // the user with better code reusability and organization.
    

    val pipeline = new Pipeline().setStages(Array(vecAssembler, lr))

    // let's pass the training dataframe to pipeline object
    // this will result in a fitted pipeline, which is a transformer

    val pipeline_model = pipeline.fit(traindf)


    // defining the path of test dataset
    val test_df_path ="/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_10/data/test/"

    // load the test data into a dataframe
    val testdf = spark.read.format("parquet").load(test_df_path)

    // verify the shape of dataframe
    println(s"No. of rows in test dataset: ${testdf.count()} & No. of columns: ${testdf.columns.length}")

    // let's pass this test dataframe to our pipeline model to perform predictions on it

    val predictions = pipeline_model.transform(testdf)
    predictions.select("bedrooms","features","price","prediction").show()


    // stop the spark session
    spark.stop()
  }
}
