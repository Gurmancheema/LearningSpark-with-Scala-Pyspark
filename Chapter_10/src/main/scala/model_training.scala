// import packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression

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

    // *********STEP 1: PASS THE READY TRAINING DATA TO A TRANSFORMER => VECTORASSEMBLER ***************

    // IT IS A TRANFORMER WHICH TAKES ALL THE INPUT FEATURES AS PARAMETERS AND FEEDS INTO A SINGLE VECTOR
    // CALLED "features" WHICH IS APPENDED INTO THIS NEW DATAFRAME
    
    val vecAssembler = new VectorAssembler().setInputCols(Array("bedrooms")).setOutputCol("features")
    
    // after instantiating VectorAssembler object with setter methods defining the input and output columns
    // now we pass the training dataframe to this transformer

    val vecTraindf = vecAssembler.transform(traindf)

    // to verfiy the new transformed dataframe,let's print it
    vecTraindf.select("bedrooms","features","price").show()


    // ***************** STEP 2: USING ESTIMATORS TO BUILD MODELS *************************

    // To keep things simple in first learning iteration , i am using univariate linear regression estimator
    // post learning this concept, i will use all the features in seperate scala script

    // Estimators learn parameters from your data, have an estimator_name.fit() method, and are eagerly
    // evaluated (i.e., kick off Spark jobs), whereas transformers are lazily evaluated.
    
    // Linear Regression is part of spark.ml.regression.LinearRegression module
    
    // instantiating an object of Linear Regression class with setter methods

    val lr = new LinearRegression().setFeaturesCol("features").setLabelCol("price")

    // NOTE: the "setFeaturesCol" & "setLabelCol" are setter methods pre-defined in SparkML
    // they simply means “use this column as input features”, “use this column as target label”
    // here lr is just configured object yet, learning didn't take place

    val lr_model = lr.fit(vecTraindf)

    // only now learning took place, coeffcients are calculated , model (Transformer) is created
    // Let’s inspect the parameters it learned:

    val m = lr_model.coefficients(0)
    val b = lr_model.intercept

    println(s"The formula for the linear regression line is price = ${m}*bedrooms + ${b}")

    // stop the spark session
    spark.stop()
  }
}
