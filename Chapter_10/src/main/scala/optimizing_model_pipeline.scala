//import packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}

// creating a singleton scala object containing the main method
object optimized_random_forest{
  def main(args:Array[String]){

    //instantiating a spark session

    val spark = SparkSession.builder().appName("random_forest").getOrCreate()

    // training data source path
    val traindf_path = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_10/data/train/"

    // load into dataframe
    val traindf = spark.read.format("parquet").load(traindf_path)

    // verify the loaded dataframe
    println(s"No. of rows:${traindf.count()} & No. of columns: ${traindf.columns.length}")

    // Filter out the categorical cols
    val categorical_cols = traindf.dtypes.filter(_._2 == "StringType").map(_._1)

    // Filter out the numerical cols
    val numerical_cols = traindf.dtypes.filter(_._2 == "DoubleType").filter(_._1!= "price").map(_._1)

    // defining the output cols from the StringIndexer estimator to be defined next
    val indexed_cols = categorical_cols.map(_+"INDEX")

    // Instantiating the StringIndexer estimator
    val string_indexer = new StringIndexer().setInputCols(categorical_cols).setOutputCols(indexed_cols).setHandleInvalid("skip")

    // Instantiating the Vector Assembler transformer
    val assembler_inputs = indexed_cols ++ numerical_cols

    val assembler = new VectorAssembler().setInputCols(assembler_inputs).setOutputCol("features")

    // Instantiating the Random Forest regressor estimator
    val rf = new RandomForestRegressor().setLabelCol("price").setMaxBins(40).setSeed(42)

    // Instantiating the Pipeline API and pass the required stages
    val pipeline = new Pipeline().setStages(Array(string_indexer, assembler, rf))

    // Instantiating the Regressor Evaluator, using RMSE score for evaluation
    val regressor_eval = new RegressionEvaluator().setLabelCol("price").setPredictionCol("prediction").setMetricName("rmse")

    // Instantiating the ParamGridBuild class & specifying the hyperparameters to check for
    val params = new ParamGridBuilder().addGrid(rf.maxDepth, Array(2,4,6)).addGrid(rf.numTrees, Array(10,100)).build()

    // Instantiating the K-FOLD CROSS VALIDATOR which accepts the "estimator","evaluator" & "estimatorParamMaps"
    // as arguments

    val k_fold_cv = new CrossValidator().setEstimator(pipeline)
                                        .setEvaluator(regressor_eval)
                                        .setEstimatorParamMaps(params)
                                        .setNumFolds(3)
                                        .setSeed(42)

    // triggering spark job now to train the model
    // keep track of time taken for training the model
    val start = System.nanoTime()
    val cv_model = k_fold_cv.fit(traindf)

    println(f"Initial Training time: ${(System.nanoTime() - start)/1e9}%.2f seconds")

    // **************** OPTIMIZING USING PARALLELISM ******************

    val new_start = System.nanoTime()
    val paralled_cv_model = k_fold_cv.setParallelism(4).fit(traindf)

    println(f"Training time with Parallelism: ${(System.nanoTime() - new_start)/1e9}%.2f seconds")

    // *************** OPTIMIZING BY FITTING THE CROSS VALIDATOR INTO THE PIPELINE **************

    // defining the new cross validator with different parameters
    // passing the random forest regressor as "estimator"
    
    val new_k_fold_cv = new CrossValidator().setEstimator(rf)
                                            .setEvaluator(regressor_eval)
                                            .setEstimatorParamMaps(params)
                                            .setNumFolds(3)
                                            .setSeed(42)

    // now passing this Cross validator as a stage in Pipeline API
    // then fit the model using pipeline
    // again record the training time for comparison

    val new_pipeline = new Pipeline().setStages(Array(string_indexer, assembler, new_k_fold_cv))

    val pipeline_train_time_start = System.nanoTime()
    val new_pipeline_model = new_pipeline.fit(traindf)

    println(f"Training time by optimized pipeline API: ${(System.nanoTime() - pipeline_train_time_start)/1e9}%.2f seconds")

    // stop the spark session
    spark.stop()
  }
}
