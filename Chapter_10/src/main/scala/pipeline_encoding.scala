// import packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.{StringIndexer,OneHotEncoder,VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator

// creating a singleton scala object containing the main method
object pipeline_ml_flow{
  def main(args:Array[String]){

    // instantiating a spark session
    val spark = SparkSession.builder().appName("pipelineFlow").getOrCreate()

    // defining data source & ingesting data into a dataframe
    val data_source_path = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_10/data/train"

    val traindf = spark.read.format("parquet").load(data_source_path)
    traindf.printSchema()
    println(s"No. of rows of training data: ${traindf.count()} & No. of columns of training data: ${traindf.columns.length}")

    // From this training data, let's filter out the categorical columns having dtype as "StringType"
    // these categorical cols will be further fed to StringIndexer estimator

    val categorical_cols = traindf.dtypes.filter(_._2 =="StringType").map(_._1)
    println(s"Number of Categorical columns are: ${categorical_cols.length}")
    categorical_cols.foreach(println)

    // defining output cols of the StringIndexer estimator
    val indexer_output_cols = categorical_cols.map(_+"INDEX")

    
    // instantiating StringIndexer with the setter methods

    val string_indexer = new StringIndexer().setInputCols(categorical_cols).setOutputCols(indexer_output_cols).setHandleInvalid("skip")

    // The transformed output of StringIndexer will be the input for OneHotEncoder estimator
    // therefore, let's define the output cols of OHE
    val ohe_output_cols = categorical_cols.map(_+"OHE")

    // instantiating the OneHotEncoder with setter methods
    val ohe_model = new OneHotEncoder().setInputCols(indexer_output_cols).setOutputCols(ohe_output_cols)

    // Uptil now the categorical columns are transformed to numerical values,
    // before feeding them to a vector transformer like "Vector Assembler"
    // let's concatenate them into a single array of columns
    // first let's filter out the numerical cols

    val numerical_cols = traindf.dtypes.filter(_._2 == "DoubleType").filter(_._1!="price").map(_._1)
    println(s"Number of numerical cols filtered out are: ${numerical_cols.length}")
    numerical_cols.foreach(println)

    // concatenation
    val assembler_inputs = ohe_output_cols ++ numerical_cols

    // let's feed the inputs to the transformer to convert them into a vector
    // further the outputs of this vector will be used as input to the learning estimator
    // using VectorAssembler
    
    val vec_assembler = new VectorAssembler().setInputCols(assembler_inputs).setOutputCol("features")

    // lastly , before feeding everything to a pipeline, let's instantiate our main learning estimator
    // Using linear regression with it's own setter methods

    val lr_model = new LinearRegression().setFeaturesCol("features").setLabelCol("price")

    // Finally, let's instantiate the PipeLine API and feed all these objects as stages
    // to the machine learning flow we want to implement

    val ml_pipeline = new Pipeline().setStages(Array(string_indexer, ohe_model, vec_assembler, lr_model))

    // uptil now this "ml_pipline" hasn't learnt anything, i just defined the object & configured the 
    // stages to be followed during the learning process
    // now, let's pass the training data "traindf" to this pipeline model

    val pipeline = ml_pipeline.fit(traindf)

    // now the pipeline has learnt everything from the training data
    // let's pass the test data "testdf" to make predictions

    val test_data_source = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_10/data/test"
    val testdf = spark.read.format("parquet").load(test_data_source)

    val preddf = pipeline.transform(testdf)
    
    // print out the necessary columns to verify & compare predictions

    preddf.select("features","price","prediction").show()

    // ***************** MODEL EVALUTION ******************************
    // Since this is a regression task, let's use RMSE to evaulate the model's performance

    val regression_evaluator = new RegressionEvaluator().setPredictionCol("prediction").setLabelCol("price").setMetricName("rmse") 

    val rmse = regression_evaluator.evaluate(preddf)

    println(s"RMSE is : ${rmse}")

    // RMSE score alone is of no use unless we have another metric to compare with
    // therefore, let's build a  base line model and compute the average of "price" label from training data

    val label_avg = traindf.agg(mean(col("price")).alias("avg_price")).first().getDouble(0)
    println(s"Average of price from training data is : ${label_avg}")

    // this is the baseline model, simply an average of the label from training data
    // now let's compare it with predictions made on test data above

    // appending a new column in testdata labelled as "avg_predictions"
    // then pass the test data to the regressor evaluator to calculate baseline RMSE

    val new_test_df = testdf.withColumn("avg_prediction",lit(label_avg))
    new_test_df.select("price","avg_prediction").show()

    // now let's calculate the baseline RMSE
  
    val regression_evaluator_for_baseline_model = new RegressionEvaluator().setPredictionCol("avg_prediction").setLabelCol("price").setMetricName("rmse")

    val baseline_rmse = regression_evaluator_for_baseline_model.evaluate(new_test_df)

    println(s"Baseline_RMSE: ${baseline_rmse}")

    // *********** NOW COMPARE THE TWO RMSE *********************

    if ( rmse < baseline_rmse)
      println("Trained linear regression model learnt something new & performed well")
    else
      println(" Trained linear regression model didn't perform well")

  



     // stop the spark session
    spark.stop()
  }
}
