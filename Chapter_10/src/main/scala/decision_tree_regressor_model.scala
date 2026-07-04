// import packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer,VectorAssembler}
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator

// creating a singleton scala object containing the main method
object decision_tree_flow{
  def main(args:Array[String]) {

    // instantiating a spark session
    val spark = SparkSession.builder().appName("decision_tree").getOrCreate()

    // defining data sources
    val train_data_path = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_10/data/train"

    // creating a dataframe out of training data
    val traindf = spark.read.format("parquet").load(train_data_path)

    // verify created dataframe
    traindf.show()
    println(s"No of rows: ${traindf.count()} & No. of columns: ${traindf.columns.length}")

    // though i will be using Pipeline API to pass all the stages of transformations
    // still need to filter out the categorical cols for StringIndexer
    
    val categorical_cols = traindf.dtypes.filter(_._2 == "StringType").map(_._1)

    // verify the no. of categorical cols
    println(s"No. of categorical cols: ${categorical_cols.length}")
    categorical_cols.foreach(println)

    //let's define the name of outputcols from the StringIndexer's transformation
    val string_indexed_cols = categorical_cols.map(_+"INDEX")

    // instantiate the StringIndexer object

    val string_indexer = new StringIndexer().setInputCols(categorical_cols).setOutputCols(string_indexed_cols).setHandleInvalid("skip")

    // categorical columns are taken care of, let's filter out the numerical columns now

    val numerical_cols = traindf.dtypes.filter(_._2 == "DoubleType").filter(_._1!= "price").map(_._1)

    println(s"No. of numerical cols : ${numerical_cols.length}")
    numerical_cols.foreach(println)

    // instantiate the VectorAssembler object
    // this will take all the features of the dataframe as input
    // therefore, combine numerical & categorical cols

    val assembler_inputs = string_indexed_cols ++ numerical_cols

    val vector_assembler = new VectorAssembler().setInputCols(assembler_inputs).setOutputCol("features")

    // since i am using Decision Tree Regressor to train on this dataset
    // there is no need for OneHotEncoding as decision tree models are designed to handle 
    // categorical features very well

    // instantiate the decision tree regressor model
    val dt_model = new DecisionTreeRegressor().setLabelCol("price")

    // instantiate the pipeline api
    val pipeline_flow = new Pipeline().setStages(Array(string_indexer,vector_assembler,dt_model))

    // let's pass the training dataset to pipeline flow so that it learns the parameters from the model
    dt_model.setMaxBins(40)
    val pipeline_model = pipeline_flow.fit(traindf)  // this should throw an error initially; setMaxBins fixes the error

    // now the model has learnt the best parameters let's test it on test dataset
    val test_data_path = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_10/data/test"

    val testdf = spark.read.format("parquet").load(test_data_path)
    val preddf = pipeline_model.transform(testdf)

    // now the model has performed predictions on the test dataset
    // let's filter out the necessary columns to see the output

    preddf.select("features","price","prediction").show()

    // *********** MODEL EVALUATION ************
    
    // let's evaluate our model using RMSE score, since we do have baseline model score from
    // linear regression model

    val rmse_evaluator = new RegressionEvaluator().setPredictionCol("prediction").setLabelCol("price").setMetricName("rmse")

    val rmse = rmse_evaluator.evaluate(preddf)
    println(s"RMSE score: ${rmse}")

    // stop the spark session
    spark.stop()
  }
}
