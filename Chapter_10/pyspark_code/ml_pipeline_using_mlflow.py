# import libraries & packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import time

# defining the entry point of the application
if __name__ == "__main__":

    # creating a sparksession
    spark = SparkSession.builder.appName("mlflow_working").getOrCreate()

    # defining the data sources path
    train_data_path = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_10/data/train"
    test_data_path = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_10/data/test"

    # load both data sources into dataframes
    traindf = spark.read.format("parquet").load(train_data_path)
    testdf = spark.read.format("parquet").load(test_data_path)

    # verify the dataframe loaded
    traindf.printSchema()
    print(f"No. of rows: {traindf.count()} & No. of cols: {len(traindf.columns)}")

    # filter out the categorical columns from the dataframe
    categorical_cols = []
    for x,y in traindf.dtypes:
        if y == "string":
            categorical_cols.append(x)

    print(categorical_cols)
    print(f"Total Categorical Columns: {len(categorical_cols)}")

    # filter out the numerical columns from the dataframe
    numerical_cols = [x for x, y in traindf.dtypes if y == "double" and x != "price"]
    print(numerical_cols)
    print(f"Total Numerical Columns : {len(numerical_cols)}")


    # instantiate the StringIndexer estimator
    indexed_cols = [x + "INDEX" for x in categorical_cols]
    string_indexer = StringIndexer(inputCols=categorical_cols,
                                   outputCols=indexed_cols,
                                   handleInvalid="skip")

    # instantiate the VectorAssembler transformer
    assembler_input_cols = indexed_cols + numerical_cols
    vector_assembler = VectorAssembler(inputCols = assembler_input_cols,
                                       outputCol = "features")

    # instantiate the RandomForestRegressor estimator
    random_forest = RandomForestRegressor(labelCol = "price", maxBins = 40, seed = 42)

    # instantiate the Pipeline API to set stages
    pipeline = Pipeline(stages = [string_indexer, vector_assembler, random_forest])

    # ******************** MLFLOW LOGGING BEGINS HERE **************************

    import mlflow
    import mlflow.spark
    import pandas as pd

    # start a run here
    mlflow.start_run()

    # 1. log_params : num_trees and max_depth
    mlflow.log_param("Num_trees:", random_forest.getNumTrees())
    mlflow.log_param("Max_depth:",random_forest.getMaxDepth())

    # 2. log model
    pipelineModel = pipeline.fit(traindf)
    mlflow.spark.log_model(pipelineModel,"model")

    # 3. log metrics : RMSE and R2
    preddf = pipelineModel.transform(testdf)

    regressor_evaluator = RegressionEvaluator(labelCol = "price", predictionCol = "prediction")

    rmse = regressor_evaluator.setMetricName("rmse").evaluate(preddf)
    r2 = regressor_evaluator.setMetricName("r2").evaluate(preddf)

    mlflow.log_metrics({"rmse": rmse, "r2": r2})

    # 4. log artifacts : feature importance scores
    rfmodel = pipelineModel.stages[-1]
    pandasdf = (pd.DataFrame(list(zip(vector_assembler.getInputCols(),rfmodel.featureImportances)),
                             columns=["feature", "importance"]).sort_values(by="importance", ascending=False))

    # First write to local filesystem, then tell MLflow where to find that file
    pandasdf.to_csv("feature-importance.csv", index=False)
    mlflow.log_artifact("feature-importance.csv")

    mlflow.end_run()

    # stop the spark session
    spark.stop()

