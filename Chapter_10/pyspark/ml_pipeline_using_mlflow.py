# import libraries & packages
from pyspark.sql import SparkSession
from pyspark.sql import functions.*
from pyspark.sql import types.*
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# defining the entry point of the application
if __name__ == "__main__":

    # creating a sparksession
    spark = SparkSession.builder().appName("mlflow_working").getOrCreate()

    # defining the train data source path
    train_data_path = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_10/data/train"

    # load data into a dataframe
    train_df = spark.read.format("parquet").load(train_data_path)

    # verify the data

