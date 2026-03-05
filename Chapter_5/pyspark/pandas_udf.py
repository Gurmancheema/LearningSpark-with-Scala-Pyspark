#import packages

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd

# creating a main entry point of the application
if __name__ == "__main__":

    # creating a spark session instance
    spark = SparkSession.builder.appName("pandas_udf_exercise").getOrCreate()

    # defining a normal function
    def cube_it(a:pd.Series) -> pd.Series:
        return a*a*a

    # wrapping the normal function into a pandas UDF
    cubed_pandas_udf = pandas_udf(cube_it, returnType = IntegerType())

    # creating a pandas series to parse through the defined udf

    x = pd.Series([1,2,3])


    # create a spark dataframe

    df = spark.range(1,9)
    
    # registering the UDF first before using spark sql

    spark.udf.register("spark_reg_udf",cubed_pandas_udf)

    # creating a temp view from dataframe

    df.createOrReplaceTempView("new_udf_view")

    df.select("id",cubed_pandas_udf(col("id"))).show()

    spark.sql(""" SELECT id, spark_reg_udf(id) FROM new_udf_view""").show()
