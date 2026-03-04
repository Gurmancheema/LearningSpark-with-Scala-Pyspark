# import packages

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# creating an entry point of the application
if __name__ == "__main__":

    # defining a UDF
    def cube (a):
        return a*a*a
    
    # creating an instance of sparksession
    spark = SparkSession.builder.appName("udf_examples").getOrCreate()

    # register the UDF
    spark.udf.register("cube_it",cube)

    # defining a sample dataframe
    data_values = [[1,"john","smith"], [2,"adam","apple"], [3,"george","washington"]]
    
    df = spark.createDataFrame(data_values, ("id","first_name","last_name"))

    # creating a view from it
    df.createOrReplaceTempView("view_for_udf")

    # performing sparksql query including a UDF operation
    spark.sql("""SELECT id, cube_it(id) AS cubed_id FROM view_for_udf""").show()

    #stop the spark session
    spark.stop()

