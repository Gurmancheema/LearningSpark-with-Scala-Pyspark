# import packages

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# creating an entry point of the application

if __name__=="__main__":

    # instantiating a sparksession

    spark = SparkSession.builder.appName("explode_collect_example").getOrCreate()

    # defining a schema

    schema = StructType([StructField("student_id",IntegerType(),False),
                         StructField("marks_in_all_subjects",ArrayType(IntegerType(),False),False)]
                        )

    # creating a static data

    data = [ (1,[23,34,45,56]), (2,[45,32,21,34]) ]

    # forming a dataframe

    df = spark.createDataFrame(data,schema)

    # display dataframe & schema

    df.show()
    df.printSchema()

    # ************************ EXPLODE & COLLECT FUNCTIONS *********************************

    # 1. exploding values in the array

    exploded_df = df.select(col("student_id"), explode(col("marks_in_all_subjects")).alias("exploded_marks"))

    exploded_df.show()

    # 1.2 collecting the exploded array & performing mathematical operation on it

    collected_df = (exploded_df.select(col("student_id"), (col("exploded_marks") + 100).alias("add_marks")) 
                              .groupBy(col("student_id"))
                              .agg(collect_list(col("add_marks")).alias("new_marks")) 
                              .show()
                    )

    # ************************************* USER-DEFINED FUNCTION ***************************

    # 2. Acheiving the same with user-defined functions

    # creating a user-defined function

    def add_marks(a) :
        return [i + 100 for i in a]

    # register this function as UDF

    spark.udf.register("adding_marks", add_marks)

    # creating a temporary view of the dataframe

    df.createOrReplaceTempView("student_marks")

    # querying the temp view using UDF

    spark.sql(""" SELECT student_id, adding_marks(marks_in_all_subjects) FROM student_marks""").show()








    # stop the spark session
    spark.stop()


