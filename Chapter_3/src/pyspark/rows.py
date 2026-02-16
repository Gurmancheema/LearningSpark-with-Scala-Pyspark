#importing packages

from pyspark.sql import SparkSession
from pyspark.sql import Row

#creating entry point of the pyspark script

if __name__== "__main__":

    #instantiating spark session
    spark = SparkSession.builder.appName("rows_concept").getOrCreate()

    # creating row object

    blogs_row = Row(10,"Jeona","morhh","https://tinyurl.1","2/16/2025","45000",["twitter","linkedin","youtube"])

    # accessing the row item using the index
    # row index starts from 0

    print(blogs_row[4])

    # creating dataframe from row object
    df_rows = [Row("james","california","fiction"),Row("Clarke","washington","science")]

    df  = spark.createDataFrame(df_rows,["name","city","genre"])

    df.show()
    # stop the spark session

    spark.stop()
