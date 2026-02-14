# importing packages
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# creating entry point of the application

if __name__ == "__main__": 
    #checkpoint for correct datafile
    if len(sys.argv) != 2:
        print("Please provide the pyspark & datafile correctly",file = sys.stderr)
        sys.exit(1)

    #load the file 
    file_name = sys.argv[1]

    #create a spark object
    spark = SparkSession.builder.appName("cols&expressions").getOrCreate()
    
    # since the file is being loaded from external database
    # it is a good habit to define the schema beforehand, instead of letting spark infer it

    #defining schema

    my_schema = StructType([StructField ("Id", IntegerType(), False),
                         StructField ("First", StringType(), False),
                         StructField ("Last", StringType(), False),
                         StructField ("Url", StringType(), False),
                         StructField ("Published", StringType(), False),
                         StructField ("Hits", IntegerType(), False),
                         StructField ("Campaigns", ArrayType(StringType(), True), False)]
                        )

    #load the file into a dataframe

    df = spark.read.format("json").option("header","true").schema(my_schema).load(file_name)

    #alternatively, this will also wor
    # df =spark.read.schema(my_schema).json(file_name)

    df.show()
    df.printSchema()

    #1. using col() to display the column 

    df.select(col("Id")).show()

    #2. getting the list of all the columns of dataframe
    print(df.columns)

    #3. using expr() to perform calculations on a column
    df.select(expr("Hits *2")).show()

    #4. using col() to perform calculations on a column
    df.select((col("Id") * 10).alias("Operated id's")).show()

    ##NOTE: keep in mind the qoutations while performing calculations using col() & expr()

    #5. creating a new column which will be appended in dataframe using withColum() expression

    df.withColumn("Big_hitters", col("hits") > 10000).show()

    #6. creating a new column by concatenating certain columns using concat() expression

    df.withColumn("Authorsid", concat( col("First"), col("Last"), col("Id")) ).show()

    #7. using the sort() expression to sort the dataframe with respect to any column 
    # by default sort() function is ascending, pass the "desc()" methods to sort the in descending order

    df.sort(col("Hits").desc()).show()

    #stop the spark session

    spark.stop()
