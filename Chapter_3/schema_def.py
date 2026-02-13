# import libraries
from pyspark.sql.types import *
from pyspark.sql import SparkSession

#creating entry point of the pyspark application

if __name__=="__main__":
    

    # creating an spark object

    spark = SparkSession.builder.appName("schema_definition").getOrCreate()

    # defining schema here
    schema = StructType([StructField("id",IntegerType(),False),
                        StructField("first_name",StringType(),False),
                        StructField("last_name",StringType(),False),
                        StructField("url",StringType(),False),
                        StructField("Published",StringType(),False),
                        StructField("hits",IntegerType(),False),
                        StructField("campiagns",ArrayType(StringType(),False),False)
                         ])

    # Create our static data
    data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter","LinkedIn"]],
            [2, "Brooke","Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter","LinkedIn"]],            [3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web","twitter", "FB", "LinkedIn"]],
            [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568,["twitter", "FB"]],
            [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web","twitter", "FB", "LinkedIn"]],
            [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568,["twitter", "LinkedIn"]]
]

    ## putting everything together in a dataframe

    df = spark.createDataFrame(data,schema)
    
    # print the dataframe
    df.show()

    #print the schema
    df.printSchema()

    #stop the spark session

    spark.stop()

