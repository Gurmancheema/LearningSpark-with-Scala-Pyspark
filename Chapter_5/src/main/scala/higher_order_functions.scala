//import packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

// defining a singleton scala object containing the main method

object higher_order_functions{
  def main (args:Array[String]){

    // creating a spark session
    
    val spark = SparkSession.builder.appName("high_order_functions").getOrCreate()

    // defining a schema

    val schema = StructType(Array(StructField("celsius",ArrayType(IntegerType), false)))

    // creating static data
    // we want to two rows containing array of temperatures

    val data = Seq(Row(Seq(34,45,23,43,56,21,4)),
                   Row(Seq(23,54,31,34,54,24,6))
                   )

    // creating RDD[Row] before creating a dataframe

    val data_rows = spark.sparkContext.parallelize(data)

    // creating a dataframe from RDD[Row] and defined schema

    val df = spark.createDataFrame(data_rows,schema)

    // display dataframe and schema
    
    df.show()
    df.printSchema()

    // ******************** PERFORMING HIGHER ORDER FUNCTIONS ON DATAFRAME *****************************
    
    // 1. transform () function
    // Calculate Fahrenheit from Celsius for an array of temperatures
    
    val farenheit_df = df.select(col("celsius"), transform(col("celsius"), t => ((t * 9) / 5) + 32).alias("Farenheit"))

    // display the dataframe

    farenheit_df.show()
    
    // 2. filter () fucntion
    // Filter temperatures > 38C for array of temperatures

    val filtered_temp = df.select(filter(col("celsius"), t => (t > 38)).alias("filtered_temperatures")).show()

    // 3. exists () function
    // Is there a temperature of 38C in the array of temperatures

    val exists_threshold = df.select(col("celsius"), exists(col("celsius"), t => (t === 38)).alias("threshold_temp")).show()

    // 4. reduce () function
    // Calculate average temperature and convert to F
   
    val avg_temp = df.select(col("celsius"), reduce(col("celsius"), lit(0.0), (acc,temp)=> (acc + temp),
                                                    acc => ((acc/size(col("celsius")) * 9/5) + 32))
                                                    .alias("avg_temp"))
                                                    .show()

    df.select("celsius").show(false)


    // stop the spark session
    spark.stop()
  }
}
