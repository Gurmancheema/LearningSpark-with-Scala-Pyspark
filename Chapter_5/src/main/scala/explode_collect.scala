// import packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

// defining a singleton scala object containing the main method

object explode_collect{
  def main(args:Array[String]){

    // instantiating a spark session

    val spark = SparkSession.builder.appName("explode_collect_function_prac").getOrCreate()

    // defining a schema
    val schema = StructType(Array( StructField("id", IntegerType, false),
                                    StructField("values",ArrayType(IntegerType),false)
                                    ))

    // creating a static data

    val data = Seq(Row(1,Seq(2,3,4)), Row(2,Seq(3,4,5)))

    // We currently have a local Scala collection (Seq[Row]).
    // Spark DataFrames are built on distributed data (RDD[Row]) + schema.
    // Since we defined the schema explicitly, we convert the local collection
    // into an RDD[Row] using parallelize before creating the DataFrame.'

    val data_rows = spark.sparkContext.parallelize(data)

    // passing the schema & RDD[Row] to create a dataframe

    val df = spark.createDataFrame(data_rows,schema)

    // display the schema & dataframe

    df.show()
    df.printSchema()

    // ******************* EXPLODE & COLLECT FUNCTIONS *********************************

    val exploded_df = df.select(col("id"),explode(col("values")).alias("exploded_values"))

    exploded_df.show()

    val collected_df = exploded_df.select(col("id"),(col("exploded_values") + 1).alias("added_values"))
                                  .groupBy(col("id"))
                                  .agg(collect_list("added_values").alias("collected_values"))

    collected_df.show()

    // *********************** PERFORMING SAME OPERATIONS USING UDF *****************************

    // creating a UDF to add 1 in the Array of values

    val add_one_udf = (a:Seq[Int]) => { a.map(_ +1) }

    // register the udf to spark

    spark.udf.register("addoneudf",add_one_udf)

    // using the same logic in sparksql
    // therefore registering the dataframe as temporary view first

    df.createOrReplaceTempView("udf_view")

    spark.sql("""SELECT id, addoneudf(values) AS new_values FROM udf_view""").show()

 // stop the spark session
    spark.stop()
  }
}
