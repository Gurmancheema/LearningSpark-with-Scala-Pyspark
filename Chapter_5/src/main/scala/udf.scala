// import packages

import org.apache.spark.sql.SparkSession

// defining a singleton scala object containing the main method

object udf{
  def main(args:Array[String]){

    // creating a spark session

    val spark = SparkSession.builder.appName("udf_creation").getOrCreate()

    // defining a UDF

    val cube = (a : Long) => { a * a * a }

    // register UDF

    spark.udf.register("cubed",cube)

    // creating a temporary view with random data
    //
    // creating a dataframe using sequence

    val data =  Seq((1,"john","smith"),(2,"tell","tale"))

    val df = spark.createDataFrame(data).toDF("id","first_name","last_name")

    df.show()

    spark.range(1,9).createOrReplaceTempView("udf_test")

    spark.sql("""SELECT id, cubed(id) AS id_cubed FROM udf_test""").show()









    // stop spark session
    spark.stop()
  }
}

