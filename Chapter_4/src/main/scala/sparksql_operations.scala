//   import packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.fucntions._
import org.apache.spark.sql.types._

//defining a singleton scala object containing the main method

object sparksql_operationss {
  def main(args:Array[String]) {

    // check the files passed
    if (args.length < 1) {
      println("Please pass the correct files")
      sys.exit(1)
    }

    // loading the file path 
    val data_file = args(0)

    // instantiating a spark session
    val spark = Sparkk
