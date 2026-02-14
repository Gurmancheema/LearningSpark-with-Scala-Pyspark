// import packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// defining a scala singleton object which contains the main method

object cols_expressions{
  def main(args: Array[String]){
    
    //fetching the datafile
    
    if (args.length<1) {
      print("Incorrect data file passed")
      sys.exit(1)
    }

    //creating a spark object

    val spark =  SparkSession.builder.appName("cols_expression_example").getOrCreate()

    //loading the datafile

    val data = args(0)

    //loading the datafile into a dataframe

    val df = spark.read.format("json").option("header","true").option("inferschema","true").load(data)

    //trigger operation to display the dataframe

    df.show()
    
    //column expressions

    // 1.print all columns
    println("Array of columns\n")
    println(df.columns)

    // 2.access a particular column with "col" & it returns a Columntype
    println("Columntype is returned using col\n")
    println(df.col("Id"))

    // 3.Use an expression to compute a value
    println("\nUsing expr to compute a value\n")
    df.select(expr("Hits * 2")).show()

    // 4.Using "col" to comupte a value
    println("\nUsing col to compute a value\n")
    df.select(col("Hits") * 2).show()

    // 5.Use an expression to compute big hitters in the blog
    // this will append a new column "big_hitters" based on conditional expression

    df.withColumn("big_hitters",expr("Hits > 10000")).show()
    
    //6.Concatenate 3 columns into 1 column & print the dataframe
    
    df.withColumn("AuthorsId",concat( col("First"), col("Last"), col("Id") )).select(col("AuthorsId")).show()

    //7. the following commands show that col() is same as expr() as they output same result
    df.select(col("Hits")).show(2)
    df.select(expr("Hits")).show(2)
    //df.select($("Hits")).show(2)
    df.select("Hits").show(2)
    

    //8. Using the sort function

    df.sort(col("Id").desc).show()
    //df.sort($("Id").desc).show() 

    //NOTE: "$" is an implicit function & requires implicit package to work 
    //but works the same as "col()". Better is to use "col()" for more
    //readability & production grade code

    //stop the spark session
    spark.stop()
  }
}
