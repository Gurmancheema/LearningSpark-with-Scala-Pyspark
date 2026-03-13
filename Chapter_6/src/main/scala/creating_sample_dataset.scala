// import packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Random._

// defining a case class outside the main function
// for smooth encoders built by spark

case class dataset_schema (user_id: Int,  user_name: String, usage: Int)

// defining a singleton scala object containing the main method
//
object sample_dataset{
  def main(args:Array[String]){

    // instantiating a sparksession

    val spark = SparkSession.builder.appName("working_on_datasets").getOrCreate()

    import spark.implicits._ 

    // provides implicit encoders required to convert Scala objects into Spark Dataset format

    // Create Random generator object with seed 42 to produce reproducible random values

    val random_generator = new scala.util.Random(42)

    // creating random data to populate dataset
    
    val data = for (i <- 0 to 1000)
               yield(dataset_schema( i, "user-" + random_generator.alphanumeric.take(5).mkString(""),
                        random_generator.nextInt(1000)))

    // creating a dataset out of it

    val ds =  spark.createDataset(data)

    // verify dataset schema
    ds.show()
    ds.printSchema()


    // **************************** HIGHER ORDER FUNCTIONS/OPERATIONS ON DATASET **************************
    // 1. return all users whose usage exceeds more than 900 minutes

    val more_usage_users = ds.filter(d => d.usage > 900).orderBy($"user_id")

    more_usage_users.show()

    // alternatively we can create & pass a function as an argument to the filter function

    val filter_function = (d: dataset_schema) => { d.usage > 900 }

    // passing this function to the filter function as argument

    val filtered_users = ds.filter(filter_function(_)).orderBy(desc("user_id"))

    filtered_users.show()

    //stop the spark session
    spark.stop()
  }
}




