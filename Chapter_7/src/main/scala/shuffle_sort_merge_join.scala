// import packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Random

// creating a singleton scala object containing the main method
object shuffle_sort{
  def main(args:Array[String]){

    // creating a spark session
    val spark = SparkSession.builder().appName("shuffle_sort").config("spark.sql.autoBroadcastJoinThreshold","-1")
                            .getOrCreate()

    // generate some sample data for two data sets
    
    val states = scala.collection.mutable.Map[Int,String]()
    val items = scala.collection.mutable.Map[Int,String]()

    /// generate random seed
    val rnd = new scala.util.Random(42)

    // Initialize states and items purchased

    states += (0 -> "AZ", 1 -> "CO", 2-> "CA", 3-> "TX", 4 -> "NY", 5-> "MI")
    items += (0 -> "SKU-0", 1 -> "SKU-1", 2-> "SKU-2", 3-> "SKU-3", 4 -> "SKU-4",5-> "SKU-5")
  
    // create dataframes
    // import spark implicits

    import spark.implicits._

    val usersDF = (0 to 1000000).map( id => (id, s"user_${id}", 
                                                 s"user_${id}@google.com",
                                                 s"user_state_${states(rnd.nextInt(5))}"))
                                .toDF("uid","login","email","state")


    // needed columns : transaction_id, quantity, user_id, amount, states, items
    val ordersDF = (0 to 1000000).map( id => (id , id, rnd.nextInt(10000), 10 * id * 0.2d,
                                              states(rnd.nextInt(5)), items(rnd.nextInt(5))))
                                 .toDF("transaction_id","quantity","user_id","amount","states","items")

    // perform the join

    val joined_df = usersDF.join(ordersDF, $"uid" === $"user_id")

    //display dataframe
    
    joined_df.show(false) //truncate = false

    // explain the join
    joined_df.explain()

    // check sparkUI for join explaination
    
    Thread.sleep(300000)
    // stop the spark session
    spark.stop()
  }
}
