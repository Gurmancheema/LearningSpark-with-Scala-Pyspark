// import packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Random

// creating a singleton scala object containing the main method
object optimized_shuffle_sort{
  def main(args:Array[String]){

    // instantiating a spark session
    val spark = SparkSession.builder().appName("opitmized_shuffle")
                            .config("spark.sql.autoBroadCastJoinThreshold","-1")
                            .config("driver-memory","4g")
                            .getOrCreate()

    // creating synthetic datasets having 1 million records each
    // then performing the concept of bucketing to achieve maximum performance 
    // before performing shuffle sort merge join

    val states = scala.collection.mutable.Map[Int,String]()
    val items = scala.collection.mutable.Map[Int,String]()

    // using seed value for random value generator

    val rand = new scala.util.Random(35)

    // initializing the mutable collections with some data points

   states += (0 -> "AZ", 1 -> "CO", 2-> "CA", 3-> "TX", 4 -> "NY", 5-> "MI")
   items += (0 -> "SKU-0", 1 -> "SKU-1", 2-> "SKU-2", 3-> "SKU-3", 4 -> "SKU-4",5-> "SKU-5") 

   // import spark implicits
   import spark.implicits._

   // creating the dataframes out of it
   
   // creating users dataframe having attributes => "uid", "login", "email", "state"
   val usersdf = (0 to 1000000).map( id => ( id, s"user_${id}", s"user_${id}@google.com", states(rand.nextInt(5))))
                             .toDF("uid","login","email","state")

  // creating orders dataframe having attributes => "transaction_id","quantity","user_id","amount","state","items"

  val ordersdf = (0 to 1000000).map( o => (o, o, rand.nextInt(10000), 10 * o*0.2d, states(rand.nextInt(5)),
                                            items(rand.nextInt(5))))
                              .toDF("transaction_id","quantity","user_id","amount","state","items")

    // sort by the user_id, uid 
    // save the dataframes as managed tables by bucketing them in parquet format

    usersdf.orderBy(asc("uid")).write.format("parquet").bucketBy(8,"uid").saveAsTable("Userstbl")

    ordersdf.orderBy(asc("user_id")).write.format("parquet").bucketBy(8,"user_id").saveAsTable("Orderstbl")

    // cache the tables created
    spark.sql("CACHE TABLE Userstbl")
    spark.sql("CACHE TABLE Orderstbl")

    // read the tables again in dataframes

    val users_table = spark.table("Userstbl")

    val orders_table = spark.table("Orderstbl")

  
    // now perform the shuffle sort merge join , and refer to spark UI 
    // obeserve that this time the no sorting was necessary as we already performed the operation
    // before saving them as tables

    val joined_df = users_table.join(orders_table, $"uid" === $"user_id")

    // display the joined dataframe
    joined_df.show(false) //truncate output => false

    // explain the physical plan

    joined_df.explain()

    // add timer to see sparkUI before closing spark session
    
    Thread.sleep(600000)

    //stop the spark session
    spark.stop()
  }
}
