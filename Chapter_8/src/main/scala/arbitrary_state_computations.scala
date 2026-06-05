// import packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.GroupState

// defining a case class to convert the incoming streaming events into class objects
// "What does each incoming record look like?"

case class User(userId:String)

// defining a case class that will store the state of the records we want to maintain
// for keeping it simple, i am just storing the count of each record as state
// "What information should Spark remember?"

case class UserState(count:Long)

// creating a singleton scala object containing the main method & update function

object arbitrary_state_ops{

  // defining the update function which contains the code idea of arbitrary state computation
  // "Given the old state and new events, how do I compute the new state?"

  def update_function(userId:String, events:Iterator[User], state:GroupState[UserState]): (String,Long) = {
    val oldcount = if(state.exists)
                      state.get.count
                    else
                      0L
    val newcount = oldcount + events.size

    state.update(UserState(newcount))

    (userId,newcount)
  }
  def main(args:Array[String]){
    
    // creating a spark session

    val spark = SparkSession.builder().appName("arbitrary_ops").getOrCreate()


    // reading the streaming data from socket

    val read_stream = spark.readStream.format("socket").option("host","localhost").option("port",9999).load()

    // converting the read dataframe stream into class objects
    // currently this stream is being read as Dataset[Row].
    // i will tell spark that these values are Strings, so that Dataset[Row] => Dataset[String] (String values)
    // further i will map these values to be an object of the class "User" which will help me maintain a state
    import spark.implicits._

    val stream_lines_data = read_stream.as[String].map( x=> User(x))


    // now each streamed record is an object of class "User" which means it contains
    // User ( userId= "A")
    // now spark sees userId=A, userId=B & so on for each streamed record, and now we can maintain the state for this

    // let's perform the arbitrary computation now
    val result = stream_lines_data.groupByKey(_.userId).mapGroupsWithState(update_function _)

    // sink the results to console
    val query =result.writeStream.outputMode("update").format("console").start()

    // await termination first before stopping session
    query.awaitTermination()
    //stop the spark session
    spark.stop()
  }
}


