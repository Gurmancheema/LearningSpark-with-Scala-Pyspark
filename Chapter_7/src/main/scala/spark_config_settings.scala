// import packages

import org.apache.spark.sql.SparkSession

// defining a singleton scala object containing the main method

object spark_configs{
  def main(args:Array[String]){

    // creating a spark session

    val spark = SparkSession.builder
                            .config("spark.sql.shuffle.partitions", 5)
                            .config("spark.executor.memory","2g")
                            .appName("spark_configs").getOrCreate()


    // fetching the current session's configs

    val configs = spark.conf.getAll

    // looping through it & printing the configs as they get stored as key,value

    configs.foreach {case(k,v) => println(s"$k -> $v") }

    // changing some configs again
    
    println("*********** Setting partitions shuffle to default parallelism ************")
    spark.conf.set("spark.sql.shuffle.partitions",spark.sparkContext.defaultParallelism)

    val updated_configs = spark.conf.getAll

    updated_configs.foreach {case(k,v) => println(s"$k->$v") }
  
    // stop the spark session
    Thread.sleep(60000)
    spark.stop()
  }
}


