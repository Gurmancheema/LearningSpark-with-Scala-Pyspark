// importing packages

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

// creating a singleton scala object containing the main method

object hash_join{
  def main(args:Array[String]){

    // creating a sparksession
    val spark = SparkSession.builder.appName("broadcasthashjoin").getOrCreate()

    // creating a simple small dataframe of sales-data
    // this will act as bigger dataframe
    import spark.implicits._
    val sales_data_rows =  Seq(
                                (1,101,2,45.0),
                                (2,102,5,56.0),
                                (3,103,5,89.0),
                                (4,104,7,76.34),
                                (5,103,4,89.34)
                              ).toDF("sales_id","product_id","quantity","unit_price")

    // print schema to verify formed dataframe
    sales_data_rows.printSchema()

    // Now creating products dataframe, this will be smaller dataframe
    // this dataframe will be broadcasted

    val product_data_rows = Seq (
                                  (101, "tv", "appliances"),
                                  (102, "washing_machine","appliances"),
                                  (103, "mobile_phone", "hand-held"),
                                  (104, "headphones", "hand-held")
                                ).toDF("product_id","product_name","category")

    // print schema again to verify this dataframe
    product_data_rows.printSchema()

    // let's perform the broadcast hash join now , based on common key; "product_id"

    val joined_df = sales_data_rows.join(broadcast(product_data_rows),
                                      sales_data_rows("product_id") === product_data_rows("product_id"),
                                      "inner"
                                      )
    // print schema of joined_df
    joined_df.printSchema()

    val query_joined_df = joined_df.select("*")
                                   .orderBy("sales_id")

      query_joined_df.show()

    //stop the spark session
    spark.stop()
  }
}
