// import packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}

// creating a scala object containing the main method
object indexing_ohe{
  def main(args:Array[String]) {

    // instantiating a spark session
    val spark = SparkSession.builder().appName("indexing_and_onehotencoding").getOrCreate()

    // defining train data source path
    val train_df_path = "/home/gurman/spark_prac/LearningSpark-with-Scala-Pyspark/Chapter_10/data/train"

    // reading the data into a dataframe
    val traindf = spark.read.format("parquet").load(train_df_path)

    // verify by printing schema & shape of dataframe
    traindf.printSchema()
    println(s"No. of rows: ${traindf.count()} & No. of columns: ${traindf.columns.length}")

    // Step 1:  Identifying categorical columns, means columns having "StringType" as datatype
    // Firstly, let's list the datatype of each column
    
    traindf.dtypes.foreach(println)
 
    // Now, filtering out the columns having dtype as "StringType"
    // this will return an Array with all columns matching the condition

    val categoricalcols = traindf.dtypes.filter(_._2 == "StringType").map(_._1)
   
    categoricalcols.foreach(println)

    // Step 2: Instantiating the StringIndexer object with setter methods
    // also defining the format of output cols as pre-requisite to the stringindexer

    val indexed_output_cols = categoricalcols.map(_ +"INDEX")

    val stringindexer = new StringIndexer().setInputCols(categoricalcols).setOutputCols(indexed_output_cols)

    //  uptil now this is just an estimator instantiation, it has learnt nothing yet
    //  so let's feed our train data into this estimator, for it will return a transformer
    //  note that it's not a dataframe, it contains the information learned from the training data

    val stringindexer_learning_model = stringindexer.fit(traindf)

    // Now, we need to pass the dataframe again to this trained estimator
    // so that it can apply it's learnings to the dataframe & make modifications
    // this will return a dataframe now

    val transformed_indexing_model = stringindexer_learning_model.transform(traindf)

    transformed_indexing_model.printSchema()
    transformed_indexing_model.select("bed_type","bed_typeINDEX").show()
  
    // Step 3: Passing the transformed indexed model to OneHotEncoder estimator
    //


    // stop the spark session
    spark.stop()
  }
}
