package edu.ateneo.nrg.spark

import java.sql.Timestamp

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.SparkFiles._ 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.unix_timestamp


/** Run sample SQL operations on a dataset. */
object SparkCanvasBatchCleaner {
  
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine
    val spark = SparkSession
        .builder
        .appName("SparkCanvasBatchCleaner")
        .master("local[*]")
        .getOrCreate()
  
    // Load each line of the source data into an RDD
    val canvasData = spark.read
        .format("json")
        .option("inferSchema" , "true")
        .load(SparkFiles.get("09-19-03.jsonl"))

    canvasData.withColumn("metadata_event_time", unix_timestamp($"metadata_event_time", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast(TimestampType))
    
    canvasData.createOrReplaceTempView("canvasdata")

    // SQL can be run over DataFrames that have been registered as a table.
    val canvasdata = spark.sql("SELECT metadata_event_time FROM canvasdata LIMIT 5")

    canvasData.printSchema()

    canvasdata.show()

    

    // results.foreach(println)
    spark.stop()
  }
    
}
  
