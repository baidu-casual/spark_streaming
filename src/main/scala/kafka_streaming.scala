package main


import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.streaming.Trigger

/*import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD*/

class sparkStreamng {



    def temp1(batchDf: DataFrame, batchId: Long): Unit = {
        batchDf.show(false)
    }
  def temp2(): Unit = {
    val conf = new SparkConf().setAppName("Spark4").setMaster("local");
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark Structured Streaming with Kafka Demo")
    .config(conf)
    .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    println("Spark Structured Streaming with Kafka Demo Application Started ...")

    val KAFKA_TOPIC_NAME_CONS = "testtopic"
    val KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputtopic"
    val KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"

    System.setProperty("HADOOP_USER_NAME","hadoop")    

    val transaction_detail_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
      .option("subscribe", KAFKA_TOPIC_NAME_CONS)
      .option("startingOffsets", "earliest")
      .load()

    println("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()
    /**
      * Lamda Function
      */
      /*
    transaction_detail_df.writeStream.foreachBatch((batchDf: DataFrame, batchId: Long) => {
        batchDf.show(false)
    }).start().awaitTermination()*/

    transaction_detail_df
                    .writeStream
                    .foreachBatch(temp1 _)
                    .start()
                    .awaitTermination()
    
    println("Spark Structured Streaming with Kafka Demo Application Completed.")
  }
  
}


//
object kafka_streaming {  
  def main(args: Array[String]): Unit = {
    val sS = new sparkStreamng
    sS.temp2()
  }
}