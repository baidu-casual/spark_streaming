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
  def streamingFunction(batchDf: DataFrame, batchId: Long): Unit = {
        println("\n\n\t\tBATCH "+batchId+"\n\n")
        batchDf.show(false)
    }
  def kafkaConsume(kafkaTopicName: String = "test", kafkaServer: String = "localhost:9092"): Unit = {
    val conf = new SparkConf().setAppName("KAFKA").setMaster("local");
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark Kafka Demo")
    .config(conf)
    .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    println("\n\n\t\tKafka Demo Application Started ...\n\n")

    System.setProperty("HADOOP_USER_NAME","hadoop")    

    val transaction_detail_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", kafkaTopicName)
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
                    .format("console")
                    .outputMode("append")
                    .foreachBatch(streamingFunction _)
                    .start()
                    .awaitTermination()
    
    println("\n\n\t\tKafka Demo Application Completed ...\n\n")
  }
  
}


//
object kafkaStreamingConsumer {  
  def main(args: Array[String]): Unit = {
    val sS = new sparkStreamng
    sS.kafkaConsume()
  }
}