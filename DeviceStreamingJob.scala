package io.keepcoding.spark.exercise.streaming
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, types}
import org.apache.spark.sql.functions._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object DeviceStreamingJob extends StreamingJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master(master="local[20]")
    .appName(name = "Proyecto SQL Streaming")
    .getOrCreate()
  import spark.implicits._
  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format(source="kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame ={
    val jsonSchema=StructType(Seq(
        StructField("timestamp", TimestampType, nullable = false),
        StructField("id",StringType,nullable=false),
        StructField("antenna_id",StringType,nullable=false),
        StructField("bytes",LongType,nullable=false),
        StructField("app",StringType,nullable=false)

      )
    )
    dataFrame
      .select(from_json($"value".cast(StringType), jsonSchema).as(alias = "json"))
      .select(cols =$"json.*")
  }

  override def readDeviceMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame ={
    spark
      .read
      .format("jdbc")
      .option("url",jdbcURI )
      .option("dbTable",jdbcTable)
      .option("user",user )
      .option("password", password)
      .load()


  }

  override def enrichDeviceWithMetadata(deviceDF: DataFrame, metadataDF: DataFrame): DataFrame ={
    deviceDF.as(alias  ="a")
    .join(
      metadataDF.as("b"),
      joinExprs=$"a.id"===$"b.id"
    )
      .drop (col=$"b.id")

  }

  override def SumDevicestByAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(cols = $"timestamp", $"antenna_id", $"bytes")
      .withWatermark(eventTime = "timestamp", delayThreshold = "60 seconds") //1 minute
      .groupBy(cols = $"antenna_id", window(timeColumn = $"timestamp", windowDuration = "5 minutes").as("window")) //5 minutes
      .agg(
        sum($"bytes").as("value")
      )
      //bytes(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT)
      .select($"window.start".as("timestamp"), $"antenna_id".as("id"), $"value", lit("antenna_total_bytes").as("type"))
  }
  override def SumDevicestById(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(cols = $"timestamp", $"id", $"bytes")
      .withWatermark(eventTime = "timestamp", delayThreshold = "10 seconds") //1 minute
      .groupBy(cols = $"id", window(timeColumn = $"timestamp", windowDuration = "30 seconds").as("window")) //5 minutes
      .agg(
        sum($"bytes").as("value")
      )
      //bytes(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT)
      .select($"window.start".as("timestamp"), $"id", $"value", lit("user_total_bytes").as("type"))

  }

  override def SumDevicestByApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(cols = $"timestamp", $"app", $"bytes")
      .withWatermark(eventTime = "timestamp", delayThreshold = "10 seconds") //1 minute
      .groupBy(cols = ($"app").as("id"), window(timeColumn = $"timestamp", windowDuration = "30 seconds").as("window")) //5 minutes
      .agg(
        sum($"bytes").as("value")
      )
      //bytes(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT)
      .select($"window.start".as("timestamp"), $"id", $"value", lit("app_total_bytes").as("type"))

  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future{
    dataFrame
      .writeStream
      .foreachBatch{
        (batch: DataFrame,_:Long)=>{
          batch
            .write
            .mode(SaveMode.Append)
            .format("jdbc")
            .option("url", jdbcURI)
            .option("dbTable", jdbcTable)
            .option("user", user)
            .option("password", password)
            .save()
        }
      }
      .start()
      .awaitTermination()

  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    dataFrame
      .select(
        $"timestamp",$"id",$"antenna_id",$"bytes",$"app",
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour"),
      )
      .writeStream
      .format("parquet")
      .option("path", s"$storageRootPath/data" )
      .option("checkpointLocation", s"$storageRootPath/checkpoint")
      .partitionBy("year", "month", "day", "hour")
      .start
      .awaitTermination()

  }

  def main(args:Array[String]):Unit={
    //run(args=)
    val kafkaDF= readFromKafka("34.125.241.183:9092", topic = "devices") //kafka
    val parseDF=parserJsonData(kafkaDF)
    val storageFuture=writeToStorage(parseDF,storageRootPath = "/temp/device_parquet/")

    val metadataDF= readDeviceMetadata(
      "jdbc:postgresql://34.69.92.164:5432/postgres",  //sql
      "user_metadata",
      "postgres",
      "keepcoding"
    )
    val enrichDF=enrichDeviceWithMetadata(parseDF,metadataDF)
    val sumBytes=SumDevicestByAntenna(enrichDF)
    val sumBytes_1=SumDevicestById(enrichDF)
    val sumBytes_2=SumDevicestByApp(enrichDF)
    val jdbcFuture= writeToJdbc(sumBytes, "jdbc:postgresql://34.69.92.164:5432/postgres",jdbcTable = "bytes",user="postgres" , password = "keepcoding")//sql
    val jdbcFuture1= writeToJdbc(sumBytes_1, "jdbc:postgresql://34.69.92.164:5432/postgres",jdbcTable = "bytes",user="postgres" , password = "keepcoding")
    val jdbcFuture2= writeToJdbc(sumBytes_2, "jdbc:postgresql://34.69.92.164:5432/postgres",jdbcTable = "bytes",user="postgres" , password = "keepcoding")
    Await.result(Future.sequence(Seq(storageFuture, jdbcFuture, jdbcFuture1, jdbcFuture2)), Duration.Inf)

    spark.close()

  }

}
