package io.keepcoding.spark.exercise.batch
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.time.OffsetDateTime

object DeviceBatchJob extends BatchJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master(master = "local[20]")
    .appName(name = "Proyecto SQL Streaming")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(path = s"$storagePath/data")
      .filter(
        $"year"=== filterDate.getYear &&
        $"month"===filterDate.getMonthValue &&
        $"day"===filterDate.getDayOfMonth &&
        $"hour"===filterDate.getHour

      )
  }


  override def readDevicesMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
    .read
    .format ("jdbc")
    .option ("url", jdbcURI)
    .option ("dbTable", jdbcTable)
    .option ("user", user)
    .option ("password", password)
    .load ()

  }

  override def enrichDeviceWithMetadata(deviceDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    deviceDF.as(alias = "a")
      .join(
        metadataDF.as("b"),
        joinExprs = $"a.id" === $"b.id"
      )
      .drop(col = $"b.id")

  }

  override def SumDevicestByAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(cols = $"timestamp", $"antenna_id", $"bytes")
      .groupBy(cols = $"antenna_id", window(timeColumn = $"timestamp", windowDuration = "1 hour").as("window")) //5 minutes
      .agg(
        sum($"bytes").as("value")
      )
      //bytes(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT)
      .select($"window.start".as("timestamp"), $"antenna_id".as("id"), $"value", lit("antenna_total_bytes").as("type"))
  }

  override def SumDevicestByEmail(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(cols = $"timestamp", $"email", $"bytes")
      .groupBy(cols = ($"email").as("id"), window(timeColumn = $"timestamp", windowDuration = "1 hour").as("window")) //5 minutes
      .agg(
        sum($"bytes").as("value")
      )
      //bytes(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT)
      .select($"window.start".as("timestamp"), $"id", $"value", lit("user_total_bytes").as("type"))

  }

  override def SumDevicestByApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(cols = $"timestamp", $"app", $"bytes")
      .groupBy(cols = ($"app").as("id"), window(timeColumn = $"timestamp", windowDuration = "1 hour").as("window")) //5 minutes
      .agg(
        sum($"bytes").as("value")
      )
      //bytes(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT)
      .select($"window.start".as("timestamp"), $"id", $"value", lit("app_total_bytes").as("type"))

  }

  override def SumDevicestByQouta(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(cols = $"timestamp", $"email", $"bytes",$"quota")
      .groupBy(cols = $"email", window(timeColumn = $"timestamp", windowDuration = "1 hour").as("window")) //5 minutes
      .agg(
        sum($"bytes").as("usage"),
        max($"quota").as("quota")
      )
      .where($"usage">$"quota")
      //(email TEXT, usage BIGINT, quota BIGINT, timestamp TIMESTAMP);
      .select($"email",$"usage",$"quota",$"window.start".as("timestamp"))

  }


  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbTable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit ={
    dataFrame
      .write
      .format("parquet")
      .option("path", s"$storageRootPath/data")
      .option("checkpointLocation", s"$storageRootPath/checkpoint")
      .partitionBy("year","month","day","hour")
      .mode(SaveMode.Overwrite)
      .save(path = s"$storageRootPath/historical")
  }

def main(args:Array[String]): Unit= {
  val offsetDateTime=OffsetDateTime.parse("2022-10-29T17:00:00Z")
  val parquetDF =readFromStorage("/temp/device_parquet/",offsetDateTime)
  parquetDF.show(truncate = false)

  val metadataDF = readDevicesMetadata( "jdbc:postgresql://34.69.92.164:5432/postgres", //sql
    "user_metadata",
    "postgres",
    "keepcoding"
  )

  val DeviceMetadataDF = enrichDeviceWithMetadata(parquetDF, metadataDF).cache()
  val aggByByAntennaDF = SumDevicestByAntenna(DeviceMetadataDF)
  val aggByEmailDF = SumDevicestByEmail(DeviceMetadataDF)
  val aggByAppDF = SumDevicestByApp(DeviceMetadataDF)
  val aggByQoutaDF = SumDevicestByQouta(DeviceMetadataDF)

  writeToJdbc(aggByByAntennaDF,  "jdbc:postgresql://34.69.92.164:5432/postgres", "bytes_hourly", "postgres", "keepcoding")
  writeToJdbc(aggByEmailDF,  "jdbc:postgresql://34.69.92.164:5432/postgres","bytes_hourly", "postgres", "keepcoding")
  writeToJdbc(aggByAppDF,  "jdbc:postgresql://34.69.92.164:5432/postgres", "bytes_hourly", "postgres", "keepcoding")
  writeToJdbc(aggByQoutaDF,  "jdbc:postgresql://34.69.92.164:5432/postgres", "user_quota_limit", "postgres", "keepcoding")

  writeToStorage(parquetDF, "/temp/device_parquet/")

  spark.close()


}
  }