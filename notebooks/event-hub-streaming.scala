// Databricks notebook source
import org.apache.spark.eventhubs._
import org.apache.spark.sql.functions._

// COMMAND ----------

var eventHubUrl = "Endpoint=sb://casestudyadxeventhubs.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=dhuRBXoxXX6+iLT8jnu8dG2eRF28og49sjDPs2HlNEo="
val connectionString = ConnectionStringBuilder(eventHubUrl).setEventHubName("adbmessages").build
val eventHubsConf = EventHubsConf(connectionString).setStartingPosition(EventPosition.fromEndOfStream)

// COMMAND ----------

val eventhubs = 
  spark
    .readStream
    .format("eventhubs")
    .options(eventHubsConf.toMap)
    .load()

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

// COMMAND ----------

var query = 
  eventhubs
   .select(
     get_json_object(($"body").cast("string"), "$.zip").alias("zip"),    
     get_json_object(($"body").cast("string"), "$.hittime").alias("hittime"), 
     date_format(get_json_object(($"body").cast("string"), "$.hittime"), "dd.MM.yyyy").alias("day"))

// COMMAND ----------

val queryOutput =
  query
    .writeStream
    .format("parquet")
    .option("path", "/data/processed")
    .option("checkpointLocation", "/data/checkpoints")
    .partitionBy("zip", "day")
    .trigger(ProcessingTime("25 seconds"))
    .start()

// COMMAND ----------

// MAGIC %fs
// MAGIC 
// MAGIC ls /data/processed/

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS StreamData
// MAGIC USING PARQUET
// MAGIC OPTIONS
// MAGIC (
// MAGIC   PATH "/data/processed/"
// MAGIC )

// COMMAND ----------

spark.catalog.refreshTable("StreamData")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT COUNT(*) FROM StreamData