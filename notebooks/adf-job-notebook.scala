// Databricks notebook source
// MAGIC %sql
// MAGIC 
// MAGIC CREATE DATABASE IF NOT EXISTS XDb

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS XDb.Customers
// MAGIC   USING CSV
// MAGIC   OPTIONS
// MAGIC   (
// MAGIC     path "/mnt/data/customers/*.csv",
// MAGIC     inferSchema True,
// MAGIC     sep ",",
// MAGIC     header "True"
// MAGIC   )

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DROP VIEW IF EXISTS XDb.ActiveCustomersView;
// MAGIC 
// MAGIC CREATE VIEW XDb.ActiveCustomersView
// MAGIC   AS
// MAGIC   SELECT * FROM XDb.Customers
// MAGIC   WHERE status = 1
// MAGIC   

// COMMAND ----------

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

val format = "yyyyMMdd_HHmmss"
val dtf = DateTimeFormatter.ofPattern(format)
val ldt = LocalDateTime.now()

val statement = "SELECT * FROM XDb.ActiveCustomersView"
val activeCustomers = spark.sql(statement)
val fileName = "/mnt/data/processed-customers/" + ldt.format(dtf)

activeCustomers
  .write
  .format("com.databricks.spark.csv")
  .option("header", true)
  .option("sep",",")
  .save(fileName)