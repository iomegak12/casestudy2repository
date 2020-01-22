// Databricks notebook source
val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "c5c6c70a-cb22-48d2-b7c4-7fc39de875f5",
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope = "training-scope", key = "dlstoken"),
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/381a10df-8e85-43db-86e1-8893b075b027/oauth2/token")

dbutils.fs.mount(
  source = "abfss://data@casestudydls.dfs.core.windows.net/",
  mountPoint = "/mnt/data",
  extraConfigs = configs)

// COMMAND ----------

// MAGIC %fs
// MAGIC 
// MAGIC ls /mnt/data