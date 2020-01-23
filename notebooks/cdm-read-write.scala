// Databricks notebook source
val data = Seq(("C10001", "Awesome Product"), ("C10002", "Awful Products I have purchased this week!"), ("C10003", "Best Purchase")).toDF("customerid", "remarks")

// COMMAND ----------

import java.io._
import java.net._
import java.util._
import javax.net.ssl.HttpsURLConnection
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import scala.util.parsing.json._

case class Language(documents: Array[LanguageDocuments], errors: Array[Any]) extends Serializable
case class LanguageDocuments(id: String, detectedLanguages: Array[DetectedLanguages]) extends Serializable
case class DetectedLanguages(name: String, iso6391Name: String, score: Double) extends Serializable

case class Sentiment(documents: Array[SentimentDocuments], errors: Array[Any]) extends Serializable
case class SentimentDocuments(id: String, score: Double) extends Serializable

case class RequestToTextApi(documents: Array[RequestToTextApiDocument]) extends Serializable
case class RequestToTextApiDocument(id: String, text: String, var language: String = "") extends Serializable

object SentimentDetector extends Serializable {
    val accessKey = "4d2f26e17f344f31956c382be3400201"
    val host = "https://iomegatextanalytics.cognitiveservices.azure.com/"
    val languagesPath = "/text/analytics/v2.1/languages"
    val sentimentPath = "/text/analytics/v2.1/sentiment"
    val languagesUrl = new URL(host+languagesPath)
    val sentimenUrl = new URL(host+sentimentPath)
    val g = new Gson

    def getConnection(path: URL): HttpsURLConnection = {
        val connection = path.openConnection().asInstanceOf[HttpsURLConnection]
        connection.setRequestMethod("POST")
        connection.setRequestProperty("Content-Type", "text/json")
        connection.setRequestProperty("Ocp-Apim-Subscription-Key", accessKey)
        connection.setDoOutput(true)
        return connection
    }

    def prettify (json_text: String): String = {
        val parser = new JsonParser()
        val json = parser.parse(json_text).getAsJsonObject()
        val gson = new GsonBuilder().setPrettyPrinting().create()
        return gson.toJson(json)
    }

    def processUsingApi(request: RequestToTextApi, path: URL): String = {
        val requestToJson = g.toJson(request)
        val encoded_text = requestToJson.getBytes("UTF-8")
        val connection = getConnection(path)
        val wr = new DataOutputStream(connection.getOutputStream())
        wr.write(encoded_text, 0, encoded_text.length)
        wr.flush()
        wr.close()

        val response = new StringBuilder()
        val in = new BufferedReader(new InputStreamReader(connection.getInputStream()))
        var line = in.readLine()
        while (line != null) {
            response.append(line)
            line = in.readLine()
        }
        in.close()
        return response.toString()
    }

    def getLanguage (inputDocs: RequestToTextApi): Option[Language] = {
        try {
            val response = processUsingApi(inputDocs, languagesUrl)
            val niceResponse = prettify(response)
            val language = g.fromJson(niceResponse, classOf[Language])
            if (language.documents(0).detectedLanguages(0).iso6391Name == "(Unknown)")
                return None
            return Some(language)
        } catch {
            case e: Exception => return None
        }
    }

    def getSentiment (inputDocs: RequestToTextApi): Option[Sentiment] = {
        try {
            val response = processUsingApi(inputDocs, sentimenUrl)
            val niceResponse = prettify(response)
            val sentiment = g.fromJson(niceResponse, classOf[Sentiment])
            return Some(sentiment)
        } catch {
            case e: Exception => return None
        }
    }
}

val toSentiment = (textContent: String) =>
        {
            val inputObject = new RequestToTextApi(Array(new RequestToTextApiDocument(textContent, textContent)))
            val detectedLanguage = SentimentDetector.getLanguage(inputObject)
            detectedLanguage match {
                case Some(language) =>
                    if(language.documents.size > 0) {
                        inputObject.documents(0).language = language.documents(0).detectedLanguages(0).iso6391Name
                        val sentimentDetected = SentimentDetector.getSentiment(inputObject)
                        sentimentDetected match {
                            case Some(sentiment) => {
                                if(sentiment.documents.size > 0) {
                                    sentiment.documents(0).score.toString()
                                }
                                else {
                                    "Error happened when getting sentiment: " + sentiment.errors(0).toString
                                }
                            }
                            case None => "Couldn't detect sentiment"
                        }
                    }
                    else {
                        "Error happened when getting language" + language.errors(0).toString
                    }
                case None => "Couldn't detect language"
            }
        }

// COMMAND ----------

spark.udf.register("toSentiment", toSentiment)

// COMMAND ----------

data.createOrReplaceTempView("customer_remarks")

// COMMAND ----------

val appId = "c5c6c70a-cb22-48d2-b7c4-7fc39de875f5"
val tenantId = "381a10df-8e85-43db-86e1-8893b075b027"
val secret = "Kgw?_2=/FxwFsFI1aA7NJqTA4eLoK8Zu"
val cdmModel = "crmsystem"
val processedRemarks = spark.sql("SELECT customerid, remarks, toSentiment(remarks) as score FROM customer_remarks")
val outputLocation = "https://casestudydls.dfs.core.windows.net/data/processed-remarks"

// COMMAND ----------

display(processedRemarks)

// COMMAND ----------

data
  .write
  .format("com.microsoft.cdm")
  .option("entity", "customerremarks")
  .option("appId", appId)
  .option("appKey", secret)
  .option("tenantId", tenantId)
  .option("cdmFolder", outputLocation)
  .option("cdmModelName", cdmModel)
  .save()

// COMMAND ----------

case class CustomerRemarks(customerid: String, remarks: String)

val appId2 = "c5c6c70a-cb22-48d2-b7c4-7fc39de875f5"
val tenantId2 = "381a10df-8e85-43db-86e1-8893b075b027"
val secret2 = "Kgw?_2=/FxwFsFI1aA7NJqTA4eLoK8Zu"

val df = spark.read.format("com.microsoft.cdm")
                .option("cdmModel", "https://casestudydls.dfs.core.windows.net/data/processed-remarks/model.json")
                .option("entity", "customerremarks")
                .option("appId", appId2)
                .option("appKey", secret2)
                .option("tenantId", tenantId2)
                .load()
                .as[CustomerRemarks]
				
display(df)
