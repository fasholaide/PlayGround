package com.us.ibm.databasetemplates

import java.util
import java.util.HashMap

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Created by bofashol on 11/5/15.
  */
class NetworkULogs(sparkContext: SparkContext, pathToCSV: String) {
  def getNetworkULogsDataFramesFromCSV(): Array[Row] = {
    val sqlContext: SQLContext = new SQLContext(sparkContext)
    val mapvalues = new util.HashMap[String, String]()
    mapvalues.put("path", pathToCSV)
    mapvalues.put("header", "true")
    val networkULogsDataFrames: DataFrame = sqlContext.load(
      "com.databricks.spark.csv",
      mapvalues
    )
    networkULogsDataFrames.registerTempTable("ulogs")
    val aggregagtedULogsDataFrames = sqlContext.sql("select * from ulogs")
    aggregagtedULogsDataFrames.collect()
  }
}
