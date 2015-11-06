package com.us.ibm.databasetemplates

import java.util.HashMap

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}

class NetworkURLs(sparkContext: SparkContext, pathToCSV: String) {

  def getNetworkURLsDataFrameFromCSV(): Array[Row] = {
    val sqlContext: SQLContext = new SQLContext(sparkContext)
    val mapValues = new HashMap[String, String]()
    mapValues.put("path", pathToCSV)
    mapValues.put("header", "true")
    val networkURLsFromDataFrames = sqlContext.load(
      "com.databricks.spark.csv",
      mapValues
    )
    networkURLsFromDataFrames.registerTempTable("networkurls")
    val aggregateNetworkURLsDataFrames = sqlContext.sql("select * from networkurls")
    aggregateNetworkURLsDataFrames.collect()
  }

}
