package com.us.ibm.databasetemplates

import java.util
import java.util.HashMap

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

/**
  * Created by bofashol on 11/5/15.
  */
class NetworkSessionIds(sparkContext: SparkContext, pathToCSV: String) {

  def getNetworkSessionIdsDataFrameFromCSV(): Array[Row] ={
    val sqlContext: SQLContext = new SQLContext(sparkContext)
    val mapValues = new util.HashMap[String, String]()
    mapValues.put("path", pathToCSV)
    mapValues.put("header", "true")
    val sessionIdDataFrame = sqlContext.load(
      "com.databricks.spark.csv",
      mapValues)

    sessionIdDataFrame.registerTempTable("sessionid")
    val sessionIdAggregatedDataFrame = sqlContext.sql("select * from sessionid")
    sessionIdAggregatedDataFrame.collect()
  }
}
