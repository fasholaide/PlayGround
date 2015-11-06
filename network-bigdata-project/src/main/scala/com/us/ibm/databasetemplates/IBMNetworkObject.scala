package com.us.ibm.databasetemplates

import java.util
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row}

object IBMNetworkObject {
  val usage: String = "Usage: \n" +
    "network-bigdata-project-1.0-SNAPSHOT.jar OOOPS <sessionidcsvpath> <urlcsvpath> <ulogcsvpath>"

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println(usage)
      System.exit(0)
    }
    val pathToNetworkSessionIdCSV: String = args(0)
    val pathToNetworkURLCSV: String = args(1)
    val pathToNetworkULogsCSV: String = args(2)

    val sparkConf: SparkConf = new SparkConf().setAppName("IBMNetworkParser").setMaster("spark://bofashol-ThinkPad-T430:7077")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val aggregatedNewtorkURLDataFrames = new NetworkURLs(sparkContext, pathToNetworkURLCSV)
    val aggregatedNewtorkURLDataFramesArray = aggregatedNewtorkURLDataFrames.getNetworkURLsDataFrameFromCSV()

    val aggregateNetworkSessionIDDataFrames = new NetworkSessionIds(sparkContext, pathToNetworkSessionIdCSV)
    val aggregateNetworkSessionIDDataFramesArray = aggregateNetworkSessionIDDataFrames.getNetworkSessionIdsDataFrameFromCSV()

    val aggregateNetworkULogsDataFrames = new NetworkULogs(sparkContext, pathToNetworkULogsCSV)
    val aggregateNetworkULogsDataFramesArray = aggregateNetworkULogsDataFrames.getNetworkULogsDataFramesFromCSV()

    val filteredURLS = filterURLBySessionUserID(aggregateNetworkSessionIDDataFramesArray, aggregatedNewtorkURLDataFramesArray)
    val filteredULogs = filterULogsBySessionID(aggregateNetworkSessionIDDataFramesArray, aggregateNetworkULogsDataFramesArray)

    print("#########This is the filteredULogs from the existing userIDs#######\n\n" + filteredULogs)

    print("#########This is the filteredURLs from the existing userIDs#######\n\n" + filteredURLS.map(print(_)))
  }

  def filterURLBySessionUserID(
                                aggregatedNetworkSessionIDDataFrame: Array[Row],
                                aggregatedNetworkURLDataFrame: Array[Row]
                              ): Array[Row] = {
    aggregatedNetworkSessionIDDataFrame
      .map(_.getAs("userId").toString.trim)
      .flatMap(userId => aggregatedNetworkURLDataFrame.filter(_.getAs("uid") == userId))
  }

  def filterULogsBySessionID(
                              aggregatedNetworkSessionIDDataFrame: Array[Row],
                              aggregatedNeworkULogsDataFrame: Array[Row]): Array[Row] = {
    aggregatedNetworkSessionIDDataFrame
      .map(_.getAs("userId").toString.trim)
      .flatMap(userId => aggregatedNeworkULogsDataFrame.filter(_.getAs("uid") == userId))
  }
}
