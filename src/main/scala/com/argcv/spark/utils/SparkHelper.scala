package com.argcv.spark.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * configure hdfs URI and master url here
  */
object SparkHelper {
  lazy val hdfsURI = "hdfs://localhost:9000"
  lazy val sparkMasterURL = "local[*]"

  lazy val ss: SparkSession = SparkHelper.createSparkSession(
    appName = "my-spark",
    masterUrl = sparkMasterURL,
    cfg = {
      _.set("spark.executor.memory", "8G")
        .set("spark.driver.memory", "2G")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer", "24")
    }
  )
  lazy val sc: SparkContext = SparkHelper.createSparkContext(
    appName = "my-spark",
    masterUrl = sparkMasterURL,
    cfg = {
      _.set("spark.executor.memory", "8G")
        .set("spark.driver.memory", "2G")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer", "24")
    }
  )

  def hdfs(sc: SparkContext = sc): FileSystem = {
    val hadoopConf: Configuration = sc.hadoopConfiguration
    org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfsURI), hadoopConf)
  }

  def createSparkContext: SparkContext = createSparkContext("my-spark")

  def createSparkContext(appName: String,
                         masterUrl: String = sparkMasterURL,
                         cfg: (SparkConf) => SparkConf = { in => in }): SparkContext = {
    createSparkSession(appName, masterUrl, cfg).sparkContext
  }

  def createSQLContext: SQLContext = createSQLContext("css-feat")

  def createSQLContext(appName: String,
                       masterUrl: String = sparkMasterURL,
                       cfg: (SparkConf) => SparkConf = { in => in }): SQLContext = {
    createSparkSession(appName, masterUrl, cfg).sqlContext
  }

  def createSparkSession(appName: String,
                         masterUrl: String = sparkMasterURL,
                         cfg: (SparkConf) => SparkConf = { in => in }): SparkSession = {
    SparkSession.builder().config(sparkConf(appName, masterUrl, cfg)).getOrCreate()
  }

  def sparkConf(appName: String, masterUrl: String, cfg: (SparkConf) => SparkConf): SparkConf = {
    cfg(new SparkConf()
      .setAppName(appName)
      .setMaster(masterUrl)
      .set("spark.executor.memory", "1G")
      .set("spark.driver.memory", "500M")
      //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //            .set("spark.kryoserializer.buffer","24")
      //    conf.registerKryoClasses(Array(classOf[Event]))
      //    conf.registerKryoClasses(Array(classOf[Booster]))
    )
  }

  def createSparkSession: SparkSession = createSparkSession("css-feat")
}