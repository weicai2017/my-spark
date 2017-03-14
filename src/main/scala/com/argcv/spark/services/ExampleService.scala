package com.argcv.spark.services

import com.argcv.spark.utils.Awakable
import com.argcv.spark.utils.SparkHelper._

/**
  *
  * @author Yu Jing <yu@argcv.com> on 3/13/17
  */
object ExampleService extends Awakable {
  import ss.implicits._

  val projCWD = ""

  def loadData(path:String = s"file://${new java.io.File(".").getAbsolutePath.dropRight(1)}/data/case.csv"): Unit = {
    val patientEvents = ss.sqlContext.read.format("com.databricks.spark.csv").load(path).
      toDF("patientId", "eventId", "date", "rawvalue").
      withColumn("value", 'rawvalue.cast("Double"))
    patientEvents.createOrReplaceTempView("events")
    ss.sql("select patientId, eventId, count(*) count from events where eventId like 'DIAG%' group by patientId, eventId order by count desc").show
  }
}
