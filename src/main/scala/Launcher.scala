import org.slf4j.LoggerFactory
import com.argcv.spark.utils.SparkHelper.sc
import com.argcv.spark.utils.SparkHelper.ss
import org.apache.spark.rdd.RDD

import org.apache.commons.io.IOUtils
import java.net.URL
import java.nio.charset.Charset

/**
  * @author yu
  */
object Launcher extends App {
  import ss.implicits._
  //
  lazy val logger = LoggerFactory.getLogger(Launcher.getClass)
  val timeStart = System.currentTimeMillis()

  // Zeppelin creates and injects sc (SparkContext) and sqlContext (HiveContext or SqlContext)
  // So you don't need create them manually

  // load bank data
  val bankText: RDD[String] = sc.parallelize(
    IOUtils.toString(
      new URL("https://s3.amazonaws.com/apache-zeppelin/tutorial/bank/bank.csv"),
      Charset.forName("utf8")).split("\n"))
  val bank = bankText.map(s => s.split(";")).filter(s => s(0) != "\"age\"").map(
    s => Bank(s(0).toInt,
      s(1).replaceAll("\"", ""),
      s(2).replaceAll("\"", ""),
      s(3).replaceAll("\"", ""),
      s(5).replaceAll("\"", "").toInt
    )
  ).toDF()

  case class Bank(age: Integer, job: String, marital: String, education: String, balance: Integer)
  bank.createOrReplaceTempView("bank")

  ss.sql("select age, job from bank where age < 35 order by age desc limit 10").show()


  logger.info(s"finished , all time cost : ${System.currentTimeMillis() - timeStart}")
}
