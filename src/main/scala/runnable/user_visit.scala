package runnable

import constant.Constant
import org.apache.spark.sql.SparkSession

object user_visit {

  val ssc: SparkSession =
    SparkSession
      .builder()
      .enableHiveSupport()
      .appName(Constant.SPARK_APPNAME)
      .master(Constant.SPARK_MASTER)
      .getOrCreate()


  def main(args: Array[String]): Unit = {
    run_it()
  }

  def run_it(): Unit ={
    ssc.sql("").show(100)
  }

}
