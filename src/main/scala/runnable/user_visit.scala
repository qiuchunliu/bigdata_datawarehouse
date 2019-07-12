package runnable

import org.apache.spark.sql.SparkSession
import utils.ContextUtil

object user_visit {

    private val ssc: SparkSession = ContextUtil.getHiveContext

  def main(args: Array[String]): Unit = {
    run_it()
  }

  def run_it(): Unit ={
    ssc.sql("").show(100)
  }

}
