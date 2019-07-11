package runnable

import config.ConfigManager
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import utils.ContextUtil

object RunJob {

  def main(args: Array[String]): Unit = {
    query()
  }

  def query(): Unit ={
    val ssc: SparkSession = ContextUtil.getHiveContext
    val sql: String = ConfigManager.getProp("dm_sql")
    val df: DataFrame = ssc.sql(sql)
    // 保存到hdfs
    df.write.csv("hdfs://t21:9000/spark/datawarehouse")
  }

}
