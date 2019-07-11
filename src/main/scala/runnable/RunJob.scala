package runnable

import java.util.Properties

import config.ConfigManager
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import utils.{ContextUtil, JdbcUtil}

object RunJob {

  def main(args: Array[String]): Unit = {
    query()
  }


  /*
   * 获取jdbc参数
   */
  def getJdbcProp: Properties ={
    val params: Properties = JdbcUtil.getJdbcParams
    params
  }
  def query(): Unit ={
    val ssc: SparkSession = ContextUtil.getHiveContext
//    val sql: String = ConfigManager.getProp("dm_sql")
    val dm_wide_table: String = ConfigManager.getProp("dm_wide_table")
    val df: DataFrame = ssc.sql(dm_wide_table)
//    // 保存到hdfs
//    df.write.csv("hdfs://t21:9000/spark/datawarehouse2")
    val prop: Properties = getJdbcProp
    val jdbc_url: String = prop.getProperty("url")
    // 写入到mysql
    df.write.jdbc(jdbc_url, "dm_user_basic", prop)


//    // 导入到hive >>> 成功 ！
//    df.write.mode(saveMode = "append").insertInto("qfbap_dm.dm_user_basic")
  }

}
