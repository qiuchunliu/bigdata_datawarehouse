package runnable

import java.util.Properties

import config.ConfigManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.{ContextUtil, JdbcUtil}

object RunJob {

  def main(args: Array[String]): Unit = {
    query()
  }


  def query(): Unit ={
    val ssc: SparkSession = ContextUtil.getHiveContext
//    val sql: String = ConfigManager.getProp("dm_sql")
    val dm_wide_table: String = ConfigManager.getProp("dm_wide_table")
    val df: DataFrame = ssc.sql(dm_wide_table)
//    // 保存到hdfs
//    df.write.csv("hdfs://t21:9000/spark/datawarehouse2")
    val prop: Properties = JdbcUtil.getJdbcParams
    val jdbc_url: String = prop.getProperty("url")
//    // 写入到mysql
//    // 要先在mysql里建表，指定好数据类型
//    df.write.mode(saveMode = "append").jdbc(jdbc_url, "dm_user_basic1", prop)


    // 导入到hive >>> 成功 ！
    df.write.mode(saveMode = "append").insertInto("qfbap_dm.dm_user_basic")
  }

}
