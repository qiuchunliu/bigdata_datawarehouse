package utils

import java.util.Properties

import config.ConfigManager
import org.apache.spark.SparkConf

object JdbcUtil {

  def getJdbcParams: Properties ={
    val prop = new Properties()
    prop.put("user", ConfigManager.getProp("jdbc.user"))
    prop.put("driver", ConfigManager.getProp("jdbc.driver"))
    prop.put("password", ConfigManager.getProp("jdbc.password"))
    prop.put("url", ConfigManager.getProp("jdbc.url"))
    prop.put("size", ConfigManager.getProp("jdbc.datasource.size"))
    prop
  }

}
