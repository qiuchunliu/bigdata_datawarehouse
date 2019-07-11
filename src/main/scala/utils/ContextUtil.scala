package utils

import constant.Constant
import org.apache.spark.sql.SparkSession

object ContextUtil {

  def getHiveContext: SparkSession ={
    val ssc: SparkSession = SparkSession.builder().master(Constant.SPARK_MASTER).appName(Constant.SAPRK_APPNAME).enableHiveSupport().getOrCreate()
    ssc
  }

}
