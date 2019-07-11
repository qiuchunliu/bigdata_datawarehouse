package config

import java.io.InputStream
import java.util.Properties


/**
  * 通过类加载器来加载配置文件
  */
object ConfigManager {

  val prop = new Properties()

  try{  // 对异常进行捕获
    val basic: InputStream = ConfigManager.getClass.getClassLoader.getResourceAsStream("basic.properties")
    val dm_basic: InputStream = ConfigManager.getClass.getClassLoader.getResourceAsStream("dm_basic.properties")
    prop.load(basic)
    prop.load(dm_basic)
  }catch {
    case e: Exception =>
      e.printStackTrace() // 异常处理，直接输出到控制台
  }

  def getProp(key: String): String ={
    prop.getProperty(key)
  }

}
