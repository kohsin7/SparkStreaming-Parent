package spark.realtime.utils

import java.io.{FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * properties 读取配置文件
 */
object MyPropertiesUtil {
  def load(propertiesName: String): Properties = {
    val prop = new Properties()
    // 加载执行的配置文件
    // 编译之后放在class下面了
    // 用类加载器来读取
    prop.load(new InputStreamReader(
      Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
      StandardCharsets.UTF_8))
    prop
  }
}
