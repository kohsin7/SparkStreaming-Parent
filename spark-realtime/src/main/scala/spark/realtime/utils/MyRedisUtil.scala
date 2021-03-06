package spark.realtime.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Author：kohsin
 * Date：
 * Desc：获取 Jedis 客户端工具类
 */
object MyRedisUtil {
  //定义一个连接池对象
  private var jedisPool: JedisPool = null

  def build(): Unit = {
    val config = MyPropertiesUtil.load("config.properties")
    val host = config.getProperty("redis.host")
    val port = config.getProperty("redis.port")

    val jedisPoolConfig = new JedisPoolConfig()
    jedisPoolConfig.setMaxTotal(100) //最大连接数
    jedisPoolConfig.setMaxIdle(20) //最大空闲
    jedisPoolConfig.setMinIdle(20) //最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(5000) //忙碌时等待时长 毫秒
    jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

    jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
  }

  //获取 Jedis 客户端
  def getJedisClient(): Jedis = {
    if (jedisPool == null) {
      build()
    }
    jedisPool.getResource
  }
}
