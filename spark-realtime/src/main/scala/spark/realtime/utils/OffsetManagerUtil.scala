package spark.realtime.utils

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

object OffsetManagerUtil {

  // 从 redis 中获取 offset  type:hash    key:offset:topic:groupid    field:partition   value:偏移量
  def getOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    // 获取客户端连接
    val jedisClient = MyRedisUtil.getJedisClient()

    // 拼接操作 Redis 的 key     key:offset:topic:groupid
    var offsetKey = "offset:" + topic + ":" + groupId
    val offsetMap: util.Map[String, String] = jedisClient.hgetAll(offsetKey)

    // 关闭客户端
    jedisClient.close()

    // 将 java 的 map 转换为 scala 的 map
    import scala.collection.JavaConverters._

    val oMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partition, offset) => {
        //      Map[TopicPartition, Long]
        (new TopicPartition(topic, partition.toInt), offset.toLong)
      }
    }.toMap
    oMap
  }

  // 将 offset 信息保存到 Redis 中
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    var offsetKey = "offset:" + topic + ":" + groupId
    // 定义一个 java 的 map 集合，用于存放每个分区对应的偏移量
    val offsetMap = new util.HashMap[String, String]()
    val jedis = MyRedisUtil.getJedisClient()

    // 对 offsetRanges 遍历，将数据封装到 offsetMap 中
    for (offsetRange <- offsetRanges) {
      val partitionId: Int = offsetRange.partition
      val fromOffset: Long = offsetRange.fromOffset
      val untilOffset: Long = offsetRange.untilOffset
      offsetMap.put(partitionId.toString, untilOffset.toString)
      println("保存分区" + partitionId + ":" + fromOffset + "---->" + untilOffset)
    }

    jedis.hmset(offsetKey, offsetMap)
    jedis.close()
  }
}
