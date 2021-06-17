package spark.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import spark.realtime.utils.{MyKafkaUtil, MyRedisUtil}

import scala.collection.mutable.ListBuffer

/**
 * Desc 日活业务
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    // sparkstreaming 读取kafka中数据
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    var topic: String = "spark_start_logger"
    var groupId: String = "spark_dau_logger"
    val kafkaDSream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    //    val jsonDStream: DStream[String] = kafkaDSream.map(_.value())
    //    jsonDStream.print()

    val jsonObjectDStream: DStream[JSONObject] = kafkaDSream.map {
      record => {
        val jsonString: String = record.value()
        val jsonObject: JSONObject = JSON.parseObject(jsonString)
        //从 json 对象中获取时间戳
        val ts: lang.Long = jsonObject.getLong("ts")
        //将时间戳，转换成日期和小时
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        val dateStrArr: Array[String] = dateStr.split(' ')
        val dt: String = dateStrArr(0)
        val hr: String = dateStrArr(1)
        jsonObject.put("dt", dt)
        jsonObject.put("hr", hr)
        jsonObject
      }
    }

    //对采集到的启动日志，进行一个去重操作
    //checkpoint 有小文件的问题

    //通过redis 对采集到的启动日志进行去重操作 方案1
    // todo 采集周期中的每条数据都需要获取一次 redis 客户端连接，连接过于频繁
    // redis 类型 set  key：dau：2021-06-17   value：mid   expire：3600*24
    /*val filteredDStream = jsonObjectDStream.filter(
      jsonObj => {
        //获取登陆日期
        val dt: String = jsonObj.getString("dt")
        //获取设备id
        val common: JSONObject = jsonObj.getJSONObject("common")
        val mid: String = common.getString("mid")
        //拼接 redis 中保存登陆信息的key
        var dauKey = "dau:" + dt
        val jedis = MyRedisUtil.getJedisClient()
        //设置 key 的失效时间
        if (jedis.ttl(dauKey) < 0) {
          jedis.expire(dauKey, 3600 * 24)
        }
        //从 redis 中判断当前设置是否已经登录过
        val isFirst: lang.Long = jedis.sadd(dauKey, mid)
        //记得关闭连接
        jedis.close()
        if (isFirst == 1L) {
          //说明第一次登陆
          true
        } else {
          // 说明已经登录过了
          false
        }
      }
    )*/

    // 方案2
    // todo 以分区为单位，在每个分区中获取一次 redis 连接
    // todo 可选算子 mappartition / foreachrdd（行动算子，触发行动操作，相当于作业要提交。并不是以分区为单位进行的，foreachpartition才是。）
    val filteredDStream:DStream[JSONObject] = jsonObjectDStream.mapPartitions(
      jsonObjIter => { //以分区为单位对数据进行处理
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        //定义一个集合，用于存放当前分区中第一次的登录的日志
        val filteredList = new ListBuffer[JSONObject]()
        //对分区的数据进行遍历
        for (jsonObj <- jsonObjIter) {
          //获取登陆日期
          val dt: String = jsonObj.getString("dt")
          //获取设备id
          val common: JSONObject = jsonObj.getJSONObject("common")
          val mid: String = common.getString("mid")
          //拼接 redis 中保存登陆信息的key
          var dauKey = "dau:" + dt
          val jedis = MyRedisUtil.getJedisClient()
          //设置 key 的失效时间
          if (jedis.ttl(dauKey) < 0) {
            jedis.expire(dauKey, 3600 * 24)
          }
          //从 redis 中判断当前设置是否已经登录过
          val isFirst: lang.Long = jedis.sadd(dauKey, mid)
          //记得关闭连接
          jedis.close()
          if (isFirst == 1L) {
            // 第一次登陆
            filteredList.append(jsonObj)
          }
        }
        jedis.close()

        filteredList.toIterator
      }
    )

    filteredDStream.count().print()

    ssc.start()
    ssc.awaitTermination()
  }
}
