package spark.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import spark.realtime.bean.DauInfo
import spark.realtime.utils.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}

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

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null;
    // 从 Redis 中获取 Kafka 分区偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    if (offsetMap != null && offsetMap.size > 0) {
      // 如果有，从 offset 开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      // 如果没有，从最新的 开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // 获取当前采集周期从 kafka 中消费的数据的起始偏移量以及结束的偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        // 因为 recordDStream 底层封装的是 kafakRdd，混入了 HasOffsetRanges 特质，这个特质中提供了可以获取偏移量范围的方法
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    // 获取当前采集周期从 Kafka 中消费的数据的起始偏移量以及结束偏移量
    //    val jsonDStream: DStream[String] = kafkaDSream.map(_.value())
    //    jsonDStream.print()

    val jsonObjectDStream: DStream[JSONObject] = offsetDStream.map {
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
    val filteredDStream: DStream[JSONObject] = jsonObjectDStream.mapPartitions(
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
    //filteredDStream.count().print()

    // todo transform 和 foreachRDD在 Driver 中执行
    // todo RDD相关的算子的代码在 executor 中执行，算子之外的代码在 driver 中执行
    //将数据批量保存到 ES 中
    filteredDStream.foreachRDD(
      rdd => {
        // todo 根数据库打交道，最好用分区算子来做
        // mapPartition 是做转换，最终需要返回一个可迭代的集合
        // foreachPartition 可以直接保存，无返回值
        rdd.foreachPartition(
          jsonObjItr => {
            val dauInfList: List[DauInfo] = jsonObjItr.map(
              jsonObj => {
                //每次处理的是一个 json 对象 将 json 对象封装为样例类
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                DauInfo(
                  commonJsonObj.getString("mid"),
                  commonJsonObj.getString("uid"),
                  commonJsonObj.getString("ar"),
                  commonJsonObj.getString("ch"),
                  commonJsonObj.getString("vc"),
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00", //分钟我们前面没有转换，默认 00
                  jsonObj.getLongValue("ts")
                )
              }
            ).toList
            //对分区的数据进行批量处理
            //获取当前日志字符串
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauInfList, "spark_dau_info_" + dt)
          }
        )
      }
    )


    ssc.start()
    ssc.awaitTermination()
  }
}
