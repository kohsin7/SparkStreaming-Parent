package spark.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.realtime.utils.MyKafkaUtil

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

    val jsonDStream: DStream[JSONObject] = kafkaDSream.map {
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
    jsonDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
