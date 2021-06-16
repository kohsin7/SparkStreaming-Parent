package com.kohsin.spark.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Author: kohsin
 * Desc: 该 Controller 用于接收模拟生成的日志
 */
//标识为 controller 组件，交给 Sprint 容器管理，并接收处理请求 如果返回 String，会当作网页进行跳转
//@Controller
// @RestController = @Controller + @ResponseBody 会将返回结果转换为 json 进行响应

@RestController
@Slf4j
public class LoggerController {
    //Spring提供的对Kafka的支持
    @Autowired  //  将KafkaTemplate注入到Controller中
            KafkaTemplate kafkaTemplate;

    //提供一个方法，处理模拟器生成的数据
    //@RequestMapping("/applog")  把applog请求，交给方法进行处理
    //@RequestBody   表示从请求体中获取数据
    @RequestMapping("/sparkapplog")
    //在模拟数据生成的代码中，我们将数据封装为 json，通过post传递给该Controller处理，所以我们通过@RequestBody 接收
    public String applog(@RequestBody String mockLog) {
        //System.out.println(mockLog);
        //落盘
        log.info(mockLog);
        //根据日志的类型，发送到kafka的不同主题中去
        //将接收到的字符串数据转换为json对象
        JSONObject jsonObject = JSON.parseObject(mockLog);
        JSONObject startJson = jsonObject.getJSONObject("start");
        if (startJson != null) {
            //启动日志
            kafkaTemplate.send("spark_start_logger", mockLog);
        } else {
            //事件日志
            kafkaTemplate.send("spark_event_logger", mockLog);
        }
        return "success";
    }
}
