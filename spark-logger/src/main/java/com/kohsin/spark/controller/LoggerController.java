package com.kohsin.spark.controller;

import lombok.extern.slf4j.Slf4j;
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
    //通过 requestMapping 匹配请求并交给方法处理
    @RequestMapping("/applog")
    //在模拟数据生成的代码中，我们将数据封装为 json，通过post传递给该Controller处理，所以我们通过@RequestBody 接收
    public String applog(@RequestBody String jsonLog) {
        log.info(jsonLog);
        System.out.println(jsonLog);
        return jsonLog;
    }
}
