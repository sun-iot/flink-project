package com.sun.bigdata.flink.login

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 China All Rights Reserved 
  *
  * Project: flink-project
  * Package: com.sun.bigdata.flink.login
  * Version: 1.0
  *
  * Created by SunYang on 2019/7/9 16:00
  */
object LoginCEP {
  def main(args: Array[String]): Unit = {
    // 得到环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)
    // 设置我们的测试数据
    val loginDS: DataStream[LoginEvent] = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    )).assignAscendingTimestamps(_.eventTime * 1000)
    // 定义pattern，对事件流进行模式匹配
    val loginPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))
    // 在输入流的基础上应用pattern，得到匹配的pattern stream
    val patternDS: PatternStream[LoginEvent] = CEP.pattern(loginDS.keyBy(_.userId) , loginPattern)

    patternDS.select(new SelectFunctionCustomer()).print("CEP")
    env.execute("CEP")
  }
}

class SelectFunctionCustomer() extends PatternSelectFunction[LoginEvent , Warning]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    val firstFail: LoginEvent = map.getOrDefault("begin" , null).iterator().next()
    val lastFail: LoginEvent = map.getOrDefault("next" , null).iterator().next()
    Warning(firstFail.userId , firstFail.eventTime , lastFail.eventTime , "login failed ... in 2 seconds" )
  }
}


