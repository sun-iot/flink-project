package com.sun.bigdata.flink.login

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Copyright (c) 2018-2028 China All Rights Reserved 
  *
  * Project: flink-project
  * Package: com.sun.bigdata.flink.login
  * Version: 1.0
  *
  * Created by SunYang on 2019/7/9 14:27
  */
// 用户登录事件
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

// 错误信息
case class Warning(userId: Long, firstFailedTime: Long, lastFailedTime: Long, waringMessage: String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    // 得到环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)
    // 设置我们的测试数据
    val loginEventStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
    // 时间戳已经排列好了，就直接用升序的就好了
    loginEventStream.assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.userId)
      .process(new LoginFunctionMatch())
      .print()

    env.execute("Login Failed")
  }
}

// K - I - O
class LoginFunctionMatch() extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-site" , classOf[LoginEvent]))
  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    if (value.eventType == "fail"){
      loginState.add(value)
      ctx.timerService().registerEventTimeTimer((value.eventTime+2) * 1000)
    }else{
      loginState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    val buffer: ListBuffer[LoginEvent] = ListBuffer()
    // 添加到buffer
    val iterLoginState: util.Iterator[LoginEvent] = loginState.get().iterator()
    while(iterLoginState.hasNext){
      buffer += iterLoginState.next()
    }
    loginState.clear()

    // 如果state的长度大于 1 ， 说明有两个以上的登录失败事件，输出报警信息
    if (buffer.length > 1){
      out.collect(Warning(buffer.head.userId , buffer.head.eventTime , buffer.last.eventTime , "login failed ... in 2 seconds for " + buffer.length + "times"))
    }
  }
}
