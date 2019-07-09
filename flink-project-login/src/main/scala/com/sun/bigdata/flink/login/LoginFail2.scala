package com.sun.bigdata.flink.login

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 China All Rights Reserved 
  *
  * Project: flink-project
  * Package: com.sun.bigdata.flink.login
  * Version: 2.0
  *
  * Created by SunYang on 2019/7/9 15:35
  */
// 用户登录事件
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

// 错误信息
case class Warning(userId: Long, firstFailedTime: Long, lastFailedTime: Long, waringMessage: String)

/**
  * 针对状态编程的升级优化
  */
object LoginFail2 {
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
      .process(new LoginFunctionMatch2())
      .print()

    env.execute("Login Failed")
  }
}

// K - I - O
// 状态编程的升级，不注册定时器
class LoginFunctionMatch2() extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-site", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    // 首先，先把类型删选出来 , 如果success直接清空，否则在做处理
    if (value.eventType == "fail") {
      // 如果有登陆失败的数据，判断是否在 2s 内
      val iterLoginState: util.Iterator[LoginEvent] = loginState.get().iterator()
      if (iterLoginState.hasNext) {
        val firstFail = iterLoginState.next()
        // 如果两次登录时间小于 2s
        if (value.eventTime < firstFail.eventTime + 2) {
          out.collect(Warning(value.userId, firstFail.eventTime, value.eventTime, "login failed ... in 2 seconds"))
        }
        // 将当前登录失败的记录更新写入到state里面去，下次继续使用
        val failList = new util.ArrayList[LoginEvent]()
        failList.add(value)
        loginState.update(failList)
      }else{
        loginState.add(value)
      }
    }else{
      loginState.clear()
    }
  }
}
