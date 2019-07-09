package com.sun.bigdata.flink.tomeout

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 China All Rights Reserved 
  *
  * Project: flink-project
  * Package: com.sun.bigdata.flink.tomeout
  * Version: 1.0
  *
  * Created by SunYang on 2019/7/9 19:38
  */
// 输入订单事件数据流
case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

// 输出订单处理结果数据流
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeOut {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读入订单数据
    val orderEventStream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "other", 1558430845),
      OrderEvent(2, "pay", 1558430850),
      OrderEvent(1, "pay", 1558431920)
    )).assignAscendingTimestamps(_.eventTime * 1000)

    // 定义一个带时间限制的pattern , 选出创建订单，之后又支付的事件流
    val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .next("follow")
      .where("pay" == _.eventType)
      .within(Time.minutes(15))

    // 定义一个输出流标签，用来标明侧输出流
    val orderTimeOutTag: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeOut")
    // 将pattern作用到input stream上，得到一个pattern stream
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream.keyBy(_.orderId), orderPayPattern)

    /**
      * 重难点部分
      */
    import scala.collection.Map
    val complete: DataStream[OrderResult] = patternStream.select(orderTimeOutTag)(
      // pattern timeout function
      (orderPayEvents: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val timeoutOrderId = orderPayEvents.getOrElse("begin", null).iterator.next().orderId
        OrderResult(timeoutOrderId, "order time out")
      }
    )(
      //  pattern select function
      (orderPayEvents: Map[String, Iterable[OrderEvent]]) => {
        val payedOrderId = orderPayEvents.getOrElse("follow", null).iterator.next().orderId
        OrderResult(payedOrderId, "order payed successfully")
      }
    )


    // 已正常支付的
    complete.print("pay")
    complete.getSideOutput(orderTimeOutTag).print("timeout")

    env.execute("OrderTimeOut")
  }
}
