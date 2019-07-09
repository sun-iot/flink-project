package com.sun.bigdata

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 China All Rights Reserved 
  *
  * Project: flink-project
  * Package: com.sun.bigdata
  * Version: 1.0
  *
  * Created by SunYang on 2019/7/8 19:09
  */
// 定义一个数据样例类，传感器id，采集时间戳，传感器温度
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object Sensor {
  def main(args: Array[String]): Unit = {
    // 设置流的操作，进行动态 WordCount的操作
    // 批处理的环境创建
    // val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = tool.get("host")
    val port: Int = tool.getInt("port")

    // 流处理环境创建
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 我们可以通过 socket 进行数据的交互，即通过 socket 发送数据
    // 得到一个socket流
    // socketTextStream(hostname: String, port: Int, delimiter: Char = '\n', maxRetry: Long = 0)
    // keyBy : 表示按照第几位进行聚合
    // sum(Int) 表示按照第几位进行 相加操作
    val socketDS: DataStream[String] = env.socketTextStream(host, port)
    val windowDS: DataStream[SensorReading] = socketDS.map {
      records => {
        val dataArray: Array[String] = records.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }
    }.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1000)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
    })
    windowDS.map{
      data=>{
        (data.id , data.temperature)
      }
    }.keyBy(_._1).timeWindow(Time.seconds(15) , Time.seconds(5)).reduce{
      (d1,d2)=>{
        (d1._1 , d1._2.min(d2._2))
      }
    }.print("sensor").setParallelism(1)

    env.execute("Sensor")
  }
}
