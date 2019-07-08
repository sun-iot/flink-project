package com.sun.bigdata.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  *
  * @author SunYang
  * @date 2019/7/3
  */
object StreamingWordCount {
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
    val resultDS: DataStream[(String, Int)] = env.socketTextStream(host , port ).flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)
    // print() , 操作
    // setParallelism() 设置并发
    resultDS.print("stream1").setParallelism(2)
    // 必须要有，进行executor操作
    env.execute()
  }
}
