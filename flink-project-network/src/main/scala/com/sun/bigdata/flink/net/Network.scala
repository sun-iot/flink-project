package com.sun.bigdata.flink.net

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 需求1：从web服务器的日志中，统计实时的访问流量
  * 需求2：统计每分钟的ip访问量，取出访问量最大的5个地址，每5秒更新一次
  *
  * 思路1：将 apache 服务器日志中的时间，转换为时间戳，作为 Event Time
  * 思路2：构建滑动窗口，窗口长度为1分钟，滑动距离为5秒
  *
  * @author SunYang
  * @date 2019/7/8
  */
// Web log数据流
case class ApacheLogEvent(ip: String, userName: String, eventTime: Long, method: String, url: String)

// 中间统计数量的数据类型
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object Network {
  def main(args: Array[String]): Unit = {
    // 得到环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)
    // 设置时间类型为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 获取数据
    val logDS: DataStream[String] = env.readTextFile("F:\\HadoopProject\\flink-project\\flink-project-network\\src\\main\\resources\\apache.log")
    // 开始对日志文件进行分析处理                                   F:\HadoopProject\flink-project\flink-project-network\src\main\resources\apache.log
    // 1、转化为Web log数据流类型
    val logMapDS: DataStream[ApacheLogEvent] = logDS.map(
      // 对得到的每一行数据进行分析出来
      records =>{
        val strArrays: Array[String] = records.split(" ")
        // 将log里面的时间转化成时间戳
        // 先定义转化的类型，并做转换 parse
        val time: Long = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(strArrays(3)).getTime
        ApacheLogEvent(strArrays(0), strArrays(2), time, strArrays(5), strArrays(6))
      }
    )
    // 为数据流打上时间戳
    val timestampDS: DataStream[ApacheLogEvent] = logMapDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(60)) {
      // 已经是毫秒级别的了
      override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
    })
    // 开始进行分组
    timestampDS.keyBy(_.url)
      // 进行开窗 ， 窗口大小1分钟，步长5秒 , 开一个滑动窗口
      .timeWindow(Time.minutes(1), Time.seconds(5))
      .aggregate(new CountLogAgg(), new WindowLogFunction())
      // 根据时间串口来进行分区
      .keyBy(_.windowEnd)
      .process(new NetTopN(5))
      .print("net")
    env.execute("NetWork")
  }
}

// 预聚合
class CountLogAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class WindowLogFunction() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class NetTopN(i: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  // 暂存区 , 进行懒加载
  lazy val listState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("utl-state", classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    listState.add(value)
    // 注册定时器，当定时器触发时，应该收集到了数据
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 从state中获取所有的数据
    val urlListe : ListBuffer[UrlViewCount] = ListBuffer()
    val iterState: util.Iterator[UrlViewCount] = listState.get().iterator()

    while(iterState.hasNext){
      urlListe+=iterState.next()
    }
    listState.clear()

    // 按照点击量大小进行排序
    val sortListState: ListBuffer[UrlViewCount] = urlListe.sortWith(_.count > _.count).take(i)
    // 把结果格式化为 String 输出
    val result = new StringBuilder

    result.append("\n")
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(timestamp - 100)).append("\n")
    for (index <- sortListState.indices){
      val viewCount: UrlViewCount = sortListState(index)
      // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
      result.append("No").append(index+1).append(":")
        .append("  URL=").append(viewCount.url).append(",")
        .append("  流量=").append(viewCount.count).append("\n")

    }
    result.append("====================================\n")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}


