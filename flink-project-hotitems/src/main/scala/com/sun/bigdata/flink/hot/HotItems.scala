package com.sun.bigdata.flink.hot

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  *
  * @author SunYang
  * @date 2019/7
  */
object HotItems {
  def main(args: Array[String]): Unit = {
    // 创建Flink运行的环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)
    // 指定Time类型为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 开始梳理数据
    val dataStream: DataStream[String] = env.readTextFile("F:\\HadoopProject\\flink-project\\flink-project-hotitems\\src\\main\\resources\\UserBehavior.csv")
    // 开始进行转换
    val userBehaviorDS: DataStream[UserBeahvior] = dataStream.map {
      // 对得到的每一行数据做处理分析
      line => {
        val strs: Array[String] = line.split(",")
        // 将达到的数据流进行处理
        UserBeahvior(strs(0).trim.toLong, strs(1).trim.toLong, strs(2).trim.toInt, strs(3).trim, strs(4).trim.toLong)
      }
    }
    // 给数据流打上时间戳
    val timeTampDS: DataStream[UserBeahvior] = userBehaviorDS.assignAscendingTimestamps(_.timeTamp * 1000)
    // 过滤出行为是 “pv”的数据
    val filterDS: DataStream[UserBeahvior] = timeTampDS.filter(_.behavior == "pv")
    // 进行keyBy,分区操作
    val keyByDS: KeyedStream[UserBeahvior, Long] = filterDS.keyBy(_.itemId)
    //
    // keyByDS.window(SlidingEventTimeWindows.of(Time.minutes(60) , Time.milliseconds(5)))
    // 开时间窗口,滑动窗口,60分钟的统计，即窗口大小 ， 5分钟的滑动距离，
    val windowDS: WindowedStream[UserBeahvior, Long, TimeWindow] = keyByDS.timeWindow(Time.minutes(60), Time.minutes(5))

    // 聚合操作 ， 有自己操作
    val aggregateDS: DataStream[ItemViewCount] = windowDS.aggregate(new CountAgg(), new WindowResultFunction())
    // 按照 windowEnd 分组
    val windowEndKeyByDS: KeyedStream[ItemViewCount, Long] = aggregateDS.keyBy(_.windowEnd)
    val processDS: DataStream[String] = windowEndKeyByDS.process(new ProcessTopNHotItems(5))
    processDS.print("items")
    // 调用Executor
    env.execute("Hot")
  }
}

// 数据的样例类
case class UserBeahvior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timeTamp: Long)

// 中间聚合的样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

// 进行预聚合操作，来一个 +1
class CountAgg() extends AggregateFunction[UserBeahvior, Long, Long] {
  override def createAccumulator(): Long = 0L
  override def add(in: UserBeahvior, acc: Long): Long = acc + 1
  override def getResult(acc: Long): Long = acc
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

// 窗口关闭时的操作，需要包装秤自定义的操作
class WindowResultFunction() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key
    val windowEnd: Long = window.getEnd
    val count: Long = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

class ProcessTopNHotItems(i: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  // 中间存储
  private var itemState: ListState[ItemViewCount] = _

  // 初始化操作
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 先拿到当前状态
    val itemStateDesc = new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount])
    // 获取当前运行环境中的 ListState , 用来恢复 itemState
    itemState = getRuntimeContext.getListState(itemStateDesc)
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 每条数据都暂存在ListState中
    itemState.add(value)
    // 开始处理，注册一个定时器，当定时器延迟触发，触发时默认窗口该收集的数据都到了
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
  }

  // 核心处理流程，在触发定时器时操作 ， 可以认为watermark已经涨到了窗口关闭时间（当时数据生成时间，即数据都到了）
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    var items: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itemState.get()) {
      items+=item
    }
    // 清空
    itemState.clear()
    // 可以进行转换，排序，等操作
    // 按照count值进行排序，选择前 N 个
    val sortedItem: ListBuffer[ItemViewCount] = items.sortBy(_.count)(Ordering.Long.reverse).take(i)
    val builder = new StringBuilder
    // 做清空操作
    //builder.delete(0, builder.length)

    builder.append("====================================\n")
    builder.append("时间: ").append(new Timestamp(timestamp - 100)).append("\n")
    // indices 获取索引值
    for (i <- 0 until sortedItem.length) {
      val current: ItemViewCount = sortedItem(i)
      builder.append("NO.").append(i + 1).append(":")
        .append("商品ID：").append(current.itemId).append(",")
        .append("浏览量：").append(current.count).append(",").append("\n")

    }
    builder.append("====================================\n")
    // 睡眠一下
    Thread.sleep(100)
    out.collect(builder.toString())
  }

}