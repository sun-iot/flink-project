package com.sun.bigdata.wc

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  *
  * @author SunYang
  * @date 2019/7/3
  */
object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    // 读取Kafka的数据，先设置Kafka的配置信息
    val properties = new Properties()
    // 设置服务器的集群
    properties.setProperty("bootstrap.servers" , "192.168.1.104:9092,192.168.1.105:9092,192.168.1.106:9092")
    // 设置我们扥GtoupId
    properties.setProperty("group-id","flink-consumer-group")
    // 设置我们的KV的序列化
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer" , "org.apache.kafka.common.serialization.StringDeserializer")
    // 设置自动 offest的提交更新
    properties.setProperty("auto.offset.reset","latest")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 获取Kafka的数据
    // FlinkKafkaConsumer011(topic: String, valueDeserializer: DeserializationSchema[T], props: Properties)
    val kafkaDS: DataStreamSource[String] = env.addSource(new FlinkKafkaConsumer011[String]("topic_flink" , new SimpleStringSchema() , properties))
    // 对得到的数据进行解析
    kafkaDS.print("kafkaStream").setParallelism(2)

    env.execute("Kafka")

  }
}
