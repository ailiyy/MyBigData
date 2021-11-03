package com.shujia.flink.core

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

import java.util.Properties

object Demo02FlinkOnKafka {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 1、取出Kafka中的数据

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "master:9092,node1:9092,node2:9092")
    properties.setProperty("group.id","test")

    // 创建消费者
    val flinkKafkaConsumer = new FlinkKafkaConsumer[String](
      "flink",
      new SimpleStringSchema(),
      properties
    )

    val linesDS: DataStream[String] = env.addSource(flinkKafkaConsumer)

    // 统计单词的数据
    val countDS: DataStream[String] = linesDS.flatMap(_.split(","))
      .map((_, 1))
      .keyBy(_._1)
      .map(kv => kv._1 + "\t" + kv._2)

    // 将数据保存到kafka中

    // 生产者
    val flinkKafkaProducer = new FlinkKafkaProducer[String](
      "master:9092,node1:9092,node2:9092", // broker 列表
      "wordcount", // 目标 topic
      new SimpleStringSchema()) // 序列化 schema

      countDS.addSink(flinkKafkaProducer)

      env.execute()

  }

}
