package com.shujia.flink.core

import akka.actor.FSM.Event

import java.time.Duration
import org.apache.flink.api.common.eventtime.{BoundedOutOfOrdernessWatermarks, SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.events.Event
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Demo03EventTime {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置并行度
    env.setParallelism(1)

    // 指定时间模式为时间时间
    // 默认是处理时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val lineDS: DataStream[String] = env.socketTextStream("master", 8888)

    val events: DataStream[Event] = lineDS.map(line => Event(line.split(",")(0), line.split(",")(1).toLong))


    /**
     *
     * 统计最近5秒id的数量
     *
     */
    events
    // 执行事件时间字段，水位线默认等于时间戳最大的数据的时间
      .assignTimestampsAndWatermarks(
        // 指定数据最大的延迟时间
        new BoundedOutOfOrdernessTimestampExtractor[Event](Time.seconds(5)) {
          override def extractTimestamp(element: Event): Long = {
            // 返回事件时间字段
            element.ts
          }
        })
      .map(event => (event.id, 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5)) // 5秒一个窗口
      .sum(1)
      .print()

    env.execute()





  }

}
