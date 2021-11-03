package com.shujia.flink.core

import org.apache.flink.streaming.api.scala._

object Demo01WordCount  {
  def main(args: Array[String]): Unit = {


    /**
     *  创建flink环境
     *
     */

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置并行的
    env.setParallelism(2)

    /**
     *读取socket 构建DataStream
     * nc -lk 8888
     *
     */
    val linesDS: DataStream[String] = env.socketTextStream("master", 8888)

    // 将数据拆分开
    val wordDS: DataStream[String] = linesDS.flatMap(_.split(","))

    // 将数据转换成kv格式
    val kvDS: DataStream[(String, Int)] = wordDS.map((_, 1))

    // 将同一个key发送到同一个task中
    val keyedDS: KeyedStream[(String, Int), String] = kvDS.keyBy(_._1)

    // 统计value的和
    val countDS: DataStream[(String, Int)] = keyedDS.sum(1)

    // 打印结果
    countDS.print()

    // 启动flink任务
    env.execute()
  }

}
