package com.shujia.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Demo16Submit {
  def main(args: Array[String]): Unit = {
    /**
     * 1、创建Spark上下文对象
     */

    // 环境配置对象
    val conf = new SparkConf()

    // 指定任务名
    conf.setAppName("submit")

    // 创建spark环境，是写spark代码的入口
    val sc = new SparkContext(conf)

    /**
     * 统计单词数量
     *
     *
     * RDD:弹性的分布式数据集（可以当成一个Scala的集合来使用）
     */
    sc.textFile("")
  }

}
