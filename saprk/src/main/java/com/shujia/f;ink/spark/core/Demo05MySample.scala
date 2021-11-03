package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo05MySample {
  // 构建环境
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local") // 设置本地模式
      .setAppName("mysample")

    val sc = new SparkContext(conf)

    // 读取文件
    val score: RDD[String] = sc.textFile("data/score.txt")

    // 抽样
    val sampleRDD: RDD[String] = score.sample(false, 0.1)
    sampleRDD.foreach(println)
  }

}
