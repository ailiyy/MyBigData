package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo01WordCount {
  def main(args: Array[String]): Unit = {
    /**
     * 1、创建spark上下文对象
     *
     */

    // 环境配置对象
    val conf = new SparkConf()

    // 指定任务名
    conf.setAppName("WordCount")

    // 执行运行方式
    conf.setMaster("local")

    // 创建spark环境，是写spark代码的入口
    val sc = new SparkContext(conf)

    /**
     * 统计单词数量
     *
     *
     * RDD:弹性的分布式数据集（可以当作一个scala的集合使用）
     */

    // 1、读取文件
    val linesRDD:RDD[String] = sc.textFile("data/words.txt")


    // 将每一行中的单词拆分出来
    val wordsRDD:RDD[String] = linesRDD.flatMap(line => line.split(","))

    // 将数据转换成kv格式
    val kvRDD: RDD[(String,Int)] = wordsRDD.map(word => (word, 1))

    /**
     * reduceByKey:对同一个key的value进行聚合计算
     *
     */

    // 统计单词数量
    val countRDD:RDD[(String,Int)] = kvRDD.reduceByKey((x,y) => x + y)

    // 整理数据
    val resultRDD:RDD[String] = countRDD.map(kv => {
      val word:String = kv._1
      val count: Int = kv._2

      word + "," + count
    })

    // 保存数据
    resultRDD.saveAsTextFile("Data/wc")
  }

}
