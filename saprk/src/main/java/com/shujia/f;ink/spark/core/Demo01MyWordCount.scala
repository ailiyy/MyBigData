package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo01MyWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.setAppName("wc")

    conf.setMaster("local")

    val sc = new SparkContext(conf)

    // 读取文件
    val linesRDD: RDD[String] = sc.textFile("data/words.txt")

    //    linesRDD.foreach(println)
    val wordsRDD: RDD[String] = linesRDD.flatMap(line => line.split(","))

    // 打印
    // wordsRDD.foreach(println)

    // 转换成kv格式
    val kvRDD: RDD[(String, Int)] = wordsRDD.map(word => (word, 1))
    kvRDD.foreach(println)

    // 通过reduce方法聚合,统计单词数量
    val countRDD: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) => x + y)
    countRDD.foreach(println)

    // 整理数据
    val resultRDD: RDD[String] = countRDD.map(kv => {
      val word: String = kv._1
      val count: Int = kv._2

      word + "," + count // 定义数据格式
    })

    // 保存数据
    resultRDD.saveAsTextFile("Data/wc")

//    while (true) {
//
//    }



  }

}
