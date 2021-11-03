package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo02MyMap {
  def main(args: Array[String]): Unit = {
    /**
     * 构建spark环境
     */
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("mymap")
    val sc = new SparkContext(conf)

    // 构建rdd，基于集合构建rdd
    val rdd1: RDD[Int] = sc.parallelize(List(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1))

    /**
     * map：对rdd中的数据进行处理，新的rdd数据量不变，分区数不变
     */
    val rdd2: RDD[Int] = rdd1.map(i => i + 1)
    rdd2.foreach(println)

    // 奇数加一偶数乘二
    val rdd3: RDD[Int] = rdd1.map(i => {
      if (i % 2 == 1) {
        i + 1
      } else {
        i * 2
      }

    })
    rdd3.foreach(println)

  }

}
