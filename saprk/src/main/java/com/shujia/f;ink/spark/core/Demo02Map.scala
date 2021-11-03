package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo02Map {
  def main(args: Array[String]): Unit = {
    /**
     * 创建spark环境
     *
     */

    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("map")


    val sc = new SparkContext(conf)

    // 构建rdd，基于集合构建rdd
    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9))

    /**
     * map:对rdd中的数据进行处理，返回新的rdd，新的rdd数据量不变，分区数不变
     *
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
