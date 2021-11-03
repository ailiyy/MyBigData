package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo03MyFilter {
  def main(args: Array[String]): Unit = {
    // 构建spark环境
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("myfilter")
    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.parallelize(List(11, 23, 54, 65, 55, 11, 22, 33, 54, 54, 54, 54, 51, 5, 151, 574, 87, 8))
    val rdd2: RDD[Int] = rdd1.filter(i => i % 2 == 0)
    rdd2.foreach(println)

  }

}
