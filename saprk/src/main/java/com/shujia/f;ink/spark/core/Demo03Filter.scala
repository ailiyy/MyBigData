package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo03Filter {
  def main(args: Array[String]): Unit = {
    /**
     * 创建spark环境
     */

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("filter")

    val sc = new SparkContext(conf)

    // 构建rdd，基于集合构建rdd
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9))

    /**
     * filter:过滤数据，新的rdd数据量会变少，但是分区数不变
     * 函数返回true保留数据，函数返回false过滤数据
     *
     */

    //取出所有奇数
    val rdd1: RDD[Int] = rdd.filter(i => i % 2 == 1) // 如果是是奇数那么保留，反之过滤

    rdd1.foreach(println)
  }

}
