package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo09Union {
  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("union")

    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9))

    val rdd2: RDD[Int] = sc.parallelize(List(11, 12, 13, 14, 15, 16, 17, 18, 19))

    /**
     * union: 将两个rdd合并一个rdd
     * 不会产生shuffle，新的rdd的分区数等于前面两个rdd分区数的和
     *
     */

    val unionRDD: RDD[Int] = rdd1.union(rdd2)
    unionRDD.foreach(println)
  }

}
