package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo12Sort {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf()
      .setAppName("sort")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val agrRDD: RDD[(String, Int)] = sc.parallelize(List(("001", 23), ("002", 24), ("003", 25), ("004", 26)))

    /**
     * sortBy:会指定一个排序的列进行排序，默认是升序
     * 会产生shuffle，分区数默认等于前一个rdd，也可以手动指定
     *
     */
    val sortByRDD: RDD[(String, Int)] = agrRDD.sortBy(kv => kv._2, ascending = true)

    sortByRDD.foreach(println)

    /**
     * sortByKey:通过key进行排序
     *
     */
    val sortByKeyRDD: RDD[(String, Int)] = agrRDD.sortByKey()

    sortByKeyRDD.foreach(println)
  }

}
