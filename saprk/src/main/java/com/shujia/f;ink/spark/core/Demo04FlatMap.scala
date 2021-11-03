package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo04FlatMap {
  def main(args: Array[String]): Unit = {
    /**
     * 创建spark环境
     *
     */

    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("flatmap")

    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(List("java,spark,hadoop", "java,spark,flink", "java.spark,hadoop"))

    /**
     * flatMap:将rdd中的一行数据转换成多行数据，数据量变多，分区数据不变
     * 函数的返回值可以是一个集合，数组，迭代器
     *
     */

    val rdd2: RDD[String] = rdd1.flatMap(line => line.split(","))

    rdd2.foreach(println)


    val rdd3: RDD[String] = rdd1.flatMap(line => {
      // 在flatmap函数当中可以写很复杂的代码逻辑
      val array: Array[String] = line.split(",")
      val filter: Array[String] = array.filter(word => word != "java")
      filter
    })

    rdd3.foreach(println)


  }

}
