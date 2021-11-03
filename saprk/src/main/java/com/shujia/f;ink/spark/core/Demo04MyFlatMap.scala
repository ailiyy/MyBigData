package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo04MyFlatMap {
  def main(args: Array[String]): Unit = {
    // 构建spark环境
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("myflatmap")

    val sc = new SparkContext(conf)

    // 通过集合构建rdd
    val rdd1: RDD[String] = sc.parallelize(List("几个字，系噶子，姚龙", "里工行，大撒大撒，大大撒旦", "lalal，大叔的啊，哈哈哈"))

    rdd1.foreach(println)

    // 通过“，”切分
    val rdd2: RDD[String] = rdd1.flatMap(line => line.split("，"))
    rdd2.foreach(println)

    // 过滤
    val rdd3: RDD[String] = rdd1.flatMap(line => {
      val array: Array[String] = line.split("，") // 将数据分割放入array数组当中
      val filter: Array[String] = array.filter(word => word != "姚龙") // 通过方法过滤掉关键字
      filter // 最后返回过滤之后的数组
    })

    rdd3.foreach(println)
  }

}
