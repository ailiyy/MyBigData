package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.Ordering.Double

object Demo08ReduceByKey {
  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf()
      .setAppName("reducebykey")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val studentusRDD: RDD[String] = sc.textFile("data/students.txt")

    /**
     * 统计每个班级的平均年龄
     *
     */

    // 1、取出班级和年龄
    val kvRDD: RDD[(String, (Int, Int))] = studentusRDD.map(studentusRDD => {
      val split: Array[String] = studentusRDD.split(",")
      val clazz: String = split(4)
      val age: Int = split(2).toInt

      // 班级作为key 年龄作为value，后面加上1，是为了计算总人数
      (clazz, (age, 1))
    })

    /**
     * reduceByKey:对同一个key的value进行聚合计算
     * 会产生shuffle，分区默认等于前一个rdd，可以手动设置分区数据
     *
     * reduceByKey会在shuffle之前进行预聚合
     * 尽量使用reduceByKey代替groupByKey
     */

    kvRDD.foreach(println)

    val reduceRDD: RDD[(String, (Int, Int))] = kvRDD.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    val avgAgeRDD: RDD[(String, Double)] = reduceRDD.map {
      case (clazz: String, (sunAge: Int, sumNum: Int)) =>
        // 计算平均年龄
        val avgAge: Double = sunAge / sumNum

        (clazz, avgAge)
    }
    avgAgeRDD.foreach(println)

  }

}
