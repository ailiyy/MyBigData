package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo07GroupBy {
  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("groupby")

    val sc = new SparkContext(conf)

    // 读取文件
    val students: RDD[String] = sc.textFile("data/students.txt")

    /**
     * 统计每个班级的平均年龄
     *
     */


    /**
     * groupby：指定一个列进行分组，返回一个新的Rdd，rdd的key是分组的key，rdd的value是同一个key所有的值
     * 会产生shuffle，分区默认等于一个rdd的分区，也可以手动指定
     */

    val groupByRDD: RDD[(String, Iterable[String])] = students.groupBy(line => line.split(",")(4))

    groupByRDD.foreach(print)
    val avgAgeRDD: RDD[(String, Double)] = groupByRDD.map(kv => {
      val clazz: String = kv._1

      // 同一个班级所有的学生
      val stus: List[String] = kv._2.toList

      // 取出年龄
      val ages: List[Int] = stus.map(line => line.split(",")(2).toInt)

      val avgAge: Double = ages.sum.toDouble / ages.size

      (clazz, avgAge)
    })
    avgAgeRDD.foreach(println)
  }

}
