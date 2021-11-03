package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo06GroupByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("group")

    val sc = new SparkContext(conf)

    // 读取文件
    val students: RDD[String] = sc.textFile("data/students.txt")

    /**
     * 统计每个班级的平均年龄
     *
     */


    // 1、取出班级和年龄

    val kvRDD: RDD[(String, Int)] = students.map(students => {
      val split: Array[String] = students.split(",")

      val clazz: String = split(4)
      val age: Int = split(2).toInt

      // 班级作为key 年龄作为value
      (clazz, age)
    })

    /**
     * groupBykey:将同一个key分到同一个组内，会产生shuffle，分区数默认等于前一个rdd的分区数
     *
     * numPartitions：手动指定shuffle之后rdd的分区数
     * 所有的shuffle类的算子都可以手动指定rdd的分区数据
     */

    // 2、按照班级进行分组
    val groupRDD: RDD[(String, Iterable[Int])] = kvRDD.groupByKey(2)
    val avgAgeRDD: RDD[(String, Double)] = groupRDD.map(kv => {
      // 班级
      val clazz: String = kv._1

      // 同一个班级的所有的年龄
      val ages: List[Int] = kv._2.toList

      val avgAge: Double = ages.sum.toDouble / ages.size

      (clazz, avgAge)
    })


    avgAgeRDD.foreach(println)

    while(true) {

    }

  }

}
