package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo06MyGroupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("mygroupbykey")

    val sc = new SparkContext(conf)
    val student: RDD[String] = sc.textFile("data/students.txt")

    // 取出班级和年龄
    val kvRDD: RDD[(String, Int)] = student.map(student => {
      val split: Array[String] = student.split(",")
      val clazz: String = split(4)
      val age: Int = split(2).toInt
      (clazz, age)
    })


    // 通过groupbykey
    val groupRDD: RDD[(String, Iterable[Int])] = kvRDD.groupByKey()
    groupRDD.foreach(println)

    val avgAgeRDD: RDD[(String, Double)] = groupRDD.map(kv => {
      val clazz: String = kv._1
      val ages: List[Int] = kv._2.toList
      val avgAge: Double = ages.sum / ages.size

      (clazz, avgAge)
    })

    avgAgeRDD.foreach(println)

    }
}

