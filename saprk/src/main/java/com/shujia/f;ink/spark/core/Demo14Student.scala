package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo14Student {
  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("student")

    val sc = new SparkContext(conf)

    /**
     * 1、统计班级人数
     *
     */
    val studentsRDD: RDD[String] = sc.textFile("data/students.txt")
    studentsRDD
      .map(line => (line.split(",")(4),1))
      .reduceByKey(_ + _)
      .foreach(println)

    /**
     * // (x,y) =>x+y 这里可以简写，
     * (x,y) =>x+y的意思是这里两个参数我们逻辑上让他分别代表同一个key的两个不同values，而不是kv
     */

    /**
     * 2、统计学生的总分
     */

    val scoreRDD: RDD[String] = sc.textFile("data/score.txt")

    //通过groupby先进行聚合
    val scoRDD: RDD[(String, Iterable[String])] = scoreRDD.groupBy((line => line.split(",")(0)))

    // 通过map算子将id和成绩取出来
    scoRDD.map(kv =>{
      val id: String = kv._1

      val score: List[String] = kv._2.toList //将value变成集合
      val fs: List[Int] = score.map(line => line.split(",")(2).toInt) //通过split分割，并将分数转换成Int类型
      val sumScore: Int = fs.sum // 通过sum函数求和
      (id,sumScore) // 返回id和分数
    })
      .foreach(println)

    // 首先分割，然后取出学号和分数，并将分数转换成Int格式
    val kvRDD: RDD[(String, Int)] = scoreRDD.map(line => {
      val split: Array[String] = line.split(",")

      (split(0), split(2).toInt)

    })

    // 统计学生总分
    val resultRDD: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) => x + y)
    resultRDD.foreach(println)
  }

}
