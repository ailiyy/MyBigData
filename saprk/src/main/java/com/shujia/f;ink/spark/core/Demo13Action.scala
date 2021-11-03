package com.shujia.spark.core

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo13Action {
  def main(args: Array[String]): Unit = {

    /**
     * 转换算子：懒执行，由一个rdd转换成另一个rdd，需要一个action触发触发
     * 操作算子：触发任务执行，不会返回rdd，每个action算子都会出发一个job
     */

    val conf:SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("action")

    val sc = new SparkContext(conf)

    //###########################################
    val ageRDD: RDD[(String, Int)] = sc.parallelize(List(("001", 23), ("002", 24), ("003", 25), ("004", 26)))

    // 转换算子不能单独存在
    val rdd1: RDD[(String, Int)] = ageRDD.map(kv => {
      println("map算子内部")
      (kv._1, kv._2 + 1)
    })

    //#############################################
    val studentsRDD: RDD[String] = sc.textFile("data/students.txt")

    /**
     * count:统计rdd的行数
     */
    val count: Long = studentsRDD.count()
    println(count)


    /**
     * foreach:遍历rdd
     *
     */

    studentsRDD.foreach(println)


    /**
     * reduce:全局聚合
     *
     */

    val str: String = studentsRDD.reduce((x, y) => x + y)
    println(str)


    /**
     * save:将数据保存到hdfs，
     * 如果以local模式运行，保存的位置就是本地磁盘
     * 如果是以集群模式运行，保存的位置就是hdfs
     *
     */

    // 使用hdfs java api 删除输出目录

    val configuration = new Configuration()
    val fileSystem: FileSystem = FileSystem.get(configuration)

    // 删除输出目录
    if (fileSystem.exists((new Path("data/save")))) {
      fileSystem.delete(new Path("data/save"),true)
    }

    studentsRDD.saveAsTextFile("data/save")

//    while (true) {
//
//    }

  }

}
