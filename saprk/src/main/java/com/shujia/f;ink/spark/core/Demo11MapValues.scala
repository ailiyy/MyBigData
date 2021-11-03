package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo11MapValues {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("mapvalues")

    val sc = new SparkContext(conf)

    val ageRDD: RDD[(String, Int)] = sc.parallelize(List(("001", 23), ("002", 24), ("003", 25), ("004", 26)))

    val rdd2: RDD[(String, Int)] = ageRDD.map(kv => (kv._1, kv._2 + 1))

    rdd2.foreach(println)

    /**
     * mapValues:处理value，key不变
     *
     */

    val rdd3: RDD[(String, Int)] = ageRDD.mapValues(age => age + 1)

    rdd3.foreach(println)
  }

}
