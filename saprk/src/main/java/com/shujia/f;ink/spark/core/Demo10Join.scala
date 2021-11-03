package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo10Join {
  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf()
      .setAppName("join")
      .setMaster("local")

    val sc = new SparkContext(conf)


    val nameRDD: RDD[(String, String)] = sc.parallelize(List(("001", "张三"), ("002", "李四"), ("003", "王五"), ("005", "赵六")))


    val ageRDD: RDD[(String, Int)] = sc.parallelize(List(("001", 23), ("002", 24), ("003", 25), ("004", 26)))

    /**
     * join:对两个rdd进行关联，通过key进行关联，默认是inner join
     *
     */

    val innerJoinRDD: RDD[(String, (String, Int))] = nameRDD.join(ageRDD)
    innerJoinRDD.foreach(println)

    // 整理数据
    val resultRDD: RDD[(String, String, Int)] = innerJoinRDD.map {
      case (id: String, (name: String, age: Int)) =>
        (id, name, age)
    }

    resultRDD.foreach(println)


    /**
     * leftJoin：以左表为基础，如果没有关联上右表补空
     *
     */

    val leftJoinRDD: RDD[(String, (String, Option[Int]))] = nameRDD.leftOuterJoin(ageRDD)

    val resultRDD2: RDD[(String, String, Int)] = leftJoinRDD.map {

      // 分开处理两种情况
      case (id: String, (name: String, age: Some[Int])) => {
        (id, name, age.get)
      }

      case (id: String, (name: String, None)) => {
        // 如果没有关联给一个默认值
        (id, name, 0)
      }
    }
    resultRDD2.foreach(println)


    /**
     *
     * fullOuterJoin:全关联，只要有一边有数据会关联，列一边如果没有数据，则补空
     *
     */
    val fullJoinRDD: RDD[(String, (Option[String], Option[Int]))] = nameRDD.fullOuterJoin(ageRDD)

    val resultRDD3: RDD[(String, String, Int)] = fullJoinRDD.map {

      // 分开处理两种情况
      case (id: String, (name: Some[String], age: Some[Int])) => {
        (id, name.get, age.get)
      }

      case (id: String, (name: Some[String], None)) => {
        // 如果没有关联就给一个默认值
        (id, name.get, 0)
      }
      case (id: String, (None, age: Some[Int])) => {
        // 如果没有关联就给一个默认值

        (id, "默认值", age.get)
      }



    }
    resultRDD3.foreach(println)
  }

}
