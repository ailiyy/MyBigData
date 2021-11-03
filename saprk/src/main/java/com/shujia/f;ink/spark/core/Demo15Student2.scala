package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo15Student2 {
  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("student2")

    val sc = new SparkContext(conf)

    val studentsRDD: RDD[String] = sc.textFile("data/students.txt")
    val scoresRDD: RDD[String] = sc.textFile("data/score.txt")
    val courceRDD: RDD[String] = sc.textFile("data/cource.txt")



    /**
     * 1、统计班级级排名前十学生各科的分数 [学号,学生姓名，学生班级，科目，分数]
     *
     */

    // 1、计算学生总分
    val stuSumScoreRDD: RDD[(String, Int)] = scoresRDD
      .map(line => (line.split(",")(0), line.split(",")(2).toInt))
      .reduceByKey((x, y) => x + y)

//    stuSumScoreRDD.foreach(println)

    // 2、取出学号和班级
    val idAndClazz: RDD[(String, String)] = studentsRDD.map(line => {
      val split: Array[String] = line.split(",")
      val id: String = split(0)
      val clazz: String = split(4)
      (id, clazz)
    })

    // 3、关联
    val stuJoinRDD: RDD[(String, Int, String)] = stuSumScoreRDD
      .join(idAndClazz)
      .map {
        // 整理数据
        case (id: String, (sumSco: Int, clazz: String)) =>
          (id, sumSco, clazz)
      }
//    stuJoinRDD.foreach(println)

    // 计算每个班级前十的学生

    // 通过班级聚合
    val groupByRDD: RDD[(String, Iterable[(String, Int, String)])] = stuJoinRDD.groupBy(_._3)
//    groupByRDD.foreach(println)


    // 取班级前十的学生学号
    val clazzTop10RDD: RDD[String] = groupByRDD.flatMap {
      case (_: String, stus: Iterable[(String, Int, String)]) =>
        // 取出每个班级前十的学生
        val top10: List[(String, Int, String)] = stus.toList.sortBy(_._2).take(10)

        // 取出学号
        (top10.map(_._1))
    }
//    clazzTop10RDD.foreach(println)

    // 将rdd转换成集合
    val top10: Array[String] = clazzTop10RDD.collect()
//    top10.foreach(println)

    // 取出前十学生的信息
    val top10Student: RDD[String] = studentsRDD.filter(line => {
      val id: String = line.split(",")(0)
      top10.contains(id)
    })
//    top10Student.foreach(println)

    /**
     * 整理数据 【学号，学生姓名，学生班级，科目，分数】
     *
     */

    // 关联学生表和分数表
    // 转换成kv格式
    val scoreKvRDD: RDD[(String, String)] = scoresRDD.map(line => (line.split(",")(0), line))

    val stuScoJoinRDD: RDD[(String, (String, String))] = top10Student
      .map(line => (line.split(",")(0), line))
      .join(scoreKvRDD)

//    stuScoJoinRDD.foreach(println)


    // 整理数据以科目编号作为key
    val couAndStuRDD: RDD[(String, (String, String, String, String))] = stuScoJoinRDD.map {
      case (id: String, (stu: String, sco: String)) =>
        val stusplit: Array[String] = stu.split(",")
        // 姓名
        val name: String = stusplit(1)
        // 班级
        val clazz: String = stusplit(4)

        val scoSplit: Array[String] = sco.split(",")
        // 科目编号
        val couId: String = scoSplit(1)
        // 分数
        val s: String = scoSplit(2)
        (couId, (id, name, clazz, s))

    }
//    couAndStuRDD.foreach(println)

    // 将科目表转换成kv格式
    val courceKvRDD: RDD[(String, String)] = courceRDD.map(line => (line.split(",")(0), line.split(",")(1)))
//    courceKvRDD.foreach(println)

    // 关联科目表
    val resultRDD: RDD[(String, ((String, String, String, String), String))] = couAndStuRDD.join(courceKvRDD)


    // 整理数据
    resultRDD.map {
      case (_:String, ((id:String,name: String,clazz: String,s: String), couName:String)) =>

        // [学号，学生姓名，学生班级，科目，分数]
        s"$id,$name,$clazz,$couName,$s"
    }
//      .foreach(println)

  }
}
