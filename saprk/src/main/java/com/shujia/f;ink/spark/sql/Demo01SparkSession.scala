package com.shujia.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql._


object Demo01SparkSession {
  def main(args: Array[String]): Unit = {

    /**
     * 创建sparkSession，是spark 2.0统一的入口
     *
     */

     val spark:SparkSession =  SparkSession
       .builder()
       .master("local")
       .appName("sparkSession")
       .config("spark.sql.shuffle.partitions",1) // spark sql shuffle之后分区数据，默认值是200
       .getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 也可以直接sparkContext 使用rdd的api
    val sc: SparkContext = spark.sparkContext

    /**
     * 1、读取数据
     *
     */

    // 读取一个json格式的数据
    val df: DataFrame = spark.read.json("data/students.json")

    // 打印数据
    // 相当于一个action算子
    df.show()

    // 打印表结构
    df.printSchema()

    /**
     *
     * DSL
     */

    // 使用字符串表达式
    val whereDF: Dataset[Row] = df.where("gender = '男'")

    whereDF.show()

    // 列对象表达式
    whereDF.where($"age" > 23).show()

    val countDF: DataFrame = df
      .groupBy("clazz")
      .count()

    countDF.show()

    /**
     *
     * 写sal
     */

    // 注册临时视图
    df.createTempView("student")

    // 返回值是一个DF

    val sqlCount: DataFrame = spark.sql("select clazz,count(1) as c from student group by clazz")

    sqlCount.show()

    /**
     *
     * 保存数据
     */

    sqlCount.write
      .mode(SaveMode.Overwrite) // 如果输出目录存在自动覆盖
      .json("data/json")
  }


}
