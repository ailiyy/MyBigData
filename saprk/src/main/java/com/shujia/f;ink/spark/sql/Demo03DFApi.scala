package com.shujia.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo03DFApi {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("dfApi")
      .config("spark.sql.shuffle.partitions", 1) // spark sql shuffle之后分区数据，默认值是200
      .getOrCreate()

    val student: DataFrame = spark
      .read
      .format("csv")
      .option("sep", ",") // 指定数据分割方式，默认是逗号
      // 指定列名和列的类型，顺序要和数据的顺序一致
      .schema("id string, name string, age int, gender string, clazz string")
      .load("data/students.txt")


    /**
     * show：相当于action算子，查看部分数据
     *
     */

    student.show(100)
    // 完整显示数据
    student.show(110,false )
    // 使用列名
    student.select("id","name").show()
  }

}
