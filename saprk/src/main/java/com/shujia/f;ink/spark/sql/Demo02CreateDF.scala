package com.shujia.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame,Row, SparkSession}

object Demo02CreateDF {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("createDf")
      .getOrCreate()

    /**
     * 读取json格式的数据
     *
     * 会自动识别json中的列名和类型
     */

    val jsonDF:DataFrame = spark
      .read
      .format("json") // 指定读取数据的格式
      .load("data/students.json") //指定读取数据的路径

    jsonDF.show()

    /**
     * 读取csv
     *
     */
    val csvDF: DataFrame = spark
      .read
      .format("csv")
      .option("sep",",")
      .schema("id string, name string, age int, gender string, clazz string")
      .load("data/students.txt")

    csvDF.printSchema()
    csvDF.show()

    /**
     * 链接jdbc 构建DF
     *
     */

    val jdbcDF: DataFrame = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://master:3306")
      .option("dbtable", "bigdata.student")
      .option("user", "root")
      .option("password", "123456")
      .load()

    jdbcDF.printSchema()
    jsonDF.show()


    /**
     * 可以基于RDD构建DF
     *
     */

    val sc: SparkContext = spark.sparkContext

    // 读取数据构建RDD
    val rdd: RDD[String] = sc.textFile("data/students.txt")

    // 将数据切分出来，转换成元组
    val studentRDD: RDD[(String, String, Int, String, String)] = rdd.map(line => {
      val split: Array[String] = line.split(",")
      val id: String = split(0)
      val name: String = split(1)
      val age: Int = split(2).toInt
      val gender: String = split(3)
      val clazz: String = split(4)
      (id, name, age, gender, clazz)
    })
    studentRDD.foreach(println)

    import  spark.implicits._

    // 将RDD转换成DF ，指定列名
    val rddToDF: DataFrame = studentRDD.toDF("id", "name", "age", "gender", "clazz")
    rddToDF.show()

    /**
     * 样例类
     *
     */

    val caseClassRDD: RDD[(String, String, Int, String, String)] = rdd.map(line => {
      val split: Array[String] = line.split(",")
      val id: String = split(0)
      val name: String = split(1)
      val age: Int = split(2).toInt
      val gender: String = split(3)
      val clazz: String = split(4)
      (id, name, age, gender, clazz)
    })

    // 将样例类的rdd转换成DF 不需要指定列名
    val caseClassDF: DataFrame = caseClassRDD.toDF()
    caseClassDF.show()

    /**
     * 将DF 转换成RDD
     *
     */

    val toRDD: RDD[Row] = caseClassDF.rdd
    toRDD.foreach(println)


    // 解析ROW对象
    val resultRDD: RDD[(String, String, Int, String, String)] = toRDD.map(row => {
      // 通过列名获取数据
      val id: String = row.getAs[String]("id")
      val name: String = row.getAs[String]("name")
      val age: Int = row.getAs[Int]("age")
      val gender: String = row.getAs[String]("gender")
      val clazz: String = row.getAs[String]("clazz")
      (id, name, age, gender, clazz)
    })
    resultRDD.foreach(println)

  }
  case class Student(id:String, name:String, age:Int, gender:String, clazz:String)

}
