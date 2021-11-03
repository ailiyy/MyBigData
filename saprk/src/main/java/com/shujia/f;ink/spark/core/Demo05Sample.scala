package com.shujia.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo05Sample {
  def main(args: Array[String]): Unit = {
    /**
     * 创建spark环境
     *
     */

    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("sample")

    val sc = new SparkContext(conf)

    // 读取文件
    val students: RDD[String] = sc.textFile("data/students.txt")


    /**
     * sample:抽样
     * withReplacement : Boolean , True表示进行替换采样，False表示进行非替换采样
     * fraction : Double, 在0~1之间的一个浮点值，表示要采样的记录在全体记录中的比例


     * sample(withReplacement : scala.Boolean, fraction : scala.Double，seed scala.Long)
     * sample算子是用来抽样用的，其有3个参数
     * withReplacement：表示抽出样本后是否在放回去，true表示会放回去，这也就意味着抽出的样本可能有重复
     * fraction ：抽出多少，这是一个double类型的参数,0-1之间，eg:0.3表示抽出30%
     * seed：表示一个种子，根据这个seed随机抽取，一般情况下只用前两个参数就可以，
     * 那么这个参数是干嘛的呢，这个参数一般用于调试，有时候不知道是程序出问题还是数据出了问题，就可以将这个参数设置为定值
     */

    val sampleRDD: RDD[String] = students.sample(true, 0.1)
    sampleRDD.foreach(println)
  }

}
