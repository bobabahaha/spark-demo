package com.atguigu.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)
    //3.读取指定位置文件:hello atguigu atguigu
    val lineRdd: RDD[String] = sc.textFile("E:\\bigdata0426\\spark-demo\\input")
    //4.读取的一行一行的数据分解成一个一个的单词（扁平化）(hello)(atguigu)(atguigu)
    val wordRdd: RDD[String] = lineRdd.flatMap(_.split(" "))
    //5. 将数据转换结构：(hello,1)(atguigu,1)(atguigu,1)
    val word2: RDD[(String, Int)] = wordRdd.map((_,1))
    //6.将转换结构后的数据进行聚合处理 atguigu:1、1 =》1+1  (atguigu,2)
    val word3: RDD[(String, Int)] = word2.reduceByKey((_+_))
    //7.将统计结果采集到控制台打印

    word3.saveAsTextFile("E:\\bigdata0426\\spark-demo\\output")
    //一行搞定

    //8.关闭连接
    sc.stop()
  }

}
