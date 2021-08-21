package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 测试mapPartitionsWithIndex
 * @author HuGuangtao
 * @date 2021/8/2 19:50
 * @version 1.0
 */
object RddPartition {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RddPartition").setMaster("local[*]")
    val context: SparkContext = new SparkContext(conf)
    val rdd1: RDD[Int] = context.parallelize(1 to 10,2)
    val rdd2: RDD[Int] = rdd1.mapPartitionsWithIndex((index, it) => {
      val list: List[Int] = it.toList
      val ints: List[Int] = list.map(_ + 1)
      println(s"$index :${ints.toBuffer}")
      ints.toIterator
    })
    val ints: Array[Int] = rdd2.collect()
    println(ints.toBuffer)



  }

}
