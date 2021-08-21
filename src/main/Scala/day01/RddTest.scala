package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author HuGuangtao
 * @date 2021/8/2 19:21
 * @version 1.0
 */
object RddTest {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[6]").setAppName("RddTest")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(1 to 10)
//    println(rdd.getNumPartitions)
    val ints: Array[Int] = rdd.filter(_ < 3).collect()
    println(ints.toList)


  }

}
