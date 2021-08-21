package day01

import java.lang

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author HuGuangtao
 * @date 2021/8/2 22:10
 * @version 1.0
 */
object ShareVariable {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sharedvartest")
    val sc = new SparkContext(conf)
    val acc: LongAccumulator = sc.longAccumulator
    //driver端定义外部变量list
    val list = List(1, 5, 8)
    val broad: Broadcast[List[Int]] = sc.broadcast(list)
    // driver端定义外部变量count用于计数
    var count: Int = 0
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8), 2)
    val rdd1: RDD[Int] = rdd.filter(f => {
      if (broad.value.contains(f)) {
        count = count + 1
        println(s"driver端 ${count}")
        acc.add(1)
        true
      } else {
        false
      }
    })

    val rs: Array[Int] = rdd1.collect()
    // 发现executor端修改了，但driver端并没有修改
/*    println(s"count:${count}")*/
    val value: lang.Long = acc.value
    println(s"acc ${value}")
    println(rs.toBuffer)
  }


}
