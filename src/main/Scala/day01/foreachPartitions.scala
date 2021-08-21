package day01

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author HuGuangtao
 * @date 2021/8/2 21:23
 * @version 1.0
 */
object foreachPartitions {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("foreachPartitions").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(1 to 6, 2)
    //    rdd1.foreachPartition(_.foreach(println))
    var sum: Int = 0

    //累加器
    val accumulator: LongAccumulator = sc.longAccumulator

    rdd1.foreachPartition((it: Iterator[Int]) => {

      val res: Int = it.toList.sum
      //累加器累加获取sum
      accumulator.add(res)
      /*      sum += res
              println(sum)*/
    })

    println(accumulator.value)
  }

}
