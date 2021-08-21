package day01

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author HuGuangtao
 * @date 2021/8/1 22:25
 * @version 1.0
 */
object WorldCountCache {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Cache").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.parallelize(Array("java", "spark", "flink", "sqoop", "spark"))
    val rdd1: RDD[(String, Int)] = rdd.map(f=>{
      println(f)
      (f,1)
    }).cache()
//    val rdd2: RDD[(String, Int)] = rdd1.reduceByKey(_ + _)
    rdd1.count()
    rdd1.count()
    println(rdd1.getNumPartitions)







  }

}
