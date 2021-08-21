package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author HuGuangtao
 * @date 2021/8/3 10:27
 * @version 1.0
 */
object SortTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SortTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    //引入隐式转换
    val outPutDir = "H:\\output\\sparkout\\sortTestout"
    import Util.MyPredef.String2HdfsUtil
    outPutDir.deleteHdfs()
    //输入目录
    val rdd1: RDD[String] = sc.textFile("H:\\input\\sparkInput\\input_sort", 10)

    //打印分区数
    println(rdd1.partitions.length)
    println(rdd1.getNumPartitions)
    val rddSort: RDD[(String, Int)] = rdd1.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortBy(_._1)

    val rdd2: RDD[(String, Int)] = rddSort.partitionBy(new MyPartitioner(2))
    val rdd3: RDD[(String, Int)] = rdd2.mapPartitionsWithIndex((index, it) => {
      println(s"${index} ${it.toBuffer}")
      it
    })

    rdd3.collect()
/*
    rddSort.saveAsTextFile(outPutDir)
    println(rddSort.toDebugString)
    sc.stop()*/
  }
}

class MyPartitioner(val num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    val word: String = key.asInstanceOf[String]
    if (word.compareTo("h") > 0) 0 else 1
  }
}


