package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}


/**
 *
 * windows 使用ncat.exe工具：
 *
 * @author HuGuangtao
 * @date 2021/7/23 12:31
 * @version 1.0
 */
object SparkStreamingSocket {
  def main(args: Array[String]): Unit = {

    //获取spark配置文件
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamingSocket").setMaster("local[*]")

    //获取streamingContext
    val context: StreamingContext = new StreamingContext(conf, Durations.seconds(5))

    //从socket接收接收DStream
    val inputDS: ReceiverInputDStream[String] = context.socketTextStream("localhost", 6666)

    /*    //转换方法一：利用DS的转换API
        val DS1: DStream[(String, Int)] = inputDS.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

        //转换方法二：利用DS的transform转换方法
        val DS2: DStream[(String, Int)] = inputDS.transform((rdd, time) => {
          val newRdd: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
          newRdd
        })

        //输出DStream
        DS2.foreachRDD((rdd, time) => {
          val arr: Array[(String, Int)] = rdd.collect()
          println(s"time: ${time}  data:${arr.toBuffer} ")
        })

     */

    //foreachRDD 实现 将流式计算 完全用批处理的方式写
    inputDS.foreachRDD((rdd, time) => {
      val newRdd: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      val arr: Array[(String, Int)] = newRdd.collect()
      println(s"time: ${time}  data:${arr.toBuffer} ")
    })


    //    启动context
    context.start()
    // 阻塞一直运行下去
    context.awaitTermination()


  }

}
