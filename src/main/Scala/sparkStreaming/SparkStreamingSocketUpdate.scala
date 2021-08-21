package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext, Time}


/**
 *
 * windows 使用ncat.exe工具：
 *
 * @author HuGuangtao
 * @date 2021/7/23 12:31
 * @version 1.0
 */
object SparkStreamingSocketUpdate {
  def main(args: Array[String]): Unit = {

    //获取spark配置文件
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamingSocketUpdate").setMaster("local[*]")

    //创建checkpoint目录
    val checkPointPath: String = "H:\\input\\sparkInput\\sparkStreaming\\check_point"

    //创建StreamingContext的函数
    val createStreamingContext: () => StreamingContext = () => {
      //创建StreamingContext对象，每5秒赞一批数据触发运算
      val context = new StreamingContext(conf, Durations.seconds(5))

      //创建checkpoint
      context.checkpoint(checkPointPath)

      //从socket接收接收DStream
      val inputDS: ReceiverInputDStream[String] = context.socketTextStream("localhost", 6666)
      val DS1: DStream[(String, Int)] = inputDS.flatMap(_.split(" ")).map((_, 1))

      //DS2：和上批次合并的
      val DS2: DStream[(String, Int)] = DS1.updateStateByKey((seq: Seq[Int], lastOption: Option[Int]) => {
        var sum: Int = 0
        //统计本批次
        for (elem <- seq) {
          sum += elem
        }
        //上一批次
        val lastSum: Int = if (lastOption.isDefined) lastOption.get else 0

        //汇总
        Some(sum + lastSum)
      })

      DS2.foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
        val arr: Array[(String, Int)] = rdd.collect()
        println(s"time: ${time} ==> data:${arr.toBuffer} ")
      })
      context
    }


    //获取或者创建streamingContext
    // 当checkpoint目录没有数据时，会执行函数创建StreamingContext对象
    // 当checkpoint目录有数据时，会从checkPointPath恢复StreamingContext对象， 同时StreamingContext对象记录着处理的socket流
    val context1: StreamingContext = StreamingContext.getOrCreate(checkPointPath, createStreamingContext)

    //    启动context
    context1.start()
    // 阻塞一直运行下去
    context1.awaitTermination()


  }

}
