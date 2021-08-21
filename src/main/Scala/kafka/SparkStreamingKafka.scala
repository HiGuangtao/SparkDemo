package kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, HasOffsetRanges, KafkaUtils, LocationStrategies, LocationStrategy, OffsetRange}
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * 测试 kafka直连 SparkStreaming
 * @author HuGuangtao
 * @date 2021/8/20 11:31
 * @version 1.0
 */
object SparkStreamingKafka {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("SparkStreamingKafka").setMaster("local[*]")
    val topics: String = "hgt_SparkStreamingKafka"

    // 读取kafka的配置
    val kafkaParams: mutable.HashMap[String, Object] = mutable.HashMap[String, Object]()
    kafkaParams += "bootstrap.servers" -> "s1.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092"
    kafkaParams += "group.id" -> "group106"
    kafkaParams += "key.deserializer" -> classOf[StringDeserializer].getName
    kafkaParams += "value.deserializer" -> classOf[StringDeserializer].getName
    kafkaParams += "auto.offset.reset" -> "earliest"
    kafkaParams += "enable.auto.commit" -> "true"

    //创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Durations.seconds(5))

    // 位置策略 它将在所有的 Executors 上均匀分配分区；
    val locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent

    //只订阅0和1分区的数据
    val p0: TopicPartition = new TopicPartition(topics, 0)
    val p1: TopicPartition = new TopicPartition(topics, 1)
    val partitions: ListBuffer[TopicPartition] = new ListBuffer[TopicPartition]()
    partitions += p0
    partitions += p1

    //消费策略：分配策略
//    val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Assign[String, String](partitions, kafkaParams)

    //消费策略：订阅策略
    val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe[String,String](topics.split(",").toSet, kafkaParams)

    //创建Kafka DStream 通过KafkaUtils工具类创建kafka直连流
    val KafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, locationStrategy, consumerStrategy)

    // 如果想看到kafka消费的offset信息，那就得从kafka流获取，如果通过流转换后，元数据信息就看不见了
    val KafkaDS2: DStream[ConsumerRecord[String, String]] = KafkaDS.transform((KafkaRdd: RDD[ConsumerRecord[String, String]]) => {
      println(s"kafka直连流的rdd分区数：${KafkaRdd.getNumPartitions}")
      // kafka流底层就是kafkaRDD， kafkaRdd 实现了 HasOffsetRanges
      val ranges: HasOffsetRanges = KafkaRdd.asInstanceOf[HasOffsetRanges]
      val ranges1: Array[OffsetRange] = ranges.offsetRanges
      for (elem <- ranges1) {
        val topic: String = elem.topic
        val partition: Int = elem.partition
        val topicPartition: TopicPartition = elem.topicPartition()
        // 当前批次从哪个分区的哪个offset开始消费
        val offset: Long = elem.fromOffset
        println(s"消费的offset信息：主题：${topic}\t 分区：${partition}\t topicPartition: ${topicPartition} \t offset：${offset}")
      }
      KafkaRdd

    })
    //从Kafka拉取过来的流进行转换
    val KafkaDS3: DStream[(String, Int)] = KafkaDS2.flatMap(_.value().split(" ")).map((_, 1)).reduceByKey(_ + _)

    //流输出 rdd.collect()是将rdd从executor拉取到driver端
    KafkaDS3.foreachRDD((rdd, time) => {
      println(s"转换后的rdd分区数：${rdd.getNumPartitions}")
      println(s"time:${time}, data:${rdd.collect().toBuffer}")
    })
    ssc.start()
    ssc.awaitTermination()


  }
}
