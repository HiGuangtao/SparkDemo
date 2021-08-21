package kafka

import java.util.Properties

import javakafka.{KafkaDataDeserializer, KafkaDataSerializer, ProducerPartitioner}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.actors.Actor


/**
 * 定义一个样例类，用于测试 如果想通过kafka传输，需要实现序列化
 */
case class KafkaData(val data: String)

/**
 * 生产者
 *
 * @author HuGuangtao
 * @date 2021/8/20 9:21
 * @version 1.0
 */
class KafkaBrokerSer() extends Actor {

  var topic: String = _
  //
  var producer: KafkaProducer[String, KafkaData] = _

  //辅助构造器
  def this(topic: String) = {
    this()
    this.topic = topic
  }

  //
  //根据配置初始化 KafkaProducer
  val props = new Properties()
  //broker地址 用的教室集群
  props.put("bootstrap.servers", "s1.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092")
  //实现序列化
  props.put("key.serializer", classOf[StringSerializer].getName)
  props.put("value.serializer", classOf[KafkaDataSerializer].getName)
  props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,classOf[ProducerPartitioner].getName)
  //创建生产者
  producer = new KafkaProducer[String, KafkaData](props)

  override def act(): Unit = {
    var num: Int = 1
    while (true) {
      val msg: KafkaData = KafkaData(s"hainiu_${num}")
      println(s"send: ${msg}")
      this.producer.send(new ProducerRecord[String, KafkaData](topic, msg))
      num += 1
      if (num > 2) {
        num = 1
      }
      Thread.sleep(1000)
    }
  }
}

/**
 * 创建消费者
 */

class KafkaBrokerConsumerSer extends Actor {

  var topic: String = _
  var consumer: KafkaConsumer[String, KafkaData] = _

  def this(topic: String) = {
    this()
    this.topic = topic

    //创建属性
    val pro = new Properties()
    // 更高级的api，直接读broker
    pro.put("bootstrap.servers", "s1.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092")
    //每个consumer在消费数据的时候指定自己属于那个consumerGroup
    pro.put("group.id", "group101")
    //consumer读取的策略
    pro.put("auto.offset.reset", "earliest")
    //是否自动提交offset
    pro.put("enable.auto.commit", "true")
    //多长时间提交一次
    pro.put("auto.commit.interval.ms", "1000")
    //使用String的序列化工具把二进制转成String类型
    pro.put("key.deserializer", classOf[StringDeserializer].getName)
    //使用自定义的序列化工具把二进制转成String类型
    pro.put("value.deserializer", classOf[KafkaDataDeserializer].getName)

    //创建消费者
    consumer = new KafkaConsumer[String, KafkaData](pro)

    //指定consumer读取topic中所有的partition，这个叫做订阅方式
    consumer.subscribe(java.util.Arrays.asList(topic))

    //指定consumer读取topic中的那一个partition，这个叫做分配方式 只接收分区0的数据
//   this.consumer.assign(java.util.Arrays.asList(new TopicPartition(topic,0)))


  }

  override def act(): Unit = {
    while (true) {
      // Iterable[ConsumerRecord[K, V]] 消费者从kafka中拉取数据 返回一个迭代器
      val records: ConsumerRecords[String, KafkaData] = this.consumer.poll(100)
      // 通过隐式转换把java的 Iterable 转成 scala的 Iterable
      import scala.collection.convert.wrapAll._
      //遍历返回来的记录迭代器
      for (record <- records) {
        //数据的主题
        val offset: Long = record.offset()
        val topicName: String = record.topic()
        val partition: Int = record.partition()
        val msg: String = record.value().data
        println(s"receive: ${msg}\t${topicName}\t${partition}\t${offset}")
      }
    }
  }
}

object SerTest {
  def main(args: Array[String]): Unit = {

    new KafkaBrokerSer("hgt_SerTest").start()
    new KafkaBrokerConsumerSer("hgt_SerTest").start()

  }
}
