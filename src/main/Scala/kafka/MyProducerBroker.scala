package kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.actors.Actor


/**
 * 测试kafka 通过broker消费kafka
 *
 * @author HuGuangtao
 * @date 2021/8/19 22:19
 * @version 1.0
 */
class MyProducerBroker(var topic: String) extends Actor {

  //根据配置初始化 KafkaProducer
  val props = new Properties()
  //broker地址 用的教室集群
  props.put("bootstrap.servers", "s1.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092")
  //实现序列化
  props.put("key.serializer", classOf[StringSerializer].getName)
  props.put("value.serializer", classOf[StringSerializer].getName)

  //创建producer [String, String] 分别是topic 发送的内容
  val producer = new KafkaProducer[String, String](props)

  override def act(): Unit = {
    var num: Int = 1
    while (true) {
      val msg = s"haiNiu_${num} haiNiu1_${num+1} bb cc"
      println(s"send: ${msg}")

      this.producer.send(new ProducerRecord[String, String](this.topic, msg))
      num += 1
      if (num > 2) {
        num = 1
      }
      Thread.sleep(1000)
    }
  }
}

class MyConsumerBroker(val topic: String) extends Actor {

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
  pro.put("value.deserializer", classOf[StringDeserializer].getName)

  // 利用kafka的高级api消费 生成消费者
  val consumer = new KafkaConsumer[String, String](pro)

  //  consumer.assign()

  //指定consumer读取topic中所有的partition，这个叫做订阅方式
  consumer.subscribe(java.util.Arrays.asList(topic))

  override def act(): Unit = {
    while (true) {
      // Iterable[ConsumerRecord[K, V]] 消费者从kafka中拉取数据 返回一个迭代器
      val records: ConsumerRecords[String, String] = this.consumer.poll(100)
      // 通过隐式转换把java的 Iterable 转成 scala的 Iterable
      import scala.collection.convert.wrapAll._
      //遍历返回来的记录迭代器
      for (record <- records) {
        //数据的主题
        val topicName: String = record.topic()
        //获取数据所在分区
        val partition: Int = record.partition()
        //offset 读取记录位置
        val offset: Long = record.offset()
        //具体发送的消息
        val msg: String = record.value()
        println(s"receive: ${msg}\t${topicName}\t${partition}\t${offset}")
      }
    }
  }
}
//测试
object KafkaBrokerTest {
  def main(args: Array[String]): Unit = {
    val topic:String = "hgt_SparkStreamingKafka"
    val producer: Actor = new MyProducerBroker(topic).start()
//    val consumer: Actor = new MyConsumerBroker(topic).start()


  }
}
