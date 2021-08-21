package sparkcore

import Util.{OrcFormat, OrcUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.orc.{OrcNewInputFormat, OrcNewOutputFormat, OrcStruct}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
 *
 * spark core 实现mapJoin  需要用到orcUtil
 *
 * @author HuGuangtao
 * @date 2021/8/21 19:17
 * @version 1.0
 */
object MapJoin {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MapJoin")
    val sc = new SparkContext(conf)

    //将小表(字典文件)加载到广播变量 H:\input\sparkInput\input_orc
    val dirPath: String = "H:/input/country_dict.dat"
    //读取字典文件 生成一个map集合 US\t美国
    val words: List[String] = Source.fromFile(dirPath).getLines().toList
    val dirMap: Map[String, String] = words.map(line => {
      val arr: Array[String] = line.split("\t")
      (arr(0), arr(1))
    }).toMap

    //广播变量 小表
    val dirMapBroad: Broadcast[Map[String, String]] = sc.broadcast(dirMap)


    //join上的累加器
    val matchAcc: LongAccumulator = sc.longAccumulator

    //没有join上的累加器
    val NotMatchAcc: LongAccumulator = sc.longAccumulator


    //读取hdfs上的orc文件，生成pairRdd
    /* 根据读api，获取到需要的参数
      path: String, 输入路径
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V],
      conf: Configuration = hadoopConfiguration
     */
    val inputPath: String = "H:\\input\\sparkInput\\input_orc"
    val orcRdd: RDD[(NullWritable, OrcStruct)] = sc.newAPIHadoopFile(inputPath,
      classOf[OrcNewInputFormat],
      classOf[NullWritable],
      classOf[OrcStruct],
      new Configuration())

    val resRdd: RDD[(NullWritable, Writable)] = orcRdd.mapPartitions(it => {
      // 创建 OrcUtil对象
      val util = new OrcUtil
      // 根据schema获取读的inspector对象
      util.setOrcTypeReadSchema(OrcFormat.SCHEMA)
      // 根据schema获取写的inspector对象
      util.setOrcTypeWriteSchema(OrcFormat.SCHEMA1)

      //从广播变量获取字典map
      val dirMap: Map[String, String] = dirMapBroad.value

      // 新定义一个List用于存储并返回join的结果
      val listNew = new ListBuffer[(NullWritable, Writable)]

      //遍历每个分区的元素，和字典map 进行join
      it.foreach(f => {
        // f----> (NullWritable, OrcStruct)
        val countryCode: String = util.getOrcData(f._2, "country")

        val maybeString: Option[String] = dirMap.get(countryCode)
        if (maybeString == None) {
          NotMatchAcc.add(1)
        } else {
          matchAcc.add(1)

          // 将数据转成能写入orc文件的Writable对象
          // 1 添加数据，要保证字段顺序和类型要和 schema字符串
          //   struct<code:string,country:string>
          val country: String = maybeString.get
          util.addAttr(countryCode, country)
          //序列化
          val w: Writable = util.serialize()
          //
          listNew.append((NullWritable.get(), w))

        }
      })
      listNew.toIterator
    })

    //输出目录
    val outputPath: String = "H:\\output\\sparkout\\orcOutPut"
    import Util.MyPredef.String2HdfsUtil
    outputPath.deleteHdfs()

    // action算子，写入orc文件
    // 如果想写入orc，需要将rdd转成 rdd[(NullWritable,Writable)]
    resRdd.saveAsNewAPIHadoopFile(
      outputPath,
      classOf[NullWritable],
      classOf[Writable],
      classOf[OrcNewOutputFormat]
    )


    
    //    val strings: Array[String] = rddRes.take(10)
    //    strings.foreach(println)
    //
    //    println(NotMatchAcc.value)
    //    println(matchAcc.value)


  }

}
