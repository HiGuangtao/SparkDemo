package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 * 处理流程：先将日志文件加载到RDD进行处理，然后转换为DataFrame，PV 输出Text格式；UV输出json格式
 *
 * @author HuGuangtao
 * @date 2021/8/17 16:07
 * @version 1.0
 */
object ExamTest {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ExamTest")
    //因为设计到group by 默认是200个分区，所以这里重新分区
    sparkConf.set("spark.sql.shuffle.partitions", "1")

    //创建sparkSession
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //读取text文件，创建rdd
    val textRdd: RDD[String] = sparkSession.sparkContext.textFile("H:\\input\\sparkInput\\input_exam")

    //获取RDD row
    val rowRdd: RDD[Row] = textRdd.map((f: String) => {
      val words: Array[String] = f.split(" ")
      val ip: String = words(0)
      val str: String = words(1)
      val addr: Array[String] = str.split("/")
      val app: String = addr(1)
      val index: String = addr(2)
      Row(ip, app, index)
    })

    // 设置row里面每个字段的具体类型
    val fields: ArrayBuffer[StructField] = new ArrayBuffer[StructField]
    fields += StructField("ip", DataTypes.StringType, nullable = true)
    fields += StructField("app", DataTypes.StringType, nullable = true)
    fields += StructField("index", DataTypes.StringType, nullable = true)
    val structType: StructType = StructType(fields)

    // 根据rowrdd 构建dataframe
    val frame: DataFrame = sparkSession.createDataFrame(rowRdd, structType)
//    frame.printSchema()
//    frame.show()

    // 方法一：创建临时视图，写sql
    frame.createOrReplaceTempView("examTable")

    //#PV（Page View）访问量，即页面访问量，每打开一次页面PV计数+1，刷新页面也是。 根据 应用和页面 组合分组，统计汇总
    //如果以text格式输出，字段只能是一个，所以只能用concat拼接
    val sql:String = "select concat(app,'\t',index,'\t',count(ip)) from examTable group by app,index"
    val pvFrame: DataFrame = sparkSession.sql(sql)
    pvFrame.printSchema()
    pvFrame.show()

    //PV输出目录 如果已经存在，就删除
    val textOutPutPath:String ="H:\\output\\sparkout\\examPVOutPut"
    import Util.MyPredef.String2HdfsUtil
    textOutPutPath.deleteHdfs()
    pvFrame.write.text(textOutPutPath)

    //#IV（Internet Protocol）访问量指独立IP访问数，计算是以一个独立的IP在一个计算时段内访问网站计算为1次IP访问数。根据app分组，统计去重的ip数量
    val sql2:String = "select app,count(distinct ip) from examTable group by app"
    val ivFrame: DataFrame = sparkSession.sql(sql2)
    ivFrame.show()

    //IV输出目录 如果已经存在，就删除
    val JsonOutPutPath:String ="H:\\output\\sparkout\\examIVOutPut"
    JsonOutPutPath.deleteHdfs()
    ivFrame.write.json(JsonOutPutPath)
  }


}
