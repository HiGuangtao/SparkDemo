package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
 * @author HuGuangtao
 * @date 2021/8/17 15:01
 * @version 1.0
 */
object MySchemaTest {

  val sparkConf: SparkConf = new SparkConf().setAppName("BeanSchemaTest").setMaster("local[*]")

  //创建sparkSession
  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  //创建sparkContext
  //    val sparkContext = new SparkContext(sparkConf)

  /*    //读取text文件，创建DataFrame
      val textDataFrame: DataFrame = sparkSession.read.text("H:\\input\\sparkInput\\input_text")

      textDataFrame.map(line=>{
        val country: String = line.getString(0)
        val gpcategory: String = line.getString(1)
        val pkgname: String = line.getString(2)
        val num: Long = line.getLong(3)
       Row(country,gpcategory,pkgname,num)

      })*/


  //读取text文件，创建rdd
  //  val textRdd: RDD[String] = sparkContext.textFile("H:\\input\\sparkInput\\input_text")
  val textRdd: RDD[String] = sparkSession.sparkContext.textFile("H:\\input\\sparkInput\\input_text")

  //将RDD[String]转换成RDD[Row]
  val rowRdd: RDD[Row] = textRdd.map((f: String) => {
    //      CN	game	cn.gameloft.aa	1
    val arr: Array[String] = f.split("\t")
    val country: String = arr(0)
    val gpcategory: String = arr(1)
    val pkgname: String = arr(2)
    val num: Long = arr(3).toLong
    // 提供了apply方法
    Row(country, gpcategory, pkgname, num)
  })

  //设置row的字段
  val fields: ArrayBuffer[StructField] = ArrayBuffer[StructField]()
  fields += new StructField("country", DataTypes.StringType, true)
  fields += new StructField("gpcategory", DataTypes.StringType, true)
  fields += new StructField("pkgname", DataTypes.StringType, true)
  fields += new StructField("num", DataTypes.LongType, true)
  val structType: StructType = StructType(fields)

  //根据rowRdd创建DataFrame
  val dataFrame: DataFrame = sparkSession.createDataFrame(rowRdd, structType)

  dataFrame.printSchema()
  dataFrame.show()

}
