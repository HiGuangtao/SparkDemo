package sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import sparksql.MySchemaTest.{sparkConf, sparkSession, textRdd}

import scala.collection.mutable.ArrayBuffer


/**
 * 创建样例类 封装schema 测试一下通过创建var参数不通过反射获取数据 结果不行
 *
 * @param country    国家
 * @param gpcategory 种类
 * @param pkgname    包名
 * @param num        数量
 */
case class MyBean(var country: String, var gpcategory: String, var pkgname: String, var num: Long) {
  // 通过反射拿到字段名country, 拼接getCountry方法来获取数据
  def getCountry: String = this.country

  def getGpcategory: String = this.gpcategory

  def getPkgname: String = this.pkgname

  def getNum: Long = this.num
}


/**
 * 自定义bean来创建schema，用sql来代替df的api
 *
 * @author HuGuangtao
 * @date 2021/8/17 14:08
 * @version 1.0
 */
object BeanSchemaTest {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BeanSchemaTest")

    //创建sparkSession
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //读取text文件，创建rdd
    val textRdd: RDD[String] = sparkSession.sparkContext.textFile("H:\\input\\sparkInput\\input_text")

    //将RDD[String]转换成RDD[Row]
    val rowRdd: RDD[MyBean] = textRdd.map((f: String) => {
      // CN	game	cn.gameloft.aa	1
      val arr: Array[String] = f.split("\t")
      val country: String = arr(0)
      val gpcategory: String = arr(1)
      val pkgname: String = arr(2)
      val num: Long = arr(3).toLong
      // 提供了apply方法
      MyBean(country, gpcategory, pkgname, num)
    })

    val frame: DataFrame = sparkSession.createDataFrame(rowRdd, classOf[MyBean])
    //    frame.printSchema()
    //    frame.show()

    // select country, count(*) from xxx group by country
    // 通过这个方法给DataFrame数据集创建临时视图，并设置视图名称
    frame.createOrReplaceTempView("viewTable")

    // 用视图直接写sql
    val sql: String = "select country, count(*) from viewTable group by country"
    val frame1: DataFrame = sparkSession.sql(sql)
    frame1.show()


    //用DF API写
    val frame2: DataFrame = frame.groupBy("country").count()
    frame2.show()



  }
}

