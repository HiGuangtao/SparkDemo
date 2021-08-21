package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 用SparkSession 实现 rdd\dataframe\dataset间转换
 * @author HuGuangtao
 * @date 2021/8/17 9:43
 * @version 1.0
 **/
object SparkSql94 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("translate").setMaster("local[*]")

    //创建SparkSession对象
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //引入隐式函数，使得rdd具有转换的功能
    import session.implicits._

    //获取源rdd
    val rdd1: RDD[String] = session.sparkContext.parallelize(List("a 1", "b 2", "a 3"), 2)

    //rdd转换成DataFrame
    val rdd2: RDD[(String, Int)] = rdd1.map(f => {
      val strings: Array[String] = f.split(" ")
      val key: String = strings(0)
      val value: Int = strings(1).toInt
      (key, value)
    })

    // implicit def localSeqToDatasetHolder[T : Encoder](s: Seq[T]): DatasetHolder[T]
    val df1: DataFrame = rdd2.toDF("word", "num")
    df1.printSchema()
    df1.show()

    //获取每行数据
    df1.map(row => {
      val word: String = row.getString(0)
      val num: Int = row.getInt(1)
      s"${word}\t${num}"
    }).show()

    //模式匹配方式获取
    df1.map {
      case Row(word, num) => {
        s"${word}\t${num}"
      }
    }.show()

    //foreach遍历
    df1.foreach(row => {
      val word: String = row.getString(0)
      val num: Int = row.getInt(1)
      println(s"${word}\t${num}")
    })

    //rdd转DataSet
    val ds: Dataset[(String, Int)] = rdd2.toDS()

  //将Dataset[(String, Int)] 转换成 Dataset[ColClass]类型
    val ds2: Dataset[ColClass] = ds.map(f => ColClass(f._1, f._2))
    ds2.printSchema()

    val ds3: Dataset[String] = ds2.map {
      case ColClass(word, num) => {s"${word}\t${num}"}
    }
    ds3.printSchema()

  }


}

case class ColClass(val word: String, val num: Int)

