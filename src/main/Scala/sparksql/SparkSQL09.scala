package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author HuGuangtao
 * @date 2021/8/17 10:09
 * @version 1.0
 */
object SparkSQL09 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSQL09").setMaster("local[*]")
    //    conf.set("spark.sql.shuffle.partitions", "2")

    // 创建sparkSession对象
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //    sparkSession.sparkContext.xxx
    import sparkSession.implicits._
    val rdd: RDD[String] = sparkSession.sparkContext.parallelize(List("a 1", "b 2", "a 3"), 2)
    val rdd2: RDD[(String, Int)] = rdd.map(f => {
      val arr: Array[String] = f.split(" ")
      (arr(0), arr(1).toInt)
    })

    //RDD 转 DataFrame：
    // 通过隐式转换函数 rddToDatasetHolder 将 rdd --> DatasetHolder[DateSet]
    // 再通过 toDF 函数，得到 DateFrame
    val df: DataFrame = rdd2.toDF("word", "num")
    df.printSchema()
    df.show()

    //    df.map(row =>{
    //      val word: String = row.getString(0)
    //      val num: Int = row.getInt(1)
    //      s"${word}\t${num}"
    //    }).show()

    //    df.map{
    //      case Row(word, num) => {
    //        s"${word}\t${num}"
    //      }
    //    }.show()
  }
}
