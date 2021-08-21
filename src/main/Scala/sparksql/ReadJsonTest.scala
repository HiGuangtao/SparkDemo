package sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 测试sparkSQL读取JSON文件 生成DataFrame
 *SparkSession 是 spark 2.X 使用，在 spark2.X 版本中
 *  SQLContext 和 HiveContext 都被SparkSession替代；
 * @author HuGuangtao
 * @date 2021/8/17 11:45
 * @version 1.0
 */
object ReadJsonTest {

  def main(args: Array[String]): Unit = {

    //读取Json文件
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReadJsonTest")

    //创建SparkContext
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    /*    //创建SqlContext 但是被弃用
        // @deprecated("Use SparkSession.builder instead", "2.0.0") 改用SparkSession
        val sqlContext: SQLContext = new SQLContext(sparkContext)
        //读取json文件，生成DataFrame
        val df: DataFrame = sqlContext.read.json("H:\\input\\sparkInput\\input_json")*/

    //弃用SQLContext，改用SparkSession 创建SparkSession对象
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //读取json文件，生成DataFrame
    val df: DataFrame = sparkSession.read.json("H:\\input\\sparkInput\\input_json")
    sparkSession.read.orc(" ")


    //打印表结构
    //    df.printSchema()

    //默认查询前20条
    //    df.show()

    df.select("country", "num").show()
    df.select(df.col("country"), df.col("num")).show()

    // select country, count(*) from xxx group by country;
    val groupByDF: DataFrame = df.groupBy("country").count()
    groupByDF.show()

    //模拟将统计结果保存在hdfs上
    val groupByRdd: RDD[Row] = groupByDF.rdd
    //   groupByRdd.foreach(print)

    //通过map 将RDD[Row] 转换成RDD[String] 用来提取数据
    val rdd2: RDD[String] = groupByRdd.map(f => {
      val country: String = f.getString(0)
      val num: Long = f.getLong(1)
      s"$country == $num"
    })

    //因为存在groupby，默认分区是200，所以重新分区为两个
    val rdd3: RDD[String] = rdd2.coalesce(2)

    //指定输出目录,如果存在就删除
    val outPath: String = "H:\\output\\sparkout\\JsonOut"

    //引入隐式函数，使得outPath字符串具有删除目录的功能
    import Util.MyPredef.String2HdfsUtil
    outPath.deleteHdfs()

    //输出到指定目录
    rdd3.saveAsTextFile(outPath)


  }

}
