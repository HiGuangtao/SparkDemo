package Util


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


/**
 * @author HuGuangtao
 * @date 2021/8/1 21:50
 * @version 1.0
 */
class HdfsUtil(val outPutDir: String) {

  def deleteHdfs(): Unit = {
    val OutPutPath: Path = new Path(outPutDir)

    //主动删除目录
    val hadoopConf = new Configuration()
    val fs: FileSystem = FileSystem.get(hadoopConf)
    if (fs.exists(OutPutPath))
      fs.delete(OutPutPath, true)
    println(s"$outPutDir has been deleted !")


  }

}
