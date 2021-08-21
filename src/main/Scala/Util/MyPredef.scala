package Util

/**
 * @author HuGuangtao
 * @date 2021/8/1 21:57
 * @version 1.0
 */
object MyPredef {
  implicit def String2HdfsUtil(outputDir: String): HdfsUtil = new HdfsUtil(outputDir)

}
