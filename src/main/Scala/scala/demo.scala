package scala

/**
 * @author HuGuangtao
 * @date 2021/8/12 11:24
 * @version 1.0
 */
object demo {
  def main(args: Array[String]): Unit = {
/*
    for (x <- 1 to 10){
      println(s"$x:")
   }
      */

  //双重循环
  for(i <- 1 to 10;j <- 1 to 5)
  println(s" i:$i j:$j")

  }
}
