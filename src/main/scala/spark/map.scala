package spark

import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.SparkSession

object map {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()
    val sites = Map("runoob" -> "http://www.runoob.com",
      "baidu" -> "http://www.baidu.com",
      "taobao" -> "http://www.taobao.com")

    sites.foreach(e => print(e + "!!!\t"))
    val sites1 = sites.map(_._1)
    sites1.foreach(e => print(e + ","))
    print("\r\n")
    val sites2 = sites.map(_._2)
    sites2.foreach(e => print(e + "--------\t"))
    print("\r\n")

    val nums = List(1, 2, 3)
    val squareNums2 = nums.map(math.pow(_, 3))
    squareNums2.foreach(e => print(e + "##########\t"))
    print("\r\n")

  }
}
