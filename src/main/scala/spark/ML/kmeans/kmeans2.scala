package spark.ML.kmeans

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object kmeans2 {
  val conf = new SparkConf().setAppName("SparkDFebay").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
//  case class model_instance (features: vector)
//  val rawData = sc.textFile("D:\\code\\spark\\data\\iris.txt")
//  val df = rawData.map(line =>
//    { model_instance( Vectors.dense(line.split(",").filter(p => p.matches("\\d*(\\.?)\\d*")).map(_.toDouble)) )}).toDF()
}
