package stream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by MANGOCOOL on 2016/09/30.
  */
object KMeansTest {

  System.setProperty("hadoop.home.dir", "E:\\Program Files\\hadoop-2.7.0")

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("SparkDFebay").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val file="D:/code/flink-training-exercises-master/PowerModDataForKmeans/1"
   //   val rawData = sc.textFile("D:/code/flink-training-exercises-master/PowerModDataForKmeans/1")
    val df = spark.read.format("libsvm").load(file)


    // create the trainer and set its parameters
    val kmeans = new KMeans().setK(12).setSeed(1L)
    //聚类模型
    val model = kmeans.fit(df)
    // 预测 即分配聚类中心
    model.transform(df).show(false)
    //聚类中心
    model.clusterCenters.foreach(println)
    // SSE误差平方和
    println("SSE:"+model.computeCost(df))

    //Step 6

  }
}
