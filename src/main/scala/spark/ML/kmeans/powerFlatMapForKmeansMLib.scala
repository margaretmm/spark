package spark.ML.kmeans

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object powerFlatMapForKmeansMLib{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkDFebay").setMaster("local")
    val sc = new SparkContext(conf)

    val file="D:/code/flink-training-exercises-master/powerFlatMapForKmeansMLib/8"
    val rawTrainingData = sc.textFile(file)

    val RDDData = rawTrainingData.map(line => {
      Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
    }).cache()

    val Array(trainingData, testData) = RDDData.randomSplit(Array(0.7, 0.3))

//    val numClusters = 25 //分类数
//    val numIterations = 30 //迭代次数
//    val runTimes = 3 //运行次数，每次运行的分类结果是不同的
//    var clusterIndex: Int = 0
//    val clusters: KMeansModel = KMeans.train(trainingData, numClusters, numIterations, runTimes)
//    println("群数:" + clusters.clusterCenters.length)
//
//    clusters.clusterCenters.foreach(x => {
//      println("Center Point of Cluster " + clusterIndex + ":"); println(x); clusterIndex += 1;
//    }) //打印各中心点
//
//    testData.collect().foreach(testDataLine => {
//      val predictedClusterIndex: Int = clusters.predict(testDataLine)
//      println("The data " + testDataLine.toString + " belongs to cluster " + predictedClusterIndex)
//    })

    //选取最合适的分类数
    val ks: Array[Int] = Array(3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
    var  ret:Map[Int,Double] =Map()
    //    var  ret2:Array[Tuple3[Int,Double,Int]] =new Array[Tuple3[Int,Double,Int]](ks)

    ks.foreach(cluster => {
      val model: KMeansModel = KMeans.train(trainingData, cluster, 30, 1)
      val ssd = model.computeCost(trainingData)
      ret += (cluster->ssd)
      //      ret2 += new Tuple3()
    })

    ret.foreach{ i =>
      println( "Key = " + i )}
  }
}
