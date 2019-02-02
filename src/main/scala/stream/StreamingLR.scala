package stream

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingLR {

  def main(args: Array[String]) {

    val TrainPath="D:/code/flink-training-exercises-master/travalModDataTrain"
    val TestPath="D:/code/flink-training-exercises-master/travalModDataTest"

    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamingKMeansExample")
    val ssc = new StreamingContext(conf, Seconds(10.toLong))

    val path="D:/code/flink-training-exercises-master/PowerModData"//被监控的实时流文件目录，由前面的flink服务实时生成并更新文件
    val trainingData = ssc.textFileStream(path)
    trainingData.print()
//    val trainingData = ssc.textFileStream(path).map(LabeledPoint.parse).cache()
//    val testData = ssc.textFileStream(path).map(LabeledPoint.parse)
//
//    val numFeatures = 8//特征的数量
//    val model = new StreamingLinearRegressionWithSGD()
//      .setInitialWeights(Vectors.zeros(numFeatures))
//
//    model.trainOn(trainingData)
//    val ret = model.predictOnValues(testData.map(lp => (lp.label, lp.features)))
//    ret.print()

    ssc.start()
    ssc.awaitTermination()
  }
}