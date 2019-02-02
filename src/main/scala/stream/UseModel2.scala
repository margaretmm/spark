package stream

import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UseModel2{

  //流程代码
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("useModel").setMaster("local[2]");
    val ssc = getStreamingContext(conf, 10);

    val inputDir="D:/code/flink-training-exercises-master/travalModDataT2"
    val inputDir2="D:/code/flink-training-exercises-master/travalModDataT2"

    val trainingData = ssc.textFileStream(inputDir)
    val testData = ssc.textFileStream(inputDir2)

//    val data=trainingData.flatMap(_.split(",")).map(lp => (lp., lp.features)
//    //data.print()
//
//    val numFeatures = 2
//    val model = new StreamingLinearRegressionWithSGD()
//      .setInitialWeights(Vectors.zeros(numFeatures))
//
//    model.trainOn(trainingData.map(LabeledPoint.parse))
//    val prediction= model.predictOnValues(testData.map(LabeledPoint.parse).map(lp => (lp.label, lp.features)))
//    prediction.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def countErrors(rdd: RDD[String]): RDD[(String, Int)] = {
    rdd
      .filter(_.contains("ERROR")) // Keep "ERROR" lines
      .map( s => (s.split(" ")(0), 1) ) // Return tuple with date & count
      .reduceByKey(_+_) // Sum counts for each date
  }

  def getStreamingContext(conf:SparkConf,secend:Int):StreamingContext = {
    return new StreamingContext(conf, Seconds(secend))
  }

  //得到sparkSession
  def getSparkSession(conf:SparkConf): SparkSession = {
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    return spark;
  }

}

