import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.{HashingTF, IDF, LabeledPoint, Tokenizer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD

object UseModel1{
  private val modPath="D:\\code\\spark\\model\\spark-LR-model-travel"//"./model/spark-LR-model-travel"
  //流程代码
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("useModel").setMaster("local[2]");
    val ssc = getStreamingContext(conf, 10);
    //val Array(zkQuorum, group, topics, numThreads) =Array("192.168.10.199:2181","order","order","2");
    //val dstreams = getKafkaDstream(ssc, topics, zkQuorum, group, numThreads);
//    val dstream = dstreams.inputDStream.map(_._2);
//    dstream.persist()

    val inputDir="D:/code/dataML/iris.csv"
    //val a=ssc.fileStream()
    val lines = ssc.socketTextStream("127.0.0.1",9999, StorageLevel.MEMORY_ONLY)
    //val lines = ssc.socketTextStream("spark02",8089,StorageLevel.MEMORY_ONLY)


    ssc.start()
    ssc.awaitTermination()
  }

  //得到StreamingContext
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

  //每一个rdd做动作
  def everyRDD(rdd:RDD[String]){
    val spark = getSparkSession(rdd.context.getConf)
    import spark.implicits._
    val rddDF = rdd.map(_.split(",")).map(_.map(_.toDouble)).map{parseIris}.toDF()
    rddDF.show()

    val classifier = new LinearRegression().setFeaturesCol("features").setLabelCol("time").setFitIntercept(true).setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val NewModel = classifier.fit(rddDF)
//
//
//    val rescaledData = NewModel.transform(rddDF)
//
//    // 模型进行评价
//    val trainingSummary = NewModel.summary
//    val rmse =trainingSummary.rootMeanSquaredError
//    if (rmse <0.3) {
//      try {
//        NewModel.write.overwrite().save(modPath)
//      } catch {
//        case ex: Exception => println(ex)
//        case ex: Throwable => println("found a unknown exception" + ex)
//      }
//    }


  }

  case class Iris(
                     f1: Double,
                     f2: Double, Type:Double)

  def parseIris(line: Array[Double]): Iris = {
    Iris(
      line(0), line(1) , line(2)
    )
  }

}