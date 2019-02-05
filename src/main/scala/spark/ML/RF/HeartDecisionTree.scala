package spark.ML.RF

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object HeartDecisionTree {

  case class Diag(
                   ageTeenage: Double, ageYoung: Double, ageMid: Double, ageOld: Double,  sex: Double, chestPain0: Double,chestPain1: Double,chestPain2: Double,chestPain3: Double,
                   bloodPressure:Double,serumCholestoral:Double,bloodSugar:Double,ed0:Double,ed1:Double,ed2:Double,
                   maximumHeartRate:Double,angina:Double,oldpeak:Double,slope0:Double,slope1:Double,slope2:Double,
                   ca:Double,thal0:Double,thal1:Double,thal2:Double,thal3:Double,target:Double
                 )
  //解析一行event内容,并映射为Diag类
  def parseTravel(line: Array[Double]): Diag = {
    Diag(
      line(0), line(1) , line(2) , line(3) , line(4), line(5), line(6), line(7), line(8), line(9), line(10),
      line(11), line(12), line(13),line(14), line(15), line(16), line(17),line(18), line(19), line(20),
      line(21), line(22), line(23), line(24), line(25), line(26)
    )
  }

  //RDD转换函数：解析一行文件内容从String，转变为Array[Double]类型，并过滤掉缺失数据的行
  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
    //rdd.foreach(a=> print(a+"\n"))
    val a=rdd.map(_.split(","))
    a.map(_.map(_.toDouble)).filter(_.length==27)
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("sparkIris").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data_path = "D:/code/flink-training-exercises-master/HeartDataForLR/*"
    val FareDF = parseRDD(sc.textFile(data_path)).map(parseTravel).toDF().cache()
    println(FareDF.count())

    val featureCols = Array("ageTeenage","ageYoung", "ageMid", "ageOld","sex",
      "chestPain0", "chestPain1","chestPain2","chestPain3","bloodPressure",
      "serumCholestoral","bloodSugar","ed0","ed1","ed2","maximumHeartRate",
      "angina","oldpeak","slope0","slope1","slope2","ca",
      "thal0","thal1","thal2","thal3")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(FareDF)
    df2.show


   //val featureIndexer = new VectorIndexer().setInputCol("featureCols").setOutputCol("indexedFeatures")//.fit(data)

    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3))


    //val classifier = new RandomForestClassifier()
    val classifier = new DecisionTreeClassifier()
             .setLabelCol("target")
             .setFeaturesCol("features")
             .setImpurity("gini")
             .setMaxDepth(5)

    val model = classifier.fit(trainingData)
    try {
      // model.write.overwrite().save("D:\\code\\spark\\model\\spark-LF-Occupy")
      // val sameModel = DecisionTreeClassificationModel.load("D:\\code\\spark\\model\\spark-LF-Occupy")
      val predictions = model.transform(testData)
      predictions.show()
      val evaluator = new BinaryClassificationEvaluator().setLabelCol("target").setRawPredictionCol("prediction")
      val accuracy = evaluator.evaluate(predictions)
      println("accuracy  fitting: " + accuracy)
    }catch{
      case ex: Exception =>println(ex)
      case ex: Throwable =>println("found a unknown exception"+ ex)
    }
  }
}
