package spark.ML.LR

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import spark.ML.RF.DiagModRF.Diag

object TestModLR {

  case class Diag(
                   age: Double,  sex: Double, cp: Double, trestbps:Double,chol:Double,fbs:Double,restecg:Double,
                   thalach:Double,exang:Double,oldpeak:Double,slope:Double,ca:Double,thal:Double,target:Double
                 )
  //解析一行event内容,并映射为Diag类
  def parseTravel(line: Array[Double]): Diag = {
    Diag(
      line(0), line(1) , line(2) , line(3) , line(4), line(5), line(6), line(7), line(8), line(9), line(10), line(11), line(12), line(13)
    )
  }


  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
    rdd.map(_.split(",")).map(_.map(_.toDouble)).filter(_.length==14)
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkDFEnergy").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data_path = "D:/code/dataML/heart.csv"
    val FareDF = parseRDD(sc.textFile(data_path)).map(parseTravel).toDF().cache()
    println(FareDF.count())
    //    creditDF.registerTempTable("Travel")
//    creditDF.printSchema
//    creditDF.show

    val featureCols = Array("age","sex","cp","trestbps","chol","fbs","restecg","thalach","exang","oldpeak","exang","slope","ca","thal")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(FareDF)
    df2.show

//    val labelIndexer = new StringIndexer().setInputCol("T1").setOutputCol("label")
//    val df3 = labelIndexer.fit(df2).transform(df2)
//    df3.show
    val splitSeed = 5043
    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), splitSeed)

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
      println("accuracy pipeline fitting: " + accuracy)
    }catch{
      case ex: Exception =>println(ex)
      case ex: Throwable =>println("found a unknown exception"+ ex)
    }
  }

}
