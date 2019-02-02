package spark.ML.RF

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object OccupyDecisionTree {

  case class Occupy(
                   Temp: Double, Humidity: Double, Light: Double, CO2: Double, HumidityRatio: Double, Occupancy: Double
                 )

  def parseOccupy(line: Array[Double]): Occupy = {
    Occupy(
      line(0),line(1) , line(2), line(3), line(4), line(5)
    )
  }

  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
    rdd.map(_.split(",")).map(_.map(_.toDouble))
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkDFebay").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = parseRDD(sc.textFile("D:/code/flink-training-exercises-master/OccupyDataForLR/1")).map(parseOccupy).toDF().cache()

    val featureCols = Array("Temp", "Humidity", "Light", "CO2", "HumidityRatio")


    val featureIndexer = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = featureIndexer.transform(df)
    df2.show

//    val labelIndexer = new StringIndexer().setInputCol("Iris_class").setOutputCol("label")
//    val df3 = labelIndexer.fit(df2).transform(df2)
//    df3.show

    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), 5000)

    //val classifier = new RandomForestClassifier()
    val classifier = new DecisionTreeClassifier()
             .setLabelCol("Occupancy")
             .setFeaturesCol("features")
             .setImpurity("gini")
             .setMaxDepth(5)
    val model = classifier.fit(trainingData)
    try {
      // model.write.overwrite().save("D:\\code\\spark\\model\\spark-LF-Occupy")
      // val sameModel = DecisionTreeClassificationModel.load("D:\\code\\spark\\model\\spark-LF-Occupy")
       val predictions = model.transform(testData)
       predictions.show()
       val evaluator = new BinaryClassificationEvaluator().setLabelCol("Occupancy").setRawPredictionCol("prediction")
       val accuracy = evaluator.evaluate(predictions)
       println("accuracy pipeline fitting: " + accuracy)
    }catch{
      case ex: Exception =>println(ex)
      case ex: Throwable =>println("found a unknown exception"+ ex)
    }

  }
}
