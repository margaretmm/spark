package spark.ML.RF

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object IrisDecisionTree {

  case class Iris(
                   sepal_length: Float,
                   sepal_width: Float, petal_length: Float, petal_width: Float, Iris_class: Float
                 )

  def parseCredit(line: Array[Float]): Iris = {
    Iris(
      line(0),
      line(1) - 1, line(2), line(3), line(4)
    )
  }

  def parseRDD(rdd: RDD[String]): RDD[Array[Float]] = {
    rdd.map(_.split(",")).map(_.map(_.toFloat))
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkDFebay").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = parseRDD(sc.textFile("D:\\code\\spark\\data\\iris.txt")).map(parseCredit).toDF().cache()

    val featureCols = Array("sepal_length", "sepal_width", "petal_length", "petal_width", "Iris_class")


    val featureIndexer = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = featureIndexer.transform(df)
    df2.show

    val labelIndexer = new StringIndexer().setInputCol("Iris_class").setOutputCol("label")
    val df3 = labelIndexer.fit(df2).transform(df2)
    df3.show

    val Array(trainingData, testData) = df3.randomSplit(Array(0.7, 0.3), 5000)

    //val classifier = new RandomForestClassifier()
    val classifier = new DecisionTreeClassifier()
             .setLabelCol("label")
             .setFeaturesCol("features")
             .setImpurity("gini")
             .setMaxDepth(5)
    val model = classifier.fit(trainingData)
    try {
       model.write.overwrite().save("D:\\code\\spark\\model\\spark-LF-iris")
       val sameModel = DecisionTreeClassificationModel.load("D:\\code\\spark\\model\\spark-LF-iris")
       val predictions = sameModel.transform(testData)
       predictions.show()
       val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction")
       val accuracy = evaluator.evaluate(predictions)
       println("accuracy pipeline fitting: " + accuracy)
    }catch{
      case ex: Exception =>println(ex)
      case ex: Throwable =>println("found a unknown exception"+ ex)
    }




//    val paramGrid = new ParamGridBuilder()
//      .addGrid(classifier.maxBins, Array(25, 31))
//      .addGrid(classifier.maxDepth, Array(5, 10))
//     // .addGrid(classifier.numTrees, Array(20, 60))
//      .addGrid(classifier.impurity, Array("entropy", "gini"))
//      .build()
//
//    val steps: Array[PipelineStage] = Array(classifier)
//    val pipeline = new Pipeline().setStages(steps)
//
//    val cv = new CrossValidator()
//      .setEstimator(pipeline)
//      .setEvaluator(evaluator)
//      .setEstimatorParamMaps(paramGrid)
//      .setNumFolds(3)
//
//    val pipelineFittedModel = cv.fit(trainingData)
//
//    val predictions = pipelineFittedModel.transform(testData)
//    predictions.show()



    //println(pipelineFittedModel.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages(0))
  }
}
