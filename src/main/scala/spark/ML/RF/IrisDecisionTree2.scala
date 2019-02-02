package spark.ML.RF

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object IrisDecisionTree2 {


  case class Iris(features: Vector, label: String)




  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("sparkIris").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val data = sc.textFile("D:\\code\\spark\\data\\irisStr.txt")
            .map(_.split(","))
            .map(p => Iris(Vectors.dense(p(2).toDouble, p(3).toDouble), p(4).toString()))
            .toDF()
    data.show(20)

    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel")//.fit(data)

    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures")//.fit(data)

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    val dt = new DecisionTreeClassifier()
             .setLabelCol("indexedLabel")
             .setFeaturesCol("indexedFeatures")
             .setImpurity("gini")
             .setMaxDepth(5)
    //val classifier = new RandomForestClassifier()
    val classifier = new DecisionTreeClassifier()
             .setLabelCol("label")
             .setFeaturesCol("features")
             .setImpurity("gini")
             .setMaxDepth(5)

//    val labelConverter = new IndexToString().
//             setInputCol("prediction").
//      setOutputCol("predictedLabel").
//      setLabels(labelIndexer.labels)
//
//    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction")
//
////    val paramGrid = new ParamGridBuilder()
////      .addGrid(classifier.maxBins, Array(25, 31))
////      .addGrid(classifier.maxDepth, Array(5, 10))
////     // .addGrid(classifier.numTrees, Array(20, 60))
////      .addGrid(classifier.impurity, Array("entropy", "gini"))
////      .build()
////
////    val steps: Array[PipelineStage] = Array(classifier)
////    val pipeline = new Pipeline().setStages(steps)
////
////    val cv = new CrossValidator()
////      .setEstimator(pipeline)
////      .setEvaluator(evaluator)
////      .setEstimatorParamMaps(paramGrid)
////      .setNumFolds(3)
////
//    val pipeline = new Pipeline().
//         setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))
//    val model = pipeline.fit(trainingData)
//    val predictions = model.transform(testData)
////
////    val predictions = pipelineFittedModel.transform(testData)
//    predictions.show()
//
//    val accuracy = evaluator.evaluate(predictions)
//    println("accuracy pipeline fitting: " + accuracy)

    //println(pipelineFittedModel.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages(0))
  }
}
