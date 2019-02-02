package spark.ML.RF

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils

object JavaRandomForestClassificationExample {
  def main(args: Array[String]): Unit = { // $example on$
    val sparkConf = new SparkConf().setAppName("JavaRandomForestClassificationExample")
    val jsc = new JavaSparkContext(sparkConf)
    // Load and parse the data file.
    val datapath = "data/mllib/sample_libsvm_data.txt"
    val data = MLUtils.loadLibSVMFile(jsc.sc, datapath).toJavaRDD
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array[Double](0.7, 0.3))
    val trainingData = splits(0)
    val testData = splits(1)
    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = new util.HashMap[Integer, Integer]
    val numTrees = 3
    // Use more in practice.
    val featureSubsetStrategy = "auto"
    // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32
    val seed = 12345
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed)
    // Evaluate model on test instances and compute test error
//    val predictionAndLabel = testData.mapToPair((p: LabeledPoint) => new Tuple2[Double, Double](model.predict(p.features), p.label))
//    val testErr = predictionAndLabel.filter((pl: Tuple2[Double, Double]) => !(pl._1 == pl._2)).count / testData.count.toDouble
//    System.out.println("Test Error: " + testErr)
//    System.out.println("Learned classification forest model:\n" + model.toDebugString)
//    // Save and load model
//    model.save(jsc.sc, "target/tmp/myRandomForestClassificationModel")
//    val sameModel = RandomForestModel.load(jsc.sc, "target/tmp/myRandomForestClassificationModel")
//    // $example off$
//    jsc.stop()
  }
}
