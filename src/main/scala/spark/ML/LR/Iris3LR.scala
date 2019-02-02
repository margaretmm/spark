package spark.ML.LR

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object  Iris3LR extends App{

    val conf = new SparkConf().setAppName("IrisLogisticModelTest")
      .setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // load data
    val rddIris = sc.textFile("D:\\code\\spark\\data\\iris.txt")
    //rddIris.foreach(println)

    case class Iris(a:Double, b:Double, c:Double, d:Double, target:Double)

    //LabeledPoint中的label即target列必须是double类型，从0.0开始，两类就是0.0和1.0
    val dfIris = rddIris.map(_.split(",")).map(l => Iris(l(0).toDouble,l(1).toDouble,l(2).toDouble,l(3).toDouble,l(4).toDouble)).toDF()

    dfIris.registerTempTable("Iris")

    //sqlContext.sql("""SELECT * FROM Iris""").show

    // Map feature names to indices
    val featInd = List("a", "b", "c", "d").map(dfIris.columns.indexOf(_))

    // Get index of target
    val targetInd = dfIris.columns.indexOf("target")

    val labeledPointIris = dfIris.rdd.map(r => LabeledPoint(
      r.getDouble(targetInd), // Get target value
      // Map feature indices to values
      Vectors.dense(featInd.map(r.getDouble(_)).toArray)))

    // Split data into training (80%) and test (20%).
    val splits = labeledPointIris.randomSplit(Array(0.8, 0.2), seed = 11L)
    val trainingData = splits(0)
    val testData = splits(1)
    /*println("trainingData--------------------------->")
    trainingData.take(5).foreach(println)
    println("testData------------------------------->")
    testData.take(5).foreach(println)*/


    /*// Run training algorithm to build the model
    val lr = new LogisticRegressionWithSGD().setIntercept(true)
    lr.optimizer
      .setStepSize(10.0)
      .setRegParam(0.0)
      .setNumIterations(20)
      .setConvergenceTol(0.0005)
    val model = lr.run(trainingData)*/
    val numiteartor = 2
    //val model = LogisticRegressionWithSGD.train(trainingData, numiteartor)
    val model = new LogisticRegressionWithLBFGS().setNumClasses(numiteartor).run(trainingData)

    //预测
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)

    }
    println("labelAndPreds------------------------->")
    labelAndPreds.take(5).foreach(println)
    //计算准确率
    val metrics = new MulticlassMetrics(labelAndPreds)
    val precision = metrics.precision
    println("Precision = " + precision)


}
