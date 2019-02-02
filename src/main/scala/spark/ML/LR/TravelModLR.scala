package spark.ML.LR

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object TravelModLR {

  case class Travel(
                     time: Double,
                     distance: Double, direction: Double
                   )

  def parseTravel(line: Array[Double]): Travel = {
    Travel(
      line(0),
      line(1) , line(2)
    )
  }

  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
    rdd.map(_.split(",")).map(_.map(_.toDouble)).filter(_.length==3)
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkDFebay").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data_path = "./data/modData.csv/1.csv"
    val creditDF = parseRDD(sc.textFile(data_path)).map(parseTravel).toDF().cache()
    creditDF.registerTempTable("Travel")
    creditDF.printSchema

    creditDF.show

    val featureCols = Array("time", "distance", "direction")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(creditDF)
    df2.show

    val labelIndexer = new StringIndexer().setInputCol("time").setOutputCol("label")
    val df3 = labelIndexer.fit(df2).transform(df2)
    df3.show
    val splitSeed = 5043
    val Array(trainingData, testData) = df3.randomSplit(Array(0.7, 0.3), splitSeed)

    val classifier = new LinearRegression().setFeaturesCol("features").setLabelCol("time").setFitIntercept(true).setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val model = classifier.fit(trainingData)

    // 输出模型全部参数
    model.extractParamMap()
    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

    // 模型进行评价
    val trainingSummary = model.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    //val predictions = model.transform(testData)
    val rmse =trainingSummary.rootMeanSquaredError
    if (rmse <0.3) {
      try {
        model.write.overwrite().save("./model/spark-LR-model-travel")

        val sameModel = LinearRegressionModel.load("./model/spark-LR-model-travel")
        val predictions= sameModel.transform(testData)

        predictions.show(3)
      } catch {
        case ex: Exception => println(ex)
        case ex: Throwable => println("found a unknown exception" + ex)
      }
    }


   }
}
