package spark.ML.LR

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object EnergyModLR {

  case class Energy(
                     lights: Double, T1: Double, RH_1: Double, T2: Double, RH_2: Double, T3: Double, RH_3: Double, T4: Double, RH_4: Double, T5: Double, RH_5: Double,
                     T6: Double, RH_6: Double,Press_mm_hg: Double, RH_out: Double,Windspeed: Double, Visibility: Double,Tdewpoint:Double
                   )

  def parseEnergy(line: Array[Double]): Energy = {
    Energy(
      line(0), line(1) , line(2) , line(3) , line(4) , line(5) , line(6) , line(7) , line(8) , line(9) , line(10) , line(11) , line(12), line(13), line(14)
      , line(15), line(16), line(17)
    )
  }

  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
    rdd.map(_.split(",")).map(_.map(_.toDouble)).filter(_.length==18)
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkDFEnergy").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data_path = "D:/code/flink-training-exercises-master/energyDataForLR/5"
    val EnergyDF = parseRDD(sc.textFile(data_path)).map(parseEnergy).toDF().cache()
//    creditDF.registerTempTable("Travel")
//    creditDF.printSchema
//    creditDF.show

    val featureCols = Array( "T2", "T3", "T4","T5")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(EnergyDF)
    df2.show

//    val labelIndexer = new StringIndexer().setInputCol("T1").setOutputCol("label")
//    val df3 = labelIndexer.fit(df2).transform(df2)
//    df3.show
    val splitSeed = 5043
    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), splitSeed)

    val classifier = new LinearRegression().setFeaturesCol("features").setLabelCol("T1").setFitIntercept(true).setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val model = classifier.fit(trainingData)

    // 输出模型全部参数
    model.extractParamMap()
    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")
    val predictions = model.transform(trainingData)
    predictions.selectExpr("T1", "round(prediction,1) as prediction").show

    // 模型进行评价
    val trainingSummary = model.summary
    val rmse =trainingSummary.rootMeanSquaredError
    println(s"RMSE: ${rmse}")
    println(s"r2: ${trainingSummary.r2}")

    //val predictions = model.transform(testData)
    if (rmse <0.3) {
      try {
        model.write.overwrite().save("./model/spark-LR-model-energy")

        val sameModel = LinearRegressionModel.load("./model/spark-LR-model-energy")
        val predictions= sameModel.transform(testData)

        predictions.show(3)
      } catch {
        case ex: Exception => println(ex)
        case ex: Throwable => println("found a unknown exception" + ex)
      }
    }
   }
}
