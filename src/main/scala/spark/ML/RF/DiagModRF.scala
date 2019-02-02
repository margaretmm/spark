package spark.ML.RF

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DiagModRF{
  //定义文件读出格式类
  case class Diag(
                     Temp: Double,  what: Double, nausea: Double, Lumbar_pain:Double,Urine_pushing:Double,
                     Micturition_pains:Double,Burning_urethra:Double,decision:Double
                   )
  //解析一行event内容,并映射为Diag类
  def parseTravel(line: Array[Double]): Diag = {
    Diag(
      line(0), line(1) , line(2) , line(3) , line(4) , line(5) , line(6), line(7)
    )
  }

  //RDD转换函数：解析一行文件内容从String，转变为Array[Double]类型，并过滤掉缺失数据的行
  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
    rdd.map(_.split(",")).map(_.map(_.toDouble)).filter(_.length==8)
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkDFebay").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //读文件并做简单转换
    val data_path = "D:/code/flink-training-exercises-master/diagDataForRF/all.txt"
    val diagDF = parseRDD(sc.textFile(data_path)).map(parseTravel).toDF().cache()
//    diagDF.printSchema
    //diagDF.show

    //向量化处理---特征列向量化
    val featureCols = Array("Temp", "what", "nausea","Lumbar_pain","Urine_pushing","Micturition_pains","Burning_urethra")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(diagDF)

    //向量化处理---Label向量化
    val labelIndexer = new StringIndexer().setInputCol("decision").setOutputCol("label")
    val df3 = labelIndexer.fit(df2).transform(df2)
    //df3.show(40)
    //数据集拆分为训练集，测试集
    val splitSeed = 5043
    val Array(trainingData, testData) = df3.randomSplit(Array(0.7, 0.3), splitSeed)
    //定义算法分类器，并训练模型
    val classifier = new RandomForestClassifier()
    //定义评估器---多元分类评估（label列与Prediction列对比结果）
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction")
    //定义算法变量调试范围
    val paramGrid = new ParamGridBuilder()
      .addGrid(classifier.maxBins, Array(25, 31))
      .addGrid(classifier.maxDepth, Array(5, 10))
      .addGrid(classifier.numTrees, Array(2, 8))
      .addGrid(classifier.impurity, Array("entropy", "gini"))
      .build()
    //定义算法pipeline ，stage集合
    val steps: Array[PipelineStage] = Array(classifier)
    val pipeline = new Pipeline().setStages(steps)
    //定义交叉验证器CrossValidator
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)

    //pipeline执行，开始训练模型
    val pipelineFittedModel = cv.fit(trainingData)
    //对测试数据做预测
    val predictions = pipelineFittedModel.transform(testData)
    predictions.show(40)
    val accuracy = evaluator.evaluate(predictions)
    println("accuracy pipeline fitting:" + accuracy)

    println(pipelineFittedModel.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages(0))
    if (accuracy >0.8) {
      try {
        pipelineFittedModel.write.overwrite().save("./model/spark-LR-model-diag")

//        val sameModel = RandomForestModel.load("./model/spark-LR-model-diag")
//        val predictions= sameModel.transform(testData)
//
//        predictions.show(3)
      } catch {
        case ex: Exception => println(ex)
        case ex: Throwable => println("found a unknown exception" + ex)
      }
    }
   }
}
