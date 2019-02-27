package spark.ML.util

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, Evaluator, MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{ChiSqSelector, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.DataFrame

class MLUtil {
  def DtClassifyBinary(df :DataFrame,featureCols1:Array[String],label :String ): Unit ={
    val assembler = new VectorAssembler().setInputCols(featureCols1).setOutputCol("features")
    val df2 = assembler.transform(df)
    df2.show

    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3))
    //val classifier = new RandomForestClassifier()
    val classifier = new DecisionTreeClassifier()
      .setLabelCol(label)
      .setFeaturesCol("features")
      .setImpurity("gini")
      .setMaxDepth(5)

    val model = classifier.fit(trainingData)
    try {
      // model.write.overwrite().save("D:\\code\\spark\\model\\spark-LF-Occupy")
      // val sameModel = DecisionTreeClassificationModel.load("D:\\code\\spark\\model\\spark-LF-Occupy")
      val predictions = model.transform(testData)
      predictions.show()
      val evaluator = new BinaryClassificationEvaluator().setLabelCol(label).setRawPredictionCol("prediction")
      val accuracy = evaluator.evaluate(predictions)
      println("accuracy  fitting: " + accuracy)
    }catch{
      case ex: Exception =>println(ex)
      case ex: Throwable =>println("found a unknown exception"+ ex)
    }
  }

  def DtClassifyMulti(df :DataFrame,featureCols1:Array[String],label :String ): Unit ={
    val assembler = new VectorAssembler().setInputCols(featureCols1).setOutputCol("features")
    val df2 = assembler.transform(df)
    df2.show

    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3))
    //val classifier = new RandomForestClassifier()
    val classifier = new DecisionTreeClassifier()
      .setLabelCol(label)
      .setFeaturesCol("features")
      .setImpurity("gini")
      .setMaxDepth(5)

    val model = classifier.fit(trainingData)
    try {
      // model.write.overwrite().save("D:\\code\\spark\\model\\spark-LF-Occupy")
      // val sameModel = DecisionTreeClassificationModel.load("D:\\code\\spark\\model\\spark-LF-Occupy")
      val predictions = model.transform(testData)
      predictions.show()
      val evaluator = new MulticlassClassificationEvaluator().setLabelCol(label).setPredictionCol("prediction")
      val accuracy = evaluator.evaluate(predictions)
      println("accuracy  fitting: " + accuracy)
    }catch{
      case ex: Exception =>println(ex)
      case ex: Throwable =>println("found a unknown exception"+ ex)
    }
  }

  def DtClassifyMultiSelectFeature(df :DataFrame,featureCols1:Array[String],label :String ,featureSelectNum:Int,ModPath:String): Unit ={
    val assembler = new VectorAssembler().setInputCols(featureCols1).setOutputCol("features")
    val df2 = assembler.transform(df)
    df2.show

    val selector = new ChiSqSelector()
      .setNumTopFeatures(featureSelectNum)
      .setFeaturesCol("features")
      .setLabelCol(label)
      .setOutputCol("selectedFeatures")

    val result = selector.fit(df2).transform(df2)
    result.show()

    val Array(trainingData, testData) = result.randomSplit(Array(0.7, 0.3))
    //val classifier = new RandomForestClassifier()
    val classifier = new DecisionTreeClassifier()
      .setLabelCol(label)
      .setFeaturesCol("selectedFeatures")

    //val model = classifier.fit(trainingData)
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol(label).setPredictionCol("prediction")

    //定义算法变量调试范围
    val paramGrid = new ParamGridBuilder()
      .addGrid(classifier.maxBins, Array(8,16, 31))//3.最大分支数
      .addGrid(classifier.maxDepth, Array(5, 10))//#2.深度
      .addGrid(classifier.impurity, Array("entropy", "gini"))//#1.不纯度度量
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

    val accuracy = evaluator.evaluate(predictions)
    println("（SelectFeature）accuracy  fitting: " + accuracy)
    if (0.9 < accuracy) {
      try {
         pipelineFittedModel.write.overwrite().save(ModPath)
         //model.write.overwrite().save("D:\\code\\spark\\model\\spark-LF-Occupy")
         //val sameModel = PipelineModel.load(ModPath)
      } catch {
        case ex: Exception => println(ex)
        case ex: Throwable => println("found a unknown exception" + ex)
      }
    }
  }

  def linearRegression(df:DataFrame,featureCols:Array[String], label:String,modPath:String,featureSelectNum:Int): Unit ={

    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(df)
    df2.show

//    val selector = new ChiSqSelector()
//      .setNumTopFeatures(featureSelectNum)
//      .setFeaturesCol("features")
//      .setLabelCol(label)
//      .setOutputCol("selectedFeatures")
//
//    val result = selector.fit(df2).transform(df2)
//    result.show()

//    val scaler =new StandardScaler().setInputCol("selectedFeatures").setOutputCol("selectedFeaturesStandard").setWithMean(false).setWithStd(true)
//    // 计算均值方差等参数
//    val scalermodel = scaler.fit(result)
//    // 标准化
//    val df3=scalermodel.transform(result)
//    df3.show()

    val splitSeed = 5043
    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), splitSeed)

    val classifier = new LinearRegression().setFeaturesCol("features").setLabelCol(label).setFitIntercept(true).setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val model = classifier.fit(trainingData)

    // 输出模型全部参数
    model.extractParamMap()
    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")
    val predictions = model.transform(trainingData)
    predictions.selectExpr(label, "round(prediction,1) as prediction").show

    // 模型进行评价
    val trainingSummary = model.summary
    val rmse =trainingSummary.rootMeanSquaredError
    println(s"RMSE: ${rmse}")
    println(s"r2: ${trainingSummary.r2}")

    //val predictions = model.transform(testData)
    if (rmse <0.3) {
      try {
        model.write.overwrite().save(modPath)//"./model/spark-LR-model-energy")

//        val sameModel = LinearRegressionModel.load(modPath)//"./model/spark-LR-model-energy")
//        val predictions= sameModel.transform(testData)
//
//        predictions.show(3)
      } catch {
        case ex: Exception => println(ex)
        case ex: Throwable => println("found a unknown exception" + ex)
      }
    }
  }

  def RFClassifyMulti(df:DataFrame,featureCols:Array[String], label:String,modPath:String): Unit ={
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(df)

    //向量化处理---Label向量化
//    val labelIndexer = new StringIndexer().setInputCol("decision").setOutputCol("label")
//    val df3 = labelIndexer.fit(df2).transform(df2)

    val splitSeed = 5043
    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), splitSeed)
    //定义算法分类器，并训练模型
    val classifier = new RandomForestClassifier().setFeaturesCol("features").setLabelCol(label)
    //定义评估器---多元分类评估（label列与Prediction列对比结果）
    //val evaluator:Evaluator=null

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol(label).setPredictionCol("prediction")

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
        pipelineFittedModel.write.overwrite().save(modPath)//"./model/spark-LR-model-diag")

        val sameModel = PipelineModel.load(modPath)
        val predictions= sameModel.transform(testData)
        predictions.show(3)
      } catch {
        case ex: Exception => println(ex)
        case ex: Throwable => println("found a unknown exception" + ex)
      }
    }
  }

  def RFClassifyBinary(df:DataFrame,featureCols:Array[String], label:String,modPath:String): Unit ={
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(df)

    //向量化处理---Label向量化
    //    val labelIndexer = new StringIndexer().setInputCol("decision").setOutputCol("label")
    //    val df3 = labelIndexer.fit(df2).transform(df2)

    val splitSeed = 5043
    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), splitSeed)
    //定义算法分类器，并训练模型
    val classifier = new RandomForestClassifier().setFeaturesCol("features").setLabelCol(label)
    //定义评估器---多元分类评估（label列与Prediction列对比结果）
    //val evaluator:Evaluator=null

    val evaluator = new BinaryClassificationEvaluator().setLabelCol(label).setRawPredictionCol("prediction")


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
        pipelineFittedModel.write.overwrite().save(modPath)//"./model/spark-LR-model-diag")
//        val sameModel = PipelineModel.load(modPath)
//        val predictions= sameModel.transform(testData)
      } catch {
        case ex: Exception => println(ex)
        case ex: Throwable => println("found a unknown exception" + ex)
      }
    }
  }

  def RFReg(df:DataFrame,featureCols:Array[String], label:String,modPath:String): Unit ={
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(df)

    //向量化处理---Label向量化
    //    val labelIndexer = new StringIndexer().setInputCol("decision").setOutputCol("label")
    //    val df3 = labelIndexer.fit(df2).transform(df2)

    val splitSeed = 5043
    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), splitSeed)
    //定义算法分类器，并训练模型
    val classifier =new RandomForestRegressor().setFeaturesCol("features").setLabelCol(label)
    //定义评估器---多元分类评估（label列与Prediction列对比结果）
    //val evaluator:Evaluator=null
    val evaluator = new RegressionEvaluator().setLabelCol(label).setPredictionCol("prediction").setMetricName("rmse")

    //定义算法变量调试范围
    val paramGrid = new ParamGridBuilder()
      .addGrid(classifier.maxBins, Array(25, 31))
      .addGrid(classifier.maxDepth, Array(5, 10))
      .addGrid(classifier.numTrees, Array(2, 8))
      .addGrid(classifier.impurity, Array("variance"))
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
        pipelineFittedModel.write.overwrite().save(modPath)//"./model/spark-LR-model-diag")

//        val sameModel = PipelineModel.load("./model/spark-LR-model-diag")
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
