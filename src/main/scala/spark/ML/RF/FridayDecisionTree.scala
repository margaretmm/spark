package spark.ML.RF

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import spark.ML.util.MLUtil

object FridayDecisionTree {

  case class Friday(
                   AgeLabel: Double, Male: Double, Female: Double, AgeTeenager: Double, AgeYoung1: Double, AgeYoung2: Double, AgeMid: Double, AgeOld1: Double, AgeOld2: Double, AgeOld3: Double,
                   Occupation:Double, CityA:Double, CityB:Double, CityC:Double, StayYears1:Double, StayYears2:Double, StayYears3:Double,
                   StayYearsMoreThen4:Double, Marital_Status:Double, Product_Category_1:Double, Product_Category_2:Double, PurchaseLabel:Double, Purchase:Double
                 )
  //解析一行event内容,并映射为Diag类
  def parseFriday(line: Array[Double]): Friday = {
    Friday(
      line(0), line(1) , line(2) , line(3) , line(4), line(5), line(6), line(7), line(8), line(9), line(10),
      line(11), line(12), line(13),line(14), line(15), line(16), line(17),line(18), line(19), line(20), line(21), line(22)
    )
  }

  //RDD转换函数：解析一行文件内容从String，转变为Array[Double]类型，并过滤掉缺失数据的行
  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
    //rdd.foreach(a=> print(a+"\n"))
    val a=rdd.map(_.split(","))
    a.map(_.map(_.toDouble)).filter(_.length==23)
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("sparkIris").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data_path = "D:/code/flink-training-exercises-master/FridayDataForLR/*"
    val DF = parseRDD(sc.textFile(data_path)).map(parseFriday).toDF().cache()
    println(DF.count())

//    val splits = Array(0, 20, 30, 40, 50, Double.PositiveInfinity)
//    //初始化Bucketizer对象并进行设定：setSplits是设置我们的划分依据
//    val bucketizer = new Bucketizer().setSplits(splits).setInputCol("Purchase").setOutputCol("Purchasefeature")

    val fcol=Array("Male","Female","AgeTeenager","AgeYoung1","AgeYoung2","AgeMid","AgeOld1","AgeOld2","AgeOld3",
      "Occupation","CityA","CityB","CityC","StayYears1","StayYears2","StayYears3","StayYearsMoreThen4","Marital_Status","Product_Category_1","Product_Category_2","PurchaseLabel")
    val featureCols1 = Array("Male","Female", "Occupation",
      "CityA","CityB","CityC","StayYears1","StayYears2","StayYears3",
      "StayYearsMoreThen4","Marital_Status","Product_Category_1","Product_Category_2","Purchase")
    val label1="AgeLabel"

    val featureCols2 = Array("Male","Female","CityA","CityB","CityC", "Product_Category_1","Product_Category_2")
    val label2="PurchaseLabel"

    val mlUtil= new MLUtil()
    //mlUtil.DtClassifyMulti(DF,fcol,label1)
    mlUtil.DtClassifyMultiSelectFeature(DF,fcol,label1,8,"D:\\code\\spark\\model\\spark-DT-Friday")
    //mlUtil.DtClassifyBinary(DF,featureCols4,label4)
    //mlUtil.linearRegression(DF,fcol,label2,"./model/spark-LR-model-friday",9)
  }
}
