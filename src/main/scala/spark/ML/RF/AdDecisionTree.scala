package spark.ML.RF

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import spark.ML.util.MLUtil

object AdDecisionTree {

  case class Credit(
                   TimeScaler: Double,
                   V1: Double, V2: Double, V3: Double, V4: Double, V5: Double, V6: Double, V7: Double, V8: Double, V9: Double, V10:Double,
                   V11:Double, V12:Double, V13:Double, V14:Double, V15:Double, V16:Double, V17:Double, V18:Double, V19:Double, V20:Double,
                   V21:Double, V22:Double,V23:Double, V24:Double, V25:Double, V26:Double, V27:Double, V28:Double,
                   AmountScaler:Double, Class:Double

                 )
  //解析一行event内容,并映射为Diag类
  def parseFriday(line: Array[Double]): Credit = {
    Credit(
      line(0),
      line(1) , line(2) , line(3) , line(4), line(5), line(6), line(7), line(8), line(9), line(10),
      line(11), line(12), line(13),line(14), line(15), line(16), line(17),line(18), line(19), line(20),
      line(21), line(22),line(23),line(24),line(25),line(26),line(27),line(28),
      line(29),line(30)
    )
  }

  //RDD转换函数：解析一行文件内容从String，转变为Array[Double]类型，并过滤掉缺失数据的行
  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
    //rdd.foreach(a=> print(a+"\n"))
    val a=rdd.map(_.split(","))
    a.map(_.map(_.toDouble)).filter(_.length==31)
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("sparkIris").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data_path = "D:/code/flink-training-exercises-master/CreditDataNonFraudForLR/*"
    val DF = parseRDD(sc.textFile(data_path)).map(parseFriday).toDF().cache()
    val data_path2 = "D:/code/flink-training-exercises-master/CreditDataFraudForLR/*"
    val DFFraud = parseRDD(sc.textFile(data_path2)).map(parseFriday).toDF().cache()
    println(DF.count())

//    val DFFraudCount=new BigDecimal(DFFraud.count())
//    val DFNonFraudCount = new BigDecimal(DF.count())
//    val rate=DFFraudCount.divide(DFNonFraudCount,5)
    //val DFNonFraud=DF.sample(false,rate.longValue())
    val DFNonFraud=DF.limit(DFFraud.count().toInt)
    val df2=DFFraud.union(DFNonFraud)
    println(DFNonFraud.count())
    println(df2.count())
    println(df2.filter(df2("Class")===0).show)

    val featureCols1 = Array("TimeScaler",
      "V1","V2","V3","V4","V5","V6","V7","V8","V9","V10",
      "V11","V12","V13","V14","V15","V16","V17","V18","V19","V20",
      "V21","V22","V23","V24","V25","V26","V27","V28",
      "AmountScaler")
    val label1="Class"

    val mlUtil= new MLUtil()
    //mlUtil.DtClassifyMulti(DF,featureCols1,label1)
    //mlUtil.DtClassifyBinary(DF,featureCols4,label4)
    mlUtil.DtClassifyBinary(df2,featureCols1,label1)
  }
}
