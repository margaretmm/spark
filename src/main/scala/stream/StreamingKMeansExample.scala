package stream
import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingKMeansExample {

  def main(args: Array[String]) {

    val TrainPath="D:/code/flink-training-exercises-master/travalModDataTrain"
    val TestPath="D:/code/flink-training-exercises-master/travalModDataTest"

    val conf = new SparkConf().setMaster("local").setAppName("StreamingKMeansExample")
    val ssc = new StreamingContext(conf, Seconds(10.toLong))

    val trainingData = ssc.textFileStream(TrainPath).map(Vectors.parse)
    val testData = ssc.textFileStream(TestPath).map(LabeledPoint.parse)

    val numDimensions = 3
    val numClusters = 5
    val model = new StreamingKMeans()
      .setK(numClusters)
      .setDecayFactor(1.0)
      .setRandomCenters(numDimensions, 0.0)

    model.trainOn(trainingData)
    val ret= model.predictOnValues(testData.map(lp => (lp.label, lp.features)))//.print()//我们需要对每个Rdd中的每条数据进行处理储存
//    ret.foreachRDD(rdd => {
//      //每个rdd中包含的数据类型为(String,Int)
//      //我们把所有数据records定义为Iterator类型，方便我们遍历
//      def func(records:Iterator[(String,Int)]): Unit ={
//        //注意，conn和stmt定义为var不能是val
//        var conn: Connection = null
//        var stmt : PreparedStatement = null
//        try{
//          //连接数据库
//          val url = "jdbc:mysql://localhost:3306/spark" //地址+数据库
//          val user = "root"
//          val password = ""
//          conn = DriverManager.getConnection(url,user,password)
//          //
//          records.foreach(p =>{
//            //LRp为表名，word和count为要插入数据的属性
//            //插入数据
//            val sql = "insert into LRp(label,pridict) values(?,?)"
//            stmt = conn.prepareStatement(sql)
//            stmt.setString(1,p._1.trim)
//            stmt.setInt(2,p._2.toInt)
//            stmt.executeUpdate()
//          })
//        }catch {
//          case e : Exception => e.printStackTrace()
//        }finally {
//          if(stmt != null)
//            stmt.close()
//          if(conn != null)
//            conn.close()
//        }
//      }
//
//      val repairtitionedRDD = rdd.repartition(3)//将每个rdd重新分区
//      repairtitionedRDD.foreachPartition(func)//对重新分区后的rdd执行func函数
//    })

    ssc.start()
    ssc.awaitTermination()
  }
}