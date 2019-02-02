package spark

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object bucketizer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()

    // Double.NegativeInfinity：负无穷；Double.PositiveInfinity：正无穷
    // 分为6个组：[负无穷,-100),[-100,-10),[-10,0),[0,10),[10,90),[90,正无穷)
    val splits = Array(Double.NegativeInfinity, -100, -10, 0.0, 10, 90, Double.PositiveInfinity)

    val data: Array[Double] = Array(-180, -160, -100, -50, -70, -20, -8, -5, -3, 0.0, 1, 3, 7, 10, 30, 60, 90, 100, 120, 150)

    val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    //dataFrame:DataFrame = sqlContext.createDataFrame(data).toDF("features")//[features: double]

    val bucketizer = new Bucketizer()
      .setInputCol("features")
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)

    // 将原始数据转换为桶索引
    val bucketedData = bucketizer.transform(dataFrame)
    // bucketedData: org.apache.spark.sql.DataFrame =[features: double, bucketedFeatures: double]
    bucketedData.show(50, truncate = false)
  }


//  def saveASMysqlTable(dataFrame: DataFrame, tableName: String, saveMode: SaveMode, proPath: String) = {
//    var table = tableName
//    val properties: Properties = getProPerties(proPath)
//    val prop = new Properties //配置文件中的key 与 spark 中的 key 不同 所以 创建prop 按照spark 的格式 进行配置数据库
//    prop.setProperty("user", properties.getProperty("mysql.username"))
//    prop.setProperty("password", properties.getProperty("mysql.password"))
//    prop.setProperty("driver", properties.getProperty("mysql.driver"))
//    prop.setProperty("url", properties.getProperty("mysql.url"))
//    if (saveMode == SaveMode.Overwrite) {
//      var conn: Connection = null
//      try {
//        conn = DriverManager.getConnection(
//          prop.getProperty("url"),
//          prop.getProperty("user"),
//          prop.getProperty("password")
//        )
//        val stmt = conn.createStatement
//        table = table.toUpperCase
//        stmt.execute(s"truncate table $table") //在覆盖的时候不删除原来的表
//        conn.close()
//      }
//      catch {
//        case e: Exception =>
//          println("MySQL Error:")
//          e.printStackTrace()
//      }
//    }
//    dataFrame.write.mode(SaveMode.Append).jdbc(prop.getProperty("url"), table, prop)
//  }

//  def save2Mysql(tableName: String, saveMode: SaveMode, proPath: String,
//                 iterator: Iterator[(String, String, String, String, String, String, String, String, String, String,
//                   String, String, String, String, String, String, String, String, String, String)]): Unit = {
//    var table = tableName
//    val properties: Properties = getProPerties(proPath)
//    val prop = new Properties //配置文件中的key 与 spark 中的 key 不同 所以 创建prop 按照spark 的格式 进行配置数据库
//    prop.setProperty("user", properties.getProperty("mysql.username"))
//    prop.setProperty("password", properties.getProperty("mysql.password"))
//    prop.setProperty("driver", properties.getProperty("mysql.driver"))
//    prop.setProperty("url", properties.getProperty("mysql.url"))
//
//    var conn: Connection = null
//    var ps: PreparedStatement = null
//    val sql = "insert into blog(age,job,marital,education,default,housing,loan,contact,month,day_of_week,duration,campaign,pdays,previous,poutcome,emp.var.rate,cons.price.idx, cons.conf.idx,euribor3m,nr.employed,labelIsYes) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
//    try {
//      conn = DriverManager.getConnection(prop.getProperty("url"),
//        prop.getProperty("user"),
//        prop.getProperty("password"))
//      iterator.foreach(data => {
//        ps = conn.prepareStatement(sql)
//        ps.setString(1, data._1)
//        ps.setString(2, data._2)
//        ps.setString(3, data._3)
//        ps.setString(4, data._4)
//        ps.setString(5, data._5)
//        ps.setString(6, data._6)
//        ps.setString(7, data._7)
//        ps.setString(8, data._8)
//        ps.setString(9, data._9)
//        ps.setString(10, data._10)
//        ps.setString(11, data._11)
//        ps.setString(12, data._12)
//        ps.setString(13, data._13)
//        ps.setString(14, data._14)
//        ps.setString(15, data._15)
//        ps.setString(16, data._16)
//        ps.setString(17, data._17)
//        ps.setString(18, data._18)
//        ps.setString(19, data._19)
//        ps.setString(20, data._20)
//        ps.executeUpdate()
//      }
//      )
//    } catch {
//      case e: Exception => println("Mysql Exception")
//    } finally {
//      if (ps != null) {
//        ps.close()
//      }
//      if (conn != null) {
//        conn.close()
//      }
//    }
//  }

}
