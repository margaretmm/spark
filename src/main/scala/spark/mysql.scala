package spark

import java.io.StringReader
import java.sql.DriverManager

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import spark.Iris.{parseCredit, parseRDD}
import com.databricks.spark.csv._
/**
  * Created by toto on 2017/7/11.
  */
object JdbcRDDDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JdbcRDDDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val filePath="D:\\code\\spark\\data\\bank-additional\\bank-additional.csv"
    val input =sc.textFile(filePath)
    val result = input.map{ line =>
        val reader = new CSVReader(new StringReader(line));
        reader.readNext()
    }
    result.collect().foreach(x => {x.foreach(println);println("======")})

//    //定义数据库和表信息
//    //val url = "jdbc:mysql://x.x.x.x:3306/bankA?useUnicode=true&characterEncoding=UTF-8"
//    val table = args(1)//"marketing_dateX"
//    val sql="(select * from "+table +" ) as mydata"
//
//    //定义mysql信息
//    val jdbcDF = sqlContext.read.format("jdbc").options(
//      Map("url"->"jdbc:mysql://localhost:3306/db_ldjs",
//        "dbtable"->sql,
//        "driver"->"com.mysql.jdbc.Driver",
//        "user"-> "root",
//        //"partitionColumn"->"day_id",
//        "lowerBound"->"0",
//        "upperBound"-> "1000",
//        "fetchSize"->"100",
//        "password"->"123456")).load()
//
//    jdbcDF.show(20)

    sc.stop()
  }

//  def dataWash(sc: SparkContext,filePath: String): DataFrame = {
//    val df = parseRDD(sc.textFile(filePath)).map(parseCredit).toDF().cache()
//
//  }


}
