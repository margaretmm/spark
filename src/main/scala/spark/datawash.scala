package spark

import java.io.StringReader
import java.sql.DriverManager

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import spark.Iris.{parseCredit, parseRDD}
import com.databricks.spark.csv._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.univocity.parsers.common._

/**
  * Created by toto on 2017/7/11.
  */
object datawash {
  case class BankData(
      age:String,job:String,marital:String,education:String,default:String,housing:String,loan:String,contact:String,month:String,day_of_week:String,
      duration:String,campaign:String,pdays:String,previous:String,poutcome:String,emp_var_rate:String,cons_price_idx:String,cons_conf_idx:String,
      euribor3m:String,nr_employed:String,y:String
  )
//  val schema = StructType(
//      StructField("age", StringType)::
//      StructField("job", StringType)::
//        StructField("marital", StringType)::
//        StructField("education", StringType)::
//        StructField("default", StringType)::
//        StructField("housing", StringType)::
//        StructField("loan", StringType)::
//        StructField("contact", StringType)::
//        StructField("month", StringType)::
//        StructField("age", StringType)::
//        StructField("day_of_week", StringType)::
//        StructField("duration", StringType)::
//        StructField("campaign", StringType)::
//        StructField("pdays", StringType)::
//        StructField("previous", StringType)::
//        StructField("poutcome", StringType)::
//        StructField("emp_var_rate", StringType)::
//        StructField("cons_price_idx", StringType)::
//        StructField("poutcome", StringType)::
//        StructField("cons_conf_idx", StringType)::
//        StructField("euribor3m", StringType)::
//        StructField("nr_employed", StringType)::
//        StructField("y", StringType)::
//  )
  def parseBankData(line: Array[String]): BankData = {
    BankData(
      line(0),
      line(1) , line(2), line(3), line(4), line(5),
      line(6) , line(7) , line(8), line(9) , line(10) ,
      line(11) , line(12), line(13), line(14), line(15),
      line(16), line(17), line(18), line(19), line(20)
    )
  }

  def parseRDD(rdd: RDD[String]): RDD[Array[String]] = {
    rdd.map(_.split(";")).map(_.map(_.toString))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()


    val filePath="D:\\code\\spark\\data\\bank-additional\\bank-additional.csv"
   // val accessRDD = spark.sparkContext.textFile(filePath)
    val df = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").//org.apache.spark.sql.execution.datasources.csv.CSVFileFormat, com.databricks.spark.csv.DefaultSource15
      option("header", "true").
      option("delimiter", ";").
      load(filePath)

    val DfOri = df.select("age","job","marital","education","default","housing")
    df.show(2)
    print("hah")
//    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
//      AccessConvertUtil.struct)
//    val result = input.map{ line =>
//        val reader = new CSVReader(new StringReader(line));
//        reader.readNext()
//    }
//    result.collect().foreach(x => {x.foreach(println);println("======")})

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

//    sc.stop()
  }

//  def dataWash(sc: SparkContext,filePath: String): DataFrame = {
//    val df = parseRDD(sc.textFile(filePath)).map(parseCredit).toDF().cache()
//
//  }


}
