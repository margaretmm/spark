package spark

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark
/**
  * Created by toto on 2017/7/11.
  */
object es {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("estest").setMaster("local[*]")
    val date=new SimpleDateFormat("yyyy_MM_dd").format(new Date)
    val elasticIndex: String = "bank_market/"+date
    conf.set("es.nodes", "127.0.0.1")
    conf.set("es.port","9200")
    conf.set("es.query", " {  \"query\": {    \"match_all\": {    }  }}")
    val sc = new SparkContext(conf)

    val rdd = EsSpark.esJsonRDD(sc,elasticIndex)
    //rdd.foreach( line => println(line._1+"\n"+line._2) )
    val newRdd = rdd.map( row => (row._2) )
    newRdd.foreach( line => println(line) )


    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json(newRdd)
    df.show()

    val featureCols = Array("age","job")
    val Df2 = df.select("age","job").show()
//    val accuracy=0.9392
//    import java.text.SimpleDateFormat
//    val now = new  Date( )
//    val sdf = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss")
//    val ModelInfo =Map("modelName" -> sdf.format(now),"version" -> "v1.1" ,"accuracy"->accuracy ,"url"-> "/home/model/bankMarket")
//    var rdd = sc.makeRDD(Seq(ModelInfo))
//    EsSpark.saveToEs(rdd,"bank_market_model/rf")



    sc.stop()
  }

}
