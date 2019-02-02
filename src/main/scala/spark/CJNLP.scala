package spark

import org.apache.spark.{SparkConf, SparkContext}

object CJNLP  {

  def main(args: Array[String]): Unit = { // $example on

    val conf =new SparkConf().setAppName("SparkFromES").setMaster("local[2]")
    val sc =new SparkContext(conf)

    val path = "D:\\news\\*"
    val rdd = sc.wholeTextFiles(path)//读取所有文件
    // count the number of records in the dataset
    //println(rdd.count)

    //下面分析20 newsgroup数据
    val text = rdd.map { case (file, text) => text }
    println("文件个数："+text.count)

    val newsgroups = rdd.map{ case (file, text) =>
      file.split("/").takeRight(2).head
      //文件的全路径类似于/home/chenjie/20news-bydate-train/alt.atheism/49960
      //将路径按/分开，并取右边的两个，再取右边两个中的第一个，即取倒数第二个，为该文件对应的新闻主题分类
    }

    //去掉以下查看
    val countByGroup = newsgroups.map{n => (n,1)}.reduceByKey(_ + _).collect().sortBy(- _._2).mkString("\n")
    //将新闻主题按主题统计个数并按从大到小排序
    //println(countByGroup)


    //2、应用基本的分词方法

//    val whiteSpaceSplit = text.flatMap(t => t.split(" ").map(_.toLowerCase()))
//    //应用基本的分词方法：空格分词，并把每个文档的所有单词变成小写
//    println(whiteSpaceSplit.distinct.count)//402978
//    //查看分词之后不同单词的数量
//
//    println(whiteSpaceSplit.sample(true, 0.3, 42).take(100).mkString(","))

//    // split text on any non-word tokens
    val nonWordSplit = text.flatMap(t => t.split("""\W+""").map(_.toLowerCase))
//    println(nonWordSplit.distinct.count)
//    // inspect a look at a sample of tokens
//    println(nonWordSplit.distinct.sample(true, 0.3, 42).take(100).mkString(","))


    // filter out numbers
    val regex = """[^0-9]*""".r
    val filterNumbers = nonWordSplit.filter(token => regex.pattern.matcher(token).matches)
//    println(filterNumbers.distinct.count)
//    println(filterNumbers.distinct.sample(true, 0.3, 42).take(100).mkString(","))


    // examine potential stopwords
    val tokenCounts = filterNumbers.map(t => (t, 1)).reduceByKey(_ + _)
//    val oreringDesc = Ordering.by[(String, Int), Int](_._2)
//    println(tokenCounts.top(20)(oreringDesc).mkString("\n"))


    // filter out stopwords
    val stopwords = Set(
      "the","a","an","of","or","in","for","by","on","but", "is", "not", "with", "as", "was", "if",
      "they", "are", "this", "and", "it", "have", "from", "at", "my", "be", "that", "to"
    )
//    val tokenCountsFilteredStopwords = tokenCounts.filter { case (k, v) => !stopwords.contains(k) }
//    println(tokenCountsFilteredStopwords.top(20)(oreringDesc).mkString("\n"))


    // filter out tokens less than 2 characters
//    val tokenCountsFilteredSize = tokenCountsFilteredStopwords.filter { case (k, v) => k.size >= 2 }
//    println(tokenCountsFilteredSize.top(20)(oreringDesc).mkString("\n"))

    //5、下面基于频率去除单词：去掉在整个文本库中出现频率很低的单词
//    val oreringAsc = Ordering.by[(String, Int), Int](- _._2)//新建一个排序器，将单词，次数对按照次数从小到大排序
//    println(tokenCountsFilteredSize.top(20)(oreringAsc).mkString("\n"))


    // filter out rare tokens with total occurence < 2
    val rareTokens = tokenCounts.filter{ case (k, v) => v < 2 }.map { case (k, v) => k }.collect.toSet
//    val tokenCountsFilteredAll = tokenCountsFilteredSize.filter { case (k, v) => !rareTokens.contains(k) }
//    println(tokenCountsFilteredAll.top(20)(oreringDesc).mkString("\n"))


    // create a function to tokenize each document
    def tokenize(line: String): Seq[String] = {
      line.split("""\W+""")
        .map(_.toLowerCase)
        .filter(token => regex.pattern.matcher(token).matches)
        .filterNot(token => stopwords.contains(token))
        .filterNot(token => rareTokens.contains(token))
        .filter(token => token.size >= 2)
        .toSeq
    }

    // tokenize each document
    val tokens = text.map(doc => tokenize(doc))
    println("文件中前N个词： "+tokens.first.take(20))


    // === train TF-IDF model === //

    import org.apache.spark.mllib.linalg.{ SparseVector => SV }
    import org.apache.spark.mllib.feature.HashingTF
    import org.apache.spark.mllib.feature.IDF
    // set the dimensionality of TF-IDF vectors to 2^18
    val dim = math.pow(2, 18).toInt
    val hashingTF = new HashingTF(dim)
    //HashingTF使用特征哈希把每个输入文本的词映射为一个词频向量的下标
    //每个词频向量的下标是一个哈希值（依次映射到特征向量的某个维度）。词项的值是本身的TF-IDF权重
    val tf = hashingTF.transform(tokens)
    // cache data in memory
    tf.cache


    val v = tf.first.asInstanceOf[SV]
    println("tf的第一个向量大小:" + v.size)
    // 262144
    println("非0项个数：" + v.values.size)
    //706
    println("前10列的下标：" + v.values.take(10).toSeq)
    //WrappedArray(1.0, 1.0, 1.0, 1.0, 2.0, 1.0, 1.0, 2.0, 1.0, 1.0)
    println("前10列的词频：" + v.indices.take(10).toSeq)
    // WrappedArray(313, 713, 871, 1202, 1203, 1209, 1795, 1862, 3115, 3166)


    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)
    val v2 = tfidf.first.asInstanceOf[SV]
    println(v2.values.size)
    // 706
    println(v2.values.take(10).toSeq)
    // WrappedArray(2.3869085659322193, 4.670445463955571, 6.561295835827856, 4.597686109673142,  ...
    println(v2.indices.take(10).toSeq)
    // WrappedArray(313, 713, 871, 1202, 1203, 1209, 1795, 1862, 3115, 3166)


    val hockeyText = rdd.filter { case (file, text) =>
      file.contains("china") }
    //println(hockeyText.count)

    val hockeyTF = hockeyText.mapValues(doc =>
      hashingTF.transform(tokenize(doc)))
    println(hockeyTF.count)
    val hockeyTfIdf = idf.transform(hockeyTF.map(_._2))
    //println(hockeyTfIdf.count)


    // compute cosine similarity using Breeze
    import breeze.linalg._
    val hockey1_1 = hockeyTfIdf.sample(true, 0.5)
    //println(hockey1_1.count)

    val hockey1 = hockeyTfIdf.sample(true, 0.5 ).first.asInstanceOf[SV]
    val breeze1 = new SparseVector(hockey1.indices, hockey1.values, hockey1.size)
    val hockey2 = hockeyTfIdf.sample(true, 0.5).first.asInstanceOf[SV]
    val breeze2 = new SparseVector(hockey2.indices, hockey2.values, hockey2.size)
    val cosineSim = breeze1.dot(breeze2) / (norm(breeze1) * norm(breeze2))
    println("余弦相似度："+cosineSim)

//    import org.apache.spark.mllib.feature.Word2Vec
//    val word2vec = new Word2Vec()
//    word2vec.setSeed(42) // we do this to generate the same results each time
//    val word2vecModel = word2vec.fit(tokens)
//
//    // evaluate a few words
//    word2vecModel.findSynonyms("china", 20).foreach(println)
//    word2vecModel.findSynonyms("report", 20).foreach(println)
  }
}
