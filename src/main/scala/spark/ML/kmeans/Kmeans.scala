package spark.ML.kmeans

import org.apache.spark.{SparkConf, SparkContext}

object Kmeans  {

  def main(args: Array[String]): Unit = { // $example on

    val conf =new SparkConf().setAppName("SparkFromES").setMaster("local[2]")
    val sc =new SparkContext(conf)

    // Run ALS model to generate movie and user factors
    import org.apache.spark.mllib.recommendation.{ALS, Rating}
    val rawData = sc.textFile("D:\\code\\spark\\data\\ml-100k\\u.data")
    val rawRatings = rawData.map(_.split("\t").take(3))
    val ratings = rawRatings.map{ case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }
    ratings.cache
//    print(ratings.foreach(x=> print(x+"\t")))
    val alsModel = ALS.train(ratings, 20, 10, 0.1)

    // extract factor vectors
    import org.apache.spark.mllib.linalg.Vectors
    val movieFactors = alsModel.productFeatures.map { case (id, factor) => (id, Vectors.dense(factor)) }
    //movieFactors.foreach(e=> print(e))
    val movieVectors = movieFactors.map(_._2)
    movieVectors.foreach(e=> print(e))

    val userFactors = alsModel.userFeatures.map { case (id, factor) => (id, Vectors.dense(factor)) }
    val userVectors = userFactors.map(_._2)

    import org.apache.spark.mllib.clustering.KMeans
    val numClusters = 5
    val numIterations = 10
    val numRuns = 3

    // cross-validation for movie clusters
    val trainTestSplitMovies = movieVectors.randomSplit(Array(0.6, 0.4), 123)
    val trainMovies = trainTestSplitMovies(0)
    val testMovies = trainTestSplitMovies(1)

    val costsMovies = Seq(2, 3, 4, 5, 10, 20).map { k => (k, KMeans.train(trainMovies, numIterations, k, numRuns).computeCost(testMovies)) }
    println("Movie clustering cross-validation:")
    costsMovies.foreach { case (k, cost) => println(f"WCSS for K=$k id $cost%2.2f") }

    val  numClusters2= 10
    val movieClusterModel=new KMeans().setK(numClusters).setMaxIterations(numIterations).setSeed(3).run(movieVectors);
    //val movieClusterModel = KMeans.train(movieVectors, numClusters2, numIterations, numRuns)
    val userClusterModel = KMeans.train(userVectors, numClusters2, numIterations, numRuns)
    val movieCost=movieClusterModel.computeCost(movieVectors)
    println(movieCost)
    val userCost=movieClusterModel.computeCost(userVectors)
    println(userCost)

    movieClusterModel.clusterCenters.foreach(
       center => {
         println("Clustering Center:"+center)
       })

    /*使用聚类模型进行预测*/
    val user_predict=userClusterModel.predict(userVectors)
    user_predict.map(x =>(x,1)).reduceByKey(_+_).collect().foreach(println(_))

    val movie_predict=movieClusterModel.predict(movieVectors)
    movie_predict.map(x =>(x,1)).reduceByKey(_+_).collect.foreach(println(_))
    import breeze.linalg._
    import breeze.numerics.pow
    def computeDistance(v1: DenseVector[Double], v2: DenseVector[Double]): Double = pow(v1 - v2, 2).sum

    val path = "D:\\code\\spark\\data\\ml-100k\\u.item"
    val path2="D:\\code\\spark\\data\\ml-100k\\u.genre"
    val movies = sc.textFile(path)
    println(movies.first)

    val genres = sc.textFile(path2)
    genres.take(5).foreach(println)

    val genreMap = genres.filter(!_.isEmpty).map(line => line.split("\\|")).map(array => (array(1), array(0))).collectAsMap
    println(genreMap)


    val titlesAndGenres = movies.map(_.split("\\|")).map { array =>
      val genres = array.toSeq.slice(5, array.size)
      val genresAssigned = genres.zipWithIndex.filter { case (g, idx) =>
        g == "1"
      }.map { case (g, idx) =>
        genreMap(idx.toString)
      }
      (array(0).toInt, (array(1), genresAssigned))
    }
    println(titlesAndGenres.first)

    // join titles with the factor vectors, and compute the distance of each vector from the assigned cluster center
    val titlesWithFactors = titlesAndGenres.join(movieFactors)
    val moviesAssigned = titlesWithFactors.map { case (id, ((title, genres), vector)) => //vector可以理解为该点的坐标向量
      val pred = movieClusterModel.predict(vector)//pred为预测出的该点所属的聚点
    val clusterCentre = movieClusterModel.clusterCenters(pred)//clusterCentre为该pred聚点的坐标向量
    val dist = computeDistance(DenseVector(clusterCentre.toArray), DenseVector(vector.toArray))//求两坐标的距离
      (id, title, genres.mkString(" "), pred, dist)
    }
    val clusterAssignments = moviesAssigned.groupBy { case (id, title, genres, cluster, dist) => cluster }.collectAsMap//根据聚点分组

    for ( (k, v) <- clusterAssignments.toSeq.sortBy(_._1)) {
      println(s"Cluster $k:")
      val m = v.toSeq.sortBy(_._5)
      println(m.take(20).map { case (_, title, genres, _, d) => (title, genres, d) }.mkString("\n"))
      println("=====\n")
    }
  }
}
