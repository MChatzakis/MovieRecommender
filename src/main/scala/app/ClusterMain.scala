package app

import app.loaders.{MoviesLoader, RatingsLoader}
import app.recommender.LSH.LSHIndex
import app.recommender.Recommender
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ClusterMain {
  val MOVIES_BIG_DATASET = "hdfs://iccluster049.iccluster.epfl.ch:8020/cs460-data/movies_medium.csv"
  val RATINGS_BIG_DATASET = "hdfs://iccluster049.iccluster.epfl.ch:8020/cs460-data/ratings_medium.csv"

  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("chatzakis-epflGaspar-CS460-project")
    val sc = SparkContext.getOrCreate(conf)

    val movies = loadMoviesForCluster(sc, MOVIES_BIG_DATASET)
    val ratings = loadRatingsForCluster(sc, RATINGS_BIG_DATASET)

    println("Running baseline recommendations")
    baselineRecommendations(sc, movies, ratings)

    println("Running ALS recommendations")
    alsRecommendations(sc, movies, ratings)
  }

  def loadMoviesForCluster(sc: SparkContext, path: String): RDD[(Int, String, List[String])] = {
    val moviesRawRDD = sc.textFile(path)

    val moviesRDD = moviesRawRDD.map {
      line =>
        val trimmedLine = line.replaceAll("\"", "")
        val data = trimmedLine.split("\\|", 3)
        (data(0).toInt, data(1), data(2).split("\\|").toList)
    }

    moviesRDD.persist()

    moviesRDD
  }

  def loadRatingsForCluster(sc: SparkContext, path: String): RDD[(Int, Int, Option[Double], Double, Int)] = {
    val ratingsRawRDD = sc.textFile(path)

    val ratingsRDD = ratingsRawRDD.map {
      line =>
        val trimmedLine = line.replaceAll("\"", "")
        val data = trimmedLine.split("\\|")

        (data(0).toInt, data(1).toInt, None: Option[Double], data(2).toDouble, data(3).toInt)
    }

    ratingsRDD.persist()

    ratingsRDD
  }

  def baselineRecommendations(sc: SparkContext, movies: RDD[(Int, String, List[String])], ratings: RDD[(Int, Int, Option[Double], Double, Int)]): Unit ={
    val lsh = new LSHIndex(movies, IndexedSeq(5, 16))

    val predictor = new Recommender(sc, lsh, ratings)

    val pred_user_148 = predictor.recommendCollaborative(208, List("Action", "Adventure", "Crime", "Thriller"), 10)

  }

  def alsRecommendations(sc: SparkContext, movies: RDD[(Int, String, List[String])], ratings: RDD[(Int, Int, Option[Double], Double, Int)]): Unit ={
    val ratingsLoader = new RatingsLoader(sc, "/ratings_medium.csv")
    val moviesLoader = new MoviesLoader(sc, "/movies_medium.csv")

    val lsh = new LSHIndex(moviesLoader.load(), IndexedSeq(5, 16))

    val predictor = new Recommender(sc, lsh, ratingsLoader.load())

    val pred_user_16 = predictor.recommendCollaborative(16, List("Action", "Romance", "Western"), 10)
  }
}
