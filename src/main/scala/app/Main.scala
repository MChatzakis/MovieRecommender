package app

import app._
import app.aggregator.Aggregator
import app.analytics.SimpleAnalytics
import app.loaders.{MoviesLoader, RatingsLoader}
import app.recommender.LSH.LSHIndex
import app.recommender.Recommender
import app.recommender.baseline.BaselinePredictor
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.List

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    val moviesLoader = new MoviesLoader(sc, "/movies_small.csv")
    val moviesRDD = moviesLoader.load()
    //println("Printing the movies.")
    //moviesRDD.foreach(println)

    val ratingsLoader = new RatingsLoader(sc, "/ratings_small.csv")
    val ratingsRDD = ratingsLoader.load()
    //println("Printing the ratings.")
    //ratingsRDD.foreach(println)
    //ratingsRDD.take(5).foreach(println)

    val simpleAnalytics = new SimpleAnalytics()
    simpleAnalytics.init(null, null)
    //simpleAnalytics.getNumberOfMoviesRatedEachYear
    //simpleAnalytics.getMostRatedMovieEachYear
    //val specified_movies = simpleAnalytics.getAllMoviesByGenre(moviesRDD,sc.parallelize(List("Comedy")))
    //specified_movies.foreach(println)
    //val res = simpleAnalytics.getMostAndLeastRatedGenreAllTime
    //println(res)

    //val aggregator = new Aggregator(sc)
    //aggregator.init(ratingsRDD, moviesRDD)

    //val avg = aggregator.getKeywordQueryResult(List("Thriller")) //- 3.15611
    //println((aggregator.getKeywordQueryResult(List("Thriller")) - 3.15611).abs < 0.001)
    //println(avg)

    //val baseline = new BaselinePredictor()
    //baseline.init(ratingsRDD)
   // baseline.predict(1,1)

    //val lsh = new LSHIndex(moviesLoader.load(), IndexedSeq(5, 16))
    //val predictor = new Recommender(sc, lsh, ratingsLoader.load())
    //val pred_user_16 = predictor.recommendBaseline(16, List("Action", "Adventure", "Sci-Fi"), 3)
  }


}
