package app.recommender

import app.recommender.LSH.{LSHIndex, NNLookup}
import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext,
                  index: LSHIndex,
                  ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {

  private val nn_lookup = new NNLookup(index)

  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)

  private val groupedRatings = ratings.groupBy(_._1)

  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    // Transform the genre list to an RDD to be able to use the implemented methods
    val genreRDD = sc.parallelize(List(genre))

    // Get the movie results based on the given genres
    val results = nn_lookup.lookup(genreRDD)

    // Reality check
    assert(results.count() == 1)

    // Get the list of the movies matching the genres given
    val movies = results.first()._2

    // Filter out the movies that the user has already given rating
    val movieIDsRatedByUser = getMovieIDsRatedByUser(userId)
    val filteredMovies = movies.filter{
      case (id, _, _) =>
        !movieIDsRatedByUser.contains(id)
    }

    //println("Total filtered movies: " + filteredMovies.size)

    // Predict the rating that the user would give to the movie
    val ratingPredictions = filteredMovies.map{
      entry =>
        val movieID = entry._1
        val predictedRating = baselinePredictor.predict(userId, movieID)
        (movieID, predictedRating)
    }

    // Find and return the top-K best rated movies based on the predictions
    val topK = ratingPredictions.sortBy(-_._2).take(K)
    topK
  }

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    // Transform the genre list to an RDD to be able to use the implemented methods
    val genreRDD = sc.parallelize(List(genre))

    // Get the movie results based on the given genres
    val results = nn_lookup.lookup(genreRDD)

    // Reality check
    //assert(results.count() == 1)

    val movies = results.first()._2

    //Need to filter out the movies that the user has already given rating.
    val movieIDsRatedByUser = getMovieIDsRatedByUser(userId)
    val filteredMovies = movies.filter {
      case (id, _, _) =>
        !movieIDsRatedByUser.contains(id)
    }

    // Get the list of the movies matching the genres given
    val ratingPredictions = filteredMovies.map {
      entry =>
        val movieID = entry._1
        val userID = userId
        val predictedRating = collaborativePredictor.predict(userID, movieID)

        (movieID, predictedRating)
    }

    // Find and return the top-K best rated movies based on the predictions
    val topK = ratingPredictions.sortBy(-_._2).take(K)
    topK
  }

  /**
   * Get the movie IDs that a user has rated.
   * @param userId The user that we want to get the movies
   * @return A list of the movie IDs
   */
  def getMovieIDsRatedByUser(userId: Int): List[Int] = {
    // Locate the ratings of the user
    val userRatingData = groupedRatings.filter(_._1 == userId)

    // If the user has given no ratings, return immediately
    if (userRatingData.count() == 0){
      return List()
    }

    // Convert ratings to list in order to return the result as a list
    val userRatedMovies = userRatingData.first()._2.toList

    // Gather only the IDs of the movies that the user has rated
    val userRatedMovieIDs = userRatedMovies.map{
      entry=>
        entry._2
    }

    userRatedMovieIDs
  }
}
