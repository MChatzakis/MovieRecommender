package app.recommender.baseline

import org.apache.spark.rdd.RDD

/**
 * BaselinePredictor class offers baseline user predictions
 */
class BaselinePredictor() extends Serializable {

  private var state: (Int, Double) = null
  private var ratingsWithAvg: RDD[(Int, Int, Option[Double], Double, Int, Double)] = _
  private var movieRatings: RDD[(Int, (Int, Int, Double))] = _

  /**
   * Initialize the needed data for baseline predictions
   * @param ratingsRDD User ratings, format:
   *                   (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {

    // Calculate the average rating for each user
    val userAverageRatings = ratingsRDD.groupBy(_._1).map {
      entry =>
        val userID = entry._1
        val userRatings = entry._2.map(_._4)
        val average = userRatings.sum / userRatings.size

        (userID, average)
    }

    // Add in each rating tuple the average rating for the corresponding user
    ratingsWithAvg = userAverageRatings.keyBy(_._1).
      join(ratingsRDD.keyBy(_._1)).map{
      entry=>
        val userID = entry._1
        val avgRating = entry._2._1._2
        val titleID = entry._2._2._2
        val rating = entry._2._2._4
        val oldOptionalRating = entry._2._2._3
        val timestamp = entry._2._2._5

        (userID, titleID, oldOptionalRating, rating, timestamp, avgRating)
    }

    // For each movie, keep the ID, the total ratings and the movie average dev,
    // in order to be able to calculate the predictions
    movieRatings = ratingsWithAvg.groupBy(_._2).map{
      entry =>
        val movieID = entry._1
        val userRatingData = entry._2
        val totalRatings = userRatingData.size
        val normalizedSum = userRatingData.map{
          data =>
            val userID = data._1
            val movieID = data._2
            val userRating = data._4
            val userAvgRating = data._6

            normalizedDev(userRating, userAvgRating)
        }.sum

        (movieID, totalRatings, normalizedSum/totalRatings)
    }.keyBy(_._1)

    // Persist the RDDs in memory
    movieRatings.persist()
    ratingsWithAvg.persist()

  }

  /**
   * Scale the values according to the baseline prediction policy
   * @param x param x
   * @param r param r
   * @return The scaled value based on x and r
   */
  def scale(x: Double, r: Double): Double = {
    if(x>r)
      5-r
    else if (x<r)
      r-1
    else
      1
  }

  /**
   * Return the normalized dev
   * @param userMovieRating the rating of the user for a specific movie
   * @param userAvgRating the average rating of the user for all the movies
   * @return
   */
  def normalizedDev(userMovieRating: Double, userAvgRating: Double): Double ={
    (userMovieRating - userAvgRating)/scale(userMovieRating, userAvgRating)
  }

  /**
   * Predict the rating of user for the movie
   * @param userId User ID to predict the rating
   * @param movieId Movie ID to predict the rating
   * @return Predicted rating of user id to movie id
   */
  def predict(userId: Int, movieId: Int): Double = {

    var averageUserRating = 0.0

    // Caching optimization: Save in "state" the data of the previous user ID.
    // This helps if predict is called many times for the same user (e.g. predict the user rating for a list of movies)
    // It helps the performance because the user data do not need to be retrieved
    if(state != null && state._1 == userId){
      averageUserRating = state._2
    }else{
      // If there is no cached data, or the user id is different than the one cached, lookup for the user rating
      val userResults = ratingsWithAvg.filter(_._1 == userId)

      // If the user has given a rating, use that, else averageUserRating remains 0.0
      if (!userResults.isEmpty()) {
        averageUserRating = userResults.first()._6
      }

      // Cache the new data
      state = (userId, averageUserRating)
    }

    // Get data for movieID
    val movieResults = movieRatings.lookup(movieId)//.toList

    // If there are results for the movie find the movie average dev
    var movieAverageDev = 0.0
    if(movieResults.nonEmpty){
      movieAverageDev = movieResults.head._3
    }
    else {
      // This is a small optimization step to return immediately
      return averageUserRating
    }

    // Return the predicted rating
    averageUserRating + movieAverageDev*scale(averageUserRating+movieAverageDev, averageUserRating)
  }
}
