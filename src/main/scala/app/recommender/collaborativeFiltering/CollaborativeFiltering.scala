package app.recommender.collaborativeFiltering


import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

/**
 * Collaborative Filtering class offers ALS-based user rating predictions
 *
 * @param rank ALS related parameter
 * @param regularizationParameter ALS related parameter
 * @param seed ALS related parameter
 * @param n_parallel ALS related parameter
 */
class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  // NOTE: set the parameters according to the project description to get reproducible (deterministic) results.
  private val maxIterations = 20
  private var model: MatrixFactorizationModel = _

  /**
   * Initialized the ALS model
   * @param ratingsRDD User ratings to train the model format:
   *                   (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {

    // Transform the ratings into the Rating object that ALS supports
    val ratings = ratingsRDD.map{
      entry =>
        val userID = entry._1
        val movieID = entry._2
        val rating = entry._4
        Rating(userID, movieID, rating)
    }

    // Initialize and train the model
    model = ALS.train(ratings, this.rank, this.maxIterations, this.regularizationParameter, this.n_parallel, this.seed)
  }

  /**
   * Predict the rating of userId for movieID
   * @param userId User to predict rating
   * @param movieId Movie to predict rating
   * @return The rating value that the user would give to the movie
   */
  def predict(userId: Int, movieId: Int): Double = {
    model.predict(userId, movieId)
  }

}
