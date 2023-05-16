package app.aggregator

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  private var state = null
  private var partitioner: HashPartitioner = null

  private var moviesWithAverageRating: RDD[(Int, (Int, Double, String, List[String], Int, Double))] = _

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {

    // Grouping the ratings on the title_id field
    val groupedRatings = ratings.groupBy(entry=>entry._2)

    // Perform Join on title_id field. Left outer join to include the movies that do not have ratings.
    val mergedMoviesRatings = title.groupBy(_._1).leftOuterJoin(groupedRatings)

    // Calculate the average rating of each movie
    val moviesWithAverageRatingData = mergedMoviesRatings.map{
      entry =>
        val movieID = entry._1
        val movieDataBuffer = entry._2._1 // Iterable of single value
        val ratingsDataBuffer = entry._2._2 // Optional iterable of the ratings

        var avgRating = 0.0
        var ratingCount = 0
        var ratingsSum = 0.0

        // If this movie has ratings
        if(ratingsDataBuffer.isDefined){
          // Get the data from the optional buffer
          val ratingList = ratingsDataBuffer.get

          // Calculate the average rating for the movie
          ratingsSum = ratingList.map(_._4).sum
          ratingCount = ratingList.size
          avgRating = ratingsSum/ratingCount
        }

        // Retrieve the movie data and add the result with the corresponding rating.
        val movieData = movieDataBuffer.head
        (movieID, avgRating, movieData._2, movieData._3, ratingCount, ratingsSum)
    }.keyBy(_._1) // setting KeyBy to movieID to be able to partition the RDD among nodes

    // Partition the RDD across nodes
    val partitions = 8
    partitioner = new HashPartitioner(partitions)
    moviesWithAverageRating = moviesWithAverageRatingData.partitionBy(partitioner)

    // Keep the average movie ratings in memory
    moviesWithAverageRating.persist()
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = {
    this.moviesWithAverageRating.map{
      entry =>
        (entry._2._3, entry._2._2)
    }
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {

    // Optimization: If there are no keywords, return immediately.
    // This is not necessary for correctness, but it wont spend time traversing and filtering the movies
    if (keywords.isEmpty)
      return -1.0

    val filteredMovies = moviesWithAverageRating.map {
      // Keep only the name, rating and keywords of each movie
      entry =>
        (entry._2._3, entry._2._2, entry._2._4)
    }. // Filter the entries with matching keywords
      filter(_._3.intersect(keywords).toSet == keywords.toSet).map {
      // Keep only the movie rating and title
      entry =>
        (entry._2, entry._1)
    }

    //filteredMovies.collect().foreach(println)

    // If no matching movies are found given the keywords, return -1.
    if (filteredMovies.count() == 0)
      -1.0
    else
      filteredMovies.map(_._1).mean()

  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {

    // Optimization: If there are no updates, return immediately.
    // This is not necessary for correctness, but it wont spend time traversing the movies to find which
    // movie has updated ratings.
    if (delta_.isEmpty)
      return

    // Group the updates on title_id to have all updates for a specific movie
    val groupedNewRatings = delta_.groupBy(_._2)

    // Iterable that for each movie update ratings, it holds the sum of the new ratings
    // and the number of the new ratings
    val averageNewRatings = groupedNewRatings.map{
      entry=>
        val movieID = entry._1
        val ratingList = entry._2//.toList // New ratings

        // Calculating the sum of the new Ratings
        // If a rating is an update (i.e. optionalPreviousRating exists),
        // then we need to remove the existence of the old rating.
        // We do that by subtracting it from the sum.
        val ratingListSum = ratingList.foldLeft(0.0) {
          case (currSum, (_, _, optionalPreviousRating, currRating, _)) =>
            val ratingToAdd = currRating
            val ratingToSub = optionalPreviousRating.getOrElse(0.0) // Sub nothing if this rating is not an update

            currSum + ratingToAdd - ratingToSub
        }

        // We calculate how many new ratings we have
        // Ratings that are update of old ones are skipped.
        val newRatingsNumber =  ratingList.foldLeft(0) {
          case (currSum, (_, _, optionalPreviousRating, _, _)) =>
            if(optionalPreviousRating.isDefined)
              currSum
            else
              currSum + 1
        }

        // Keeping only the necessary data.
        (movieID, ratingListSum, newRatingsNumber)
    }//.toArray

    // Perform the updates to the current movies with ratings
    moviesWithAverageRating = moviesWithAverageRating.map{
      entryPair =>
        // Get all needed data
        val entry = entryPair._2 // Get the data for this movie ID

        val movieID = entry._1
        val oldRating = entry._2
        val movieTitle = entry._3
        val movieGenres = entry._4
        val oldUsers = entry._5
        val oldRatingSum = entry._6

        // Rating metadata that will be useful to calculate the update ratings
        var updatedRating = -1.0
        var newUsers = 0
        var newRatingsSum = 0.0

        // Search if the current movie exists in the updates
        val movieEntry = averageNewRatings.find(_._1 == movieID)

        // Check if the movie actually has registered updates
        // It is guaranteed that for any movie, we will match one of the two cases
        movieEntry match{
          // If updates exist
          case Some(data) =>
            // Get the metadata of the new update
            newRatingsSum = data._2
            newUsers = data._3

            // Calculate the new rating using old and new users and ratings
            updatedRating = (newRatingsSum + oldRatingSum)/(oldUsers+newUsers)

          // If there are no updates, leave the data as they are
          case None =>
            updatedRating = oldRating
            newUsers = 0
            newRatingsSum = 0.0
        }

        // Update the movie data.
        (movieID, updatedRating, movieTitle, movieGenres, oldUsers+newUsers, newRatingsSum+oldRatingSum)
    }.keyBy(_._1)

    // Repartition and persist again
    moviesWithAverageRating = moviesWithAverageRating.partitionBy(partitioner)
    moviesWithAverageRating.persist()

  }

}
