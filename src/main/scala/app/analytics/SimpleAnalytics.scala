package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import java.sql.Timestamp
import java.time.LocalDate


class SimpleAnalytics() extends Serializable {

  private var ratingsPartitioner: HashPartitioner = null
  private var moviesPartitioner: HashPartitioner = null

  private var moviesGroupedByID: RDD[(Int, Iterable[(Int, String, List[String])])] = _
  private var ratingsGroupedByYearByID: RDD[((Int, Int), Iterable[(Int, Int, Option[Double], Double, Int)])] = _

  /**
   * Preprocesses and initializes the movie and rating RDDs of this class
   *
   * @param ratings User ratings RDD
   * @param movie   Movie data RDD
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {

    // Grouping the movies by ID to be able to perform joins later on
    val groupedMoviesRDD = movie.groupBy(entry => entry._1)

    // Transform the timestamp field to year in the ratings
    val ratingsYearRDD = ratings.map {
      entry =>
        val timestamp: Timestamp = new Timestamp(entry._5.toLong * 1000L)
        val year: Int = LocalDate.from(timestamp.toLocalDateTime).getYear
        (entry._1, entry._2, entry._3, entry._4, year)
    }

    // Grouping the ratings first by year and then by movie ID (TA said that this is ok)
    val groupedRatingsRDD = ratingsYearRDD.groupBy(entry => (entry._5, entry._2))

    // Initialize the partitioners
    val partitions = 8
    this.moviesPartitioner = new HashPartitioner(partitions)
    this.ratingsPartitioner = new HashPartitioner(partitions)

    // Initialize the RDDs of the class by partitioning the grouped RDDs
    this.moviesGroupedByID = groupedMoviesRDD.partitionBy(moviesPartitioner)
    this.ratingsGroupedByYearByID = groupedRatingsRDD.partitionBy(ratingsPartitioner)

    // Persist the RDDs in memory
    this.moviesGroupedByID.persist()
    this.ratingsGroupedByYearByID.persist()

    /*moviesGroupedByID.foreach { case (key, values) =>
      println(s"$key: ${values.mkString(", ")}")
    }*/

    /*ratingsGroupedByYearByID.foreach { case (key, values) =>
      println(s"$key: ${values.mkString(", ")}")
    }*/
  }

  /**
   * Get the number of movies rated for each year
   *
   * @return RDD[(year: int, count:int)]
   */
  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {
    // Create an RDD[year, RatingsPerYearData] by iterating over the rating data
    val summedRatings = this.ratingsGroupedByYearByID.map {
      entry =>
        val year = entry._1._1
        val movieID = entry._1._2
        (year, movieID)
    }.groupBy(entry => entry._1)

    // Calculating the movie ratings per year
    val moviesEachYear = summedRatings.map {
      entry =>
        val year = entry._1
        val movieYearPairs = entry._2
        (year, movieYearPairs.size)
    }

    //moviesEachYear.foreach(println)
    moviesEachYear
  }

  /**
   * Get the movie with the most ratings in each year
   *
   * @return RDD[(year:int, title:String)] containing the per-year results
   */
  def getMostRatedMovieEachYear: RDD[(Int, String)] = {
    // Gather the number of ratings for each movie for each year
    val numberOfRatingsPerYear = ratingsGroupedByYearByID.map {
      // Reform the ratings to a simpler RDD containing only the needed data
      entry =>
        val year = entry._1._1
        val movieID = entry._1._2
        val ratings = entry._2.size
        (movieID, year, ratings)
    }.keyBy(_._1) //Applying keyBy to do the join on the movie ID field
      .join(moviesGroupedByID).map { //Join with movies RDD to obtain all movie data
      entry =>
        //Obtain needed data in a friendly form
        val movieID = entry._1
        val keyData = entry._2
        val year = keyData._1._2
        val ratings = keyData._1._3
        val name = keyData._2.head /*.toList.last*/ ._2
        (year, name, ratings, movieID)
    }

    // Group the results by year
    val numberOfRatingsPerYearGroupBy = numberOfRatingsPerYear.groupBy(entry => entry._1)

    // For each year, get the movie that had the most ratings for this year
    val mostRatedEachYear = numberOfRatingsPerYearGroupBy.map {
      entry =>
        val year = entry._1
        val yearDataList = entry._2 //.toList
        //In case of tie, we get the one with max ID
        val name = yearDataList.maxBy { case (_, _, count, id) => (count, id) }._2

        (year, name)
    }

    //mostRatedEachYear.collect().sortBy(_._1).foreach(println)

    mostRatedEachYear
  }

  /**
   * Get most rated genre for each year
   *
   * @return RDD[(year:int, genre(s):List[String])] containing the results
   */
  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = {
    //We apply the same functionality as getMostRatedMovieEachYear.
    val numberOfRatingsPerYear = ratingsGroupedByYearByID.map {
      entry =>
        val year = entry._1._1
        val movieID = entry._1._2
        val ratings = entry._2.size
        (movieID, year, ratings)
    }.keyBy(_._1).join(moviesGroupedByID).map {
      entry =>
        val movieID = entry._1
        val keyData = entry._2
        val year = keyData._1._2
        val ratings = keyData._1._3
        val genres = keyData._2.toList.last._3
        (year, ratings, movieID, genres)
    }

    val numberOfRatingsPerYearGroupBy = numberOfRatingsPerYear.groupBy(entry => entry._1)

    val mostRatedEachYear = numberOfRatingsPerYearGroupBy.map {
      entry =>
        val year = entry._1
        val yearDataList = entry._2.toList
        val genres = yearDataList.maxBy { case (_, count, id, _) => (count, id) }._4

        (year, genres)
    }
    //mostRatedEachYear.collect().sortBy(_._1).foreach(println)

    mostRatedEachYear
  }

  /**
   * Get most and least rated genres of all time, based on most rated genres of each year
   * Note: if two genre has the same number of rating, returns the first one based on lexicographical sorting on genre.
   *
   * @return Pair (leastRatedGenreData, mostRatedGenreData)
   */
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = {
    // The result is found based on the most rated genres of each year.
    val mostRatedGenre = this.getMostRatedGenreEachYear
    val genreYearPairs = mostRatedGenre.flatMap { case (year, strings) => strings.map((year, _)) }

    // Count occurrences for each genre
    val countData: RDD[((Int, String), Int)] = genreYearPairs.map((_, 0)).reduceByKey(_ + _)
    val resultRaw: RDD[(String, Int)] = countData.map { case ((year, str), count) => (str, 1) }.reduceByKey(_ + _)

    // Find min and max count of genres
    val maxGenre: (String, Int) =
      resultRaw.takeOrdered(1)(Ordering[(Int, String)].reverse.on(x => (x._2, x._1))).head
    val minGenre: (String, Int) =
      resultRaw.takeOrdered(1)(Ordering[(Int, String)].on(x => (x._2, x._1))).head

    (minGenre, maxGenre)
  }

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = {

    // This is the non-efficient version of the method, because collect needs to gather all data.
    // The better version is below using broadcasting
    val genreList = requiredGenres.collect()

    // Filter movies using the required genres
    movies.filter(entry => genreList.forall(entry._3.contains)).map(entry => entry._2)
  }

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = {

    // Broadcast the required genres to all Spark executors using the broadcast callback
    // The list of genres is contained in the variable as the value field and can manipulated as a normal list
    val broadcastedGenres = broadcastCallback(requiredGenres)

    // Filter the movies RDD to only include movies in the required genres
    // Same as get all movies by genre
    val filteredMovies = movies.filter(movie => movie._3.intersect(broadcastedGenres.value).nonEmpty)

    // Format and return the result
    filteredMovies.map(movie => movie._2)
  }
}

