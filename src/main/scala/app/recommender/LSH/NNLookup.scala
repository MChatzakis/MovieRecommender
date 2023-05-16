package app.recommender.LSH

import org.apache.spark.rdd.RDD

/**
 * Class for performing LSH lookups
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookup(lshIndex: LSHIndex) extends Serializable {

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, result) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    // Hash queries to perform the lookup
    val hashedQueries = lshIndex.hash(queries)

    // Return the Genre->Movies data
    lshIndex.lookup(hashedQueries).map{
      entry=>
        (entry._2, entry._3)
    }
  }
}
