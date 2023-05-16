package app.recommender.LSH


import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

/**
 * Class for indexing the data for LSH
 *
 * @param data The title data to index
 * @param seed The seed to use for hashing keyword lists
 */
class LSHIndex(data: RDD[(Int, String, List[String])], seed : IndexedSeq[Int]) extends Serializable {

  private val minhash = new MinHash(seed)

  private val partitions = 8
  private val partitioner: HashPartitioner = new HashPartitioner(partitions)

  // Create the buckets upon init
  private val hashedMovies = createBuckets()
  hashedMovies.persist()

  /**
   * Hash function for an RDD of queries.
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (signature, keyword list) pairs
   */
  def hash(input: RDD[List[String]]) : RDD[(IndexedSeq[Int], List[String])] = {
    input.map(x => (minhash.hash(x), x))
  }

  /**
   * Creates the hash buckets for the index.
   * This is meant to be called upon the initialization of the class.
   * It also partitions the the buckets using default partitioning.
   * @return The structure of the created buckets
   */
  def createBuckets(): RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = {
    val resData = data.map {
      entry =>
        (entry._1, entry._2, entry._3, minhash.hash(entry._3))
    }.groupBy(_._4).map {
      entry =>
        // casting entry2 to list to match the required returned type
        (entry._1, entry._2.toList.map(entry => (entry._1, entry._2, entry._3)))
    }

    // Partition the buckets by signature (hash paritioner is applied on signature field.
    // The default number of partitions is 8.
    // Alternatively it could be the number of the buckets, so that each bucket is directed to a node.
    val partitionedData = resData.partitionBy(this.partitioner)
    partitionedData
  }

  /**
   * Return data structure of LSH index for testing
   *
   * @return Data structure of LSH index
   */
  def getBuckets(): RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = {
    this.hashedMovies
  }

  /**
   * Lookup operation on the LSH index
   *
   * @param queries The RDD of queries. Each query contains the pre-computed signature
   *                and a payload
   * @return The RDD of (signature, payload, result) triplets after performing the lookup.
   *         If no match exists in the LSH index, return an empty result list.
   */
  def lookup[T: ClassTag](queries: RDD[(IndexedSeq[Int], T)])
  : RDD[(IndexedSeq[Int], T, List[(Int, String, List[String])])] = {

    // Get buckets and group them by the hash to perform the join
    val buckets = getBuckets().keyBy(_._1)

    val joinedQueries =  queries.keyBy(_._1).join(buckets)

    joinedQueries.map{
      entry =>
        val key = entry._1

        val queryData = entry._2._1
        val bucketData = entry._2._2

        (key, queryData._2, bucketData._2)
    }
  }
}
