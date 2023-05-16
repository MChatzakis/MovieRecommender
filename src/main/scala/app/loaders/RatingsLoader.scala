package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.{MEMORY_AND_DISK, MEMORY_ONLY}

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load(): RDD[(Int, Int, Option[Double], Double, Int)] = {
    val ratingsRawRDD = sc.textFile(getClass.getResource(path).getPath)

    val ratingsRDD = ratingsRawRDD.map {
      line =>
        val trimmedLine = line.replaceAll("\"", "")
        val data = trimmedLine.split("\\|")

        (data(0).toInt, data(1).toInt, None: Option[Double], data(2).toDouble, data(3).toInt)
    }

    ratingsRDD.persist() // Persist ratings to memory

    ratingsRDD
  }
}