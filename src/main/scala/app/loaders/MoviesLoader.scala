package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {
    val moviesRawRDD = sc.textFile(getClass.getResource(path).getPath)

    val moviesRDD = moviesRawRDD.map{
      line =>
        val trimmedLine = line.replaceAll("\"", "")
        val data = trimmedLine.split("\\|", 3)
        (data(0).toInt, data(1), data(2).split("\\|").toList)
    }

    moviesRDD.persist() //Persist the movies in memory

    moviesRDD
  }
}

