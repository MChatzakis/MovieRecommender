package app.commonUtils

import org.apache.spark.rdd.RDD

class CommonUtils {
  def printMappedRDD(mappedRDD : RDD[Any]): Unit  = {
    mappedRDD.foreach { case (key, values) =>
      println(s"$key: ${values}")
    }
  }
}
