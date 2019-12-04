package com.lavapm.utils


import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.util.control.Breaks._
import scala.math._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.Summarizer


/**
  * Provide analysis utilities.
  * Created by dalei on 5/28/19.
  */
object Analysis {

  val group = 4

  /**
    * generate the statistic information for a vector
    * @param spark                spark session
    * @param rdd                  the vector to be statistics
    * @return (Double, .....)     the mean, variance, count, max, min, normL1, normL2, 1/4 quartile, median, and 3/4 quartile for a vector
    */
  def vectorStatistic( spark: SparkSession,
                       rdd: RDD[Double]) : (Double, Double, Double, Double, Double, Double, Double, Double, Double, Double) = {

    import spark.implicits._
    import Summarizer._

    // val rdd = spark.sparkContext.parallelize(Array(9.1,10.2,11.3,22.1,34.7,4.2,5.7,6.0,15.8,21.9,18.2,1.2,3.4,77.1,44.3,52.9,28.1,23.6,14.8))

    val df = rdd.map{ p => (Vectors.dense(p), 1.0) }.toDF("features", "weight")

    val (meanVec, varianceVec, maxVec, minVec, normL1Vec, normL2Vec)
        = df.select(mean($"features"),
                    variance($"features"),
                    max($"features"),
                    min($"features"),
                    normL1($"features"),
                    normL2($"features")).as[(Vector, Vector, Vector, Vector, Vector, Vector)].first()

    val (quartile1, median, quartile3) = calculateQuartile(rdd)

    (meanVec.toArray(0), varianceVec.toArray(0), df.count.toDouble, maxVec.toArray(0),
      minVec.toArray(0), normL1Vec.toArray(0), normL2Vec.toArray(0), quartile1, median, quartile3)
  }



  /**
    * calculate the 1/4 quartile, median, and 3/4 quartile for a vector
    * @param rdd                          the vector to be calculated
    * @return (Double, Double, Double)    the 1/4 quartile, median, and 3/4 quartile for a vector
    */
  def calculateQuartile(rdd: RDD[Double]) : (Double, Double, Double) = {

    //val rdd = spark.sparkContext.parallelize(Array(9.1,10.2,11.3,22.1,34.7,4.2,5.7,6.0,15.8,21.9,18.2,1.2,3.4,77.1,44.3,52.9,28.1,23.6,14.8))

    // split the rdd to group
    val groupRDD = rdd.map(x=>(floor(x/4).toInt, x)).sortByKey()

    // calculate the count in each group
    val groupCountRDD = rdd.map(x=>(floor(x/4).toInt, 1)).reduceByKey(_+_).sortByKey()

    // create the groupCount map
    val groupCountMap = groupCountRDD.collectAsMap()

    // calculate the total count
    val sum = groupCountRDD.map(x=>x._2).sum().toInt

    var mid = 0
    var quartile1 = 0
    var quartile3 = 0

    // calculate the median position
    if(sum % 2 != 0)
      mid = sum/2 + 1
    else
      mid = sum/2

    // calculate the 1/4 and 3/4 quartile position
    if(sum % 4 != 0)
      quartile1 = sum / 4 + 1
    else
      quartile1 = sum / 4

    quartile3 = sum - sum / 4

    // get the biggest group key
    val groupUpperLimit = groupCountRDD.map(x=>x._1).max()

    val midValue = findElementInRDD(groupCountMap, groupUpperLimit, groupRDD, mid)
    val quartile1Value = findElementInRDD(groupCountMap, groupUpperLimit, groupRDD, quartile1)
    val quartile3Value = findElementInRDD(groupCountMap, groupUpperLimit, groupRDD, quartile3)

    (quartile1Value, midValue, quartile3Value)
  }



  /**
    * calculate the 1/4 quartile, median, and 3/4 quartile for a vector
    * @param groupCountMap        the count in each group
    * @param biggestGroupKey      the biggest group key
    * @param groupRDD             the element interval RDD, which had been sorted ascending
    * @param position             the element position in the whole RDD
    * @return Double              the element value
    */
  def findElementInRDD( groupCountMap: scala.collection.Map[Int, Int],
                        biggestGroupKey: Int,
                        groupRDD: RDD[(Int, Double)],
                        position: Int) : Double = {

    var temp1 = 0   // data number of cumulative intervals in which the element is located
    var temp2 = 0   // data number of cumulative intervals before which the element is located
    var index = 0   // interval in which the element is located

    breakable{
      for(i <- 0 to biggestGroupKey){
        if(groupCountMap.contains(i)) {
          temp1 = temp1 + groupCountMap(i)
          temp2 = temp1 - groupCountMap(i)
          if (temp1 >= position) {
            index = i
            break
          }
        }
      }
    }

    // the median offset in the interval in which the median is located
    val offset = position - temp2

    // takeOrdered can sort keys from small to large and get the first n elements in RDD
    val resultArray = groupRDD.filter(x=>x._1 == index).takeOrdered(offset)
    resultArray(offset-1)._2
  }

}



