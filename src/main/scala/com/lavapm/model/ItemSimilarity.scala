package com.lavapm.model

import scala.math._
import org.apache.spark.rdd.RDD


/**
  * Item Similarity
  * Created by dalei on 10/26/18.
  */


/**
  * item prefer from user
  * @param userid user
  * @param itemid preferred item
  * @param prefer prefer point from user
  */
case class ItemPrefer(
                     val userid: String,
                     val itemid: String,
                     val prefer: Double) extends Serializable

/**
  * user recommended item
  * @param userid user
  * @param itemid recommended item
  * @param prefer prefer
  */
case class UserRecommend(
                          val userid: String,
                          val itemid: String,
                          val prefer: Double) extends Serializable
/**
  * item similarity
  * @param itemid1 item one
  * @param itemid2 item two
  * @param similar similarity
  */
case class ItemSimilar(
                        val itemid1: String,
                        val itemid2: String,
                        val similar: Double) extends Serializable

/**
  * similarity calculation
  * support: cooccurrence, cosine, euclidean
  *
  */
class ItemSimilarity extends Serializable {

  /**
    * similarity calculation
    * @param user_rdd             user prefer point
    * @param stype                the calculation formula
    * @return RDD[ItemSimilarity] item similarity
    */
  def Similarity(user_rdd: RDD[ItemPrefer], stype: String): (RDD[ItemSimilar]) = {
    val similiar_rdd = stype match {
      case "cooccurrence" =>
        ItemSimilarity.CooccurrenceSimilarity(user_rdd)
      case "cosine" =>
        ItemSimilarity.CosineSimilarity(user_rdd)
      case "euclidean" =>
        ItemSimilarity.EuclideanDistanceSimilarity(user_rdd)
      case _ =>
        ItemSimilarity.CooccurrenceSimilarity(user_rdd)
    }

    similiar_rdd
  }

}

object ItemSimilarity {

  /**
    * Cooccurrence similarity calculation
    * w(i,j) = N(i)∩N(j)/sqrt(N(i)*N(j))
    * @param user_rdd            user prefer point
    * @return RDD[ItemSimilar]   return the item similarity
    */
  def CooccurrenceSimilarity(user_rdd: RDD[ItemPrefer]): (RDD[ItemSimilar]) = {

    // prepare the data, (user,item)
    val user_rdd1 = user_rdd.map(f => (f.userid, f.itemid, f.prefer)).map(f => (f._1, f._2))

    // (user,item) cartesian (user,item) => ((item,item),times)
    val item_rdd1 = user_rdd1.join(user_rdd1).map(f => (f._2, 1)).reduceByKey(_+_)

    // diagonal matrix, (item, times)
    val diag_matrix = item_rdd1.filter(f => f._1._1 == f._1._2).map(f => (f._1._1, f._2))

    // non-diagonal matrix, ((item, item), times)
    val non_diag_matrix = item_rdd1.filter(f => f._1._1 != f._1._2)

    // ((item1, item2), times) => (item1, (item1, item2, times))
    // join (item1, times) => (item1, ((item1, item2, times), times1))
    // (item1, ((item1, item2, times), times1)) => (item2, (item1, item2, times, times1))
    // join (item2, times) = (item2, ((item1, item2, times, times1), times2))
    // (item2, ((item1, item2, times, times1), times2)) => (item1, item2, times, times1, times2)
    val item_rdd2 = non_diag_matrix.map(f => (f._1._1, (f._1._1, f._1._2, f._2)))
                    .join(diag_matrix)
                    .map(f => (f._2._1._2, (f._2._1._1,f._2._1._2, f._2._1._3, f._2._2)))
                    .join(diag_matrix)
                    .map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))
                    .map(f => (f._1, f._2, (f._3 / sqrt(f._4 * f._5))))

    // result
    item_rdd2.map(f => ItemSimilar(f._1, f._2, f._3))
  }

  /**
    * Cosine similarity calculation
    * T(x,y) = ∑x(i)y(i) / sqrt(∑(x(i)*x(i)) * ∑(y(i)*y(i)))
    * @param user_rdd            user prefer point
    * @return RDD[ItemSimilar]   return the item similarity
    */
  def CosineSimilarity(user_rdd: RDD[ItemPrefer]): (RDD[ItemSimilar]) = {

    // prepare the data, (user, (item, prefer))
    val user_rdd1 = user_rdd.map(f => (f.userid, f.itemid, f.prefer)).map(f => (f._1, (f._2, f._3)))

    // (user, (item, prefer)) cartesian (user, (item, prefer)) => (user, ((item1, prefer1), (item2, prefer2))
    // (user, ((item1, prefer1), (item2, prefer2)) => ((item1, item2), (prefer1, prefer2))
    // ((item1, item2), (prefer1, prefer2)) = > ((item1, item2), prefer1 * prefer2)
    val item_rdd1 = user_rdd1.join(user_rdd1)
                    .map(f => ((f._2._1._1, f._2._2._1), (f._2._1._2, f._2._2._2)))
                    .map(f => (f._1, f._2._1 * f._2._2))
                    .reduceByKey(_ + _)

    // diagonal matrix, (item, value)
    val diag_matrix = item_rdd1.filter(f => f._1._1 == f._1._2).map(f => (f._1._1, f._2))

    // non-diagonal matrix, ((item, item), value)
    val non_diag_matrix = item_rdd1.filter(f => f._1._1 != f._1._2)

    // ((item1, item2), value) => (item1, (item1, item2, value))
    // join (item1, value) => (item1, ((item1, item2, value), value1))
    // (item1, ((item1, item2, value), value1)) => (item2, (item1, item2, value, value1))
    // join (item2, value) = (item2, ((item1, item2, value, value1), value2))
    // (item2, ((item1, item2, value, value1), value2)) => (item1, item2, value, value1, value2)
    val item_rdd2 = non_diag_matrix.map(f => (f._1._1, (f._1._1, f._1._2, f._2)))
                                    .join(diag_matrix)
                                    .map(f => (f._2._1._2, (f._2._1._1,f._2._1._2, f._2._1._3, f._2._2)))
                                    .join(diag_matrix)
                                    .map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))
                                    .map(f => (f._1, f._2, (f._3 / sqrt(f._4 * f._5))))

    // result
    item_rdd2.map(f => ItemSimilar(f._1, f._2, f._3))
  }

  /**
    * Euclidean distance similarity calculation
    * d(x, y) = sqrt(∑((x(i)-y(i)) * (x(i)-y(i))))
    * sim(x, y) = n / (1 + d(x, y))
    * @param user_rdd            user prefer point
    * @return RDD[ItemSimilar]   return the item similarity
    */
  def EuclideanDistanceSimilarity(user_rdd: RDD[ItemPrefer]): (RDD[ItemSimilar]) = {

    // prepare the data, (user, (item, prefer))
    val user_rdd1 = user_rdd.map(f => (f.userid, f.itemid, f.prefer)).map(f => (f._1, (f._2, f._3)))

    // (user, (item, prefer)) cartesian (user, (item, prefer)) => (user, ((item1, prefer1), (item2, prefer2))
    // (user, ((item1, prefer1), (item2, prefer2)) => ((item1, item2), (prefer1, prefer2))
    // ((item1, item2), (prefer1, prefer2)) = > ((item1, item2), ((prefer1 - prefer2) * (prefer1 - prefer2))
    val item_rdd1 = user_rdd1.join(user_rdd1)
                            .map(f => ((f._2._1._1, f._2._2._1), (f._2._1._2, f._2._2._2)))
                            .map(f => (f._1, (f._2._1 - f._2._2) * (f._2._1 - f._2._2)))
                            .reduceByKey(_ + _)

    // cooccurrence matrix, ((item, item), times)
    val cooccur_rdd = item_rdd1.map(f => (f._1, 1)).reduceByKey(_ + _)

    // non-diagonal matrix, ((item, item), value)
    val non_diag_matrix = item_rdd1.filter(f => f._1._1 != f._1._2)

    // ((item1, item2), (value, times))
    val item_rdd2 = non_diag_matrix.join(cooccur_rdd).map(f => (f._1._1, f._1._2, f._2._2/(1 + sqrt(f._2._1))))

    // result
    item_rdd2.map(f => ItemSimilar(f._1, f._2, f._3))
  }

}
