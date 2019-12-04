package com.lavapm.model

import scala.math._
import org.apache.spark.rdd.RDD


/**
  * Recommended Item
  * Created by dalei on 10/26/18.
  */

class RecommendedItem {


  /**
    * Recommend item to user.
    * @param items_similar          item similarity
    * @param user_prefer            user prefer point
    * @param recommend_number       item number recommended to user
    * @return RDD[UserRecommend]    recommended items to user
    *
    */
  def Recommend(items_similar: RDD[ItemSimilar],
                user_prefer: RDD[ItemPrefer],
                recommend_number: Int): (RDD[UserRecommend]) = {

    // prepare the data, (item1, item2, similar) (user, item, prefer)
    val items_similar1 = items_similar.map(f => (f.itemid1, f.itemid2, f.similar))
    val user_prefer1 = user_prefer.map(f => (f.userid, f.itemid, f.prefer))

    // (item1, (item2, similar)) join (item, (user, prefer)) => (item1, ((item2, similar), (user, prefer)))
    // (item1, ((item2, similar), (user, prefer))) => ((user, item2), prefer * similar)
    val items_similar2 = items_similar1.map(f => (f._1, (f._2, f._3)))
    val user_prefer2 = user_prefer1.map(f => (f._2, (f._1, f._3)))

    val user_item1 = items_similar2.join(user_prefer2).map(f => ((f._2._2._1, f._2._1._1), f._2._2._2 * f._2._1._2)).reduceByKey(_+_)

    // ((user, item2), (prefer * similar, 1 or None)) => (user, CompactBuffer((item2, prefer * similar), (item2, prefer * similar)...))
    // if f._2._2 is 1, it means this (user, item) pair exist already.
    val user_prefer3 = user_prefer1.map(f => ((f._1, f._2), 1))
    val user_item2 = user_item1.leftOuterJoin(user_prefer3).filter(f => f._2._2.isEmpty).map(f => (f._1._1, (f._1._2, f._2._1))).groupByKey

    // sort items on point and remove the out of scope items, (user, CompactBuffer((item2, prefer * similar), (item2, prefer * similar)...))
    val user_item3 = user_item2.map(f => {
      val item = f._2.toBuffer
      val item_sorted = item.sortBy(_._2)
      if (item_sorted.length > recommend_number)
        item_sorted.remove(0, (item_sorted.length - recommend_number))

      (f._1, item_sorted)
    })


    val user_item4 = user_item3.flatMap(f => {
      val item = f._2
      for (w <- item) yield (f._1, w._1, w._2)
    })

    user_item4.map(f => UserRecommend(f._1, f._2, f._3))
  }


}
