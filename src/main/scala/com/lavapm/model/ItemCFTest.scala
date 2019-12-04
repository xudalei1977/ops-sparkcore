package com.lavapm.model

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


/**
  * Item collaborative filtering Test
  * Created by dalei on 10/26/18.
  */
object ItemCFTest {

  def main(args: Array[String]) {

    val spark = SparkSession
                .builder()
                .appName("ItemCFTest")
                .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    // read the data
//    val data_path = "/user/dmp_app_user/sample_itemcf2.txt"
//    val data = sc.textFile(data_path)
//    val user_data = data.map(_.split(",")).map(f => (ItemPrefer(f(0), f(1), f(2).toDouble))).cache()

    import spark.implicits._

    val user_data = spark.sql("select member_no, item_name, cast(sum(cast(item_qty as int)) as double) from sandbox_mk.raw group by member_no, item_name")
                    .map(row => (ItemPrefer(row.getString(0), row.getString(1), row.getDouble(2)))).cache

    // create the model
    val similar = new ItemSimilarity()
    val similar_rdd = similar.Similarity(user_data.rdd, "cooccurrence")
    val recommendedItem = new RecommendedItem
    val recommend_rdd = recommendedItem.Recommend(similar_rdd, user_data.rdd, 30)

    similar_rdd.map(itemSimilar => itemSimilar.itemid1 + ", " + itemSimilar.itemid2 + ", " + itemSimilar.similar)
                .saveAsTextFile("/user/dmp_app_user/output/item_similar.csv")

    recommend_rdd.map(userRecommend => userRecommend.userid + ", " + userRecommend.itemid + ", " + userRecommend.prefer)
                .saveAsTextFile("/user/dmp_app_user/output/user_recommend.csv")

    // result
//    println(s"Item similarity matrix: ${similar_rdd.count()}")
//    similar_rdd.collect().foreach { itemSimilar =>
//      println(itemSimilar.itemid1 + ", " + itemSimilar.itemid2 + ", " + itemSimilar.similar)
//    }

//    println(s"Item recommended to user: ${recommend_rdd.count()}")
//    recommend_rdd.collect().foreach { userRecommend =>
//      println(userRecommend.userid + ", " + userRecommend.itemid + ", " + userRecommend.prefer)
//    }

  }
}
