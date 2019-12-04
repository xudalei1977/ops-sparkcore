package com.lavapm.persona

import com.lavapm.utils.Hdfs
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * count the frequency of interest tag of one batch number.
  * create by Ryan on 04/01/2019.
  */
object ProcessInterestTag {
  def main(args: Array[String]): Unit = {
    // received parameter batch number
    val batch_no = args(0)
    val spark = SparkSession
      .builder()
      .appName("Process the interest tag")
      .enableHiveSupport()
      //set hive table dynamic import
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .getOrCreate()

    // hdfs path for the counting result
    val tagPath = s"/user/dmp_app_user/interest/"+ batch_no +""

    println("*************before count interest tag***************")
    countInterestTag(spark,batch_no,tagPath)
    spark.stop()
  }

  /**
    * count the frequency of interest tag of one batch no.
    * @param spark      spark session
    * @param batch_no   batch number of interest tag
    * @param tagPath    hdfs path for the result of interest tag counts
    */
  def countInterestTag(spark: SparkSession,
                       batch_no:String,
                       tagPath:String):Unit = {

    val dataFrame = spark.sql(s"select interest_tag from persona.user_interest_tag where" +
                                      s" batch_no = '"+ batch_no +"'" + " and trim(interest_tag) != ''")

    val rdd = dataFrame.rdd
      .map(row => {
        val tags = row.getAs[String]("interest_tag").split(",")
        val array = new ArrayBuffer[String]()

        val strBuilder = new StringBuilder("")
        var map = Map("" -> "")

        for (tag <- tags) {
          if (tag.indexOf("-") > 0) {
            array.append(tag.split("-")(0))
            array.append(tag)
          } else if (tag.trim != "") {
            array.append(tag)
          }
        }

        // this map's key is for distinction of the same tag in one line
        for (tag <- array) map += (tag -> "")

        // concat the tag for return
        for (tag <- map.keySet) {
          if (tag != "")
            strBuilder.append(tag + ",")
        }

        strBuilder.substring(0, strBuilder.length - 1).toString

      })
      .flatMap(_.split(","))
      .filter(_ != "")
      .map((_, 1))
      .reduceByKey(_ + _)
      .map(line => line._1 + ":" + line._2)

    Hdfs.deleteFile(spark,tagPath)
    rdd.coalesce(1).saveAsTextFile(tagPath)
  }
}
