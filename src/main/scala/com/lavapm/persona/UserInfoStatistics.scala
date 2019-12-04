package com.lavapm.persona

import java.time.LocalDate

import com.lavapm.utils.Hive
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Make statistics on our data assets for easy display.
  * Create by Ryan on 06/08/2019.
  */
object UserInfoStatistics {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("User Info Statistics")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val today = LocalDate.now()

    deviceIdStatistic(spark,today)
    println("*****after device id statistic")

    ageRangeStatistic(spark)
    println("*****after age range statistic")

    genderStatistic(spark)
    println("*****after gender statistic")

    interestTagStatistic(spark)
    println("*****after interest tag statistic")

    provinceStatistic(spark)
    println("*****after province statistic")

    cityStatistic(spark)
    println("*****after city statistic")

    spark.stop()
  }

  /**
    * device id statistic, e.g. "imei","idfa","mobile","email" ...
    * @param spark  spark session
    */
  def deviceIdStatistic(spark:SparkSession,today:LocalDate) = {

    var flag = false

    val statisticDF = spark
      .sql("SELECT count(*) as total_sum,count(DISTINCT imei) as imei_sum,count(DISTINCT imei_enc) as imei_enc_sum," +
        "count(DISTINCT idfa) as idfa_sum,count(DISTINCT idfa_enc) as idfa_enc_sum,count(DISTINCT mobile) as mobile_sum," +
        "count(DISTINCT mobile_enc) as mobile_enc_sum,count(DISTINCT email) as email_sum,count(DISTINCT email_enc) as email_enc_sum," +
        "from_unixtime(unix_timestamp(),'yyyy-MM-dd') as dt from persona.user_info")

    if (spark.catalog.tableExists("persona.device_id_statistic")){
      statisticDF
        .write
        .insertInto("persona.device_id_statistic")

      flag = true
    } else {
      statisticDF
        .write
        .saveAsTable("persona.device_id_statistic")
    }

    if (flag) {
      val increaseDF = spark
        .sql("select (a.total_sum - b.total_sum) as total_increase,(a.imei_sum - b.imei_sum) as imei_increase," +
          "(a.imei_enc_sum - b.imei_enc_sum) as imei_enc_increase,(a.idfa_sum - b.idfa_sum) as idfa_increase," +
          "(a.idfa_enc_sum - b.idfa_enc_sum) as idfa_enc_increase,(a.mobile_sum - b.mobile_sum) as mobile_increase," +
          "(a.mobile_enc_sum - b.mobile_enc_sum) as mobile_enc_increase,(a.email_sum - b.email_sum) as email_increase," +
          "(a.email_enc_sum - b.email_enc_sum) as email_enc_increase,from_unixtime(unix_timestamp(),'yyyy-MM-dd') as dt " +
          "from persona.device_id_statistic as a cross join persona.device_id_statistic as b on 1 = 1 " +
          "where a.dt = '" + today + "' and b.dt = '" + today.minusDays(1) + "'")

      if (spark.catalog.tableExists("persona.device_id_increase")){
        increaseDF
          .write
          .insertInto("persona.device_id_increase")
      } else {
        increaseDF
          .write
          .saveAsTable("persona.device_id_increase")
      }
    }


  }


  /**
    * province age range statistic
    * @param spark  spark session
    */
  def ageRangeStatistic(spark:SparkSession) = {

    spark.sql("drop table if exists persona.age_range_statistic")

    spark
      .sql("SELECT age_range,count(*) as sum from persona.user_info WHERE age_range != '' GROUP BY age_range")
      .write
      .saveAsTable("persona.age_range_statistic")

    spark.sql("drop table if exists persona.province_age_range_statistic")
    val rdd = spark
      .sql("SELECT concat_ws('-',resident_province,age_range) as province_age_range,count(*) as province_sum from persona.user_info " +
        "WHERE resident_province != '' and age_range != '' GROUP BY concat_ws('-',resident_province,age_range)")
      .rdd
      .map(row => {
        val province_age_range = row.getAs[String]("province_age_range")
        val province_sum = row.getAs[Long]("province_sum")
        Row(province_age_range.split("-")(0), province_age_range.split("-")(1), province_sum.toString)
      })

    spark
      .createDataFrame(rdd,Hive.generateSchema("province,age_range,sum",","))
      .write
      .saveAsTable("persona.province_age_range_statistic")

  }


  /**
    * province gender statistic
    * @param spark  spark session
    */
  def genderStatistic(spark:SparkSession) = {

    spark.sql("drop table if exists persona.gender_statistic")

    spark
      .sql("SELECT gender,count(*) as sum from persona.user_info WHERE gender != '' GROUP BY gender")
      .write
      .saveAsTable("persona.gender_statistic")

    spark.sql("drop table if exists persona.province_gender_statistic")
    val rdd = spark
      .sql("SELECT concat_ws('-',resident_province,gender) as province_gender,count(*) as province_sum from persona.user_info " +
        "WHERE resident_province != '' and gender != '' GROUP BY concat_ws('-',resident_province,gender)")
      .rdd
      .map(row => {
        val province_gender = row.getAs[String]("province_gender")
        val province_sum = row.getAs[Long]("province_sum")
        Row(province_gender.split("-")(0), province_gender.split("-")(1), province_sum.toString)
      })

    spark
      .createDataFrame(rdd,Hive.generateSchema("province,gender,sum",","))
      .write
      .saveAsTable("persona.province_gender_statistic")
  }


  /**
    * 1st and 2nd class interest tag statistic
    * @param spark  spark session
    */
  def interestTagStatistic(spark:SparkSession) = {
    val df = spark.sql(s"select interest_tag from persona.user_info where interest_tag != ''")

    val rdd1 = generateInterest(df.rdd,0)

    spark.sql("drop table if exists persona.first_class_interest_statistic")
    spark
      .createDataFrame(rdd1,Hive.generateSchema("first_class_interest,sum",","))
      .write
      .saveAsTable("persona.first_class_interest_statistic")


    val rdd2 = generateInterest(df.rdd,1)

    spark.sql("drop table if exists persona.second_class_interest_statistic")
    spark
      .createDataFrame(rdd2,Hive.generateSchema("second_class_interest,sum,first_class_interest",","))
      .write
      .saveAsTable("persona.second_class_interest_statistic")
  }


  /**
    * distinct interest for one record and count the interests
    * @param rdd RDD[Row]
    * @param num number of the splits
    */
  def generateInterest(rdd:RDD[Row], num:Int) = {
    rdd
      .map(row => {
        val tags = row.getAs[String]("interest_tag").split(",")
        val array = new ArrayBuffer[String]()

        val strBuilder = new StringBuilder("")
        var map = Map("" -> "")

        for (tag <- tags) {
          if (tag.indexOf("-") > 0) {
            if (num == 1)
              array.append(tag)
            if (num == 0)
              array.append(tag.split("-")(0))

          } else if (num == 0 && tag.trim != "") {
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

        if (strBuilder.length > 0)
          strBuilder.substring(0, strBuilder.length - 1).toString
        else
          ""

      })
      .flatMap(_.split(","))
      .filter(_ != "")
      .map((_, 1))
      .reduceByKey(_ + _)
      .map(line => {
        if (line._1.indexOf("-") > 0)
          Row(line._1.split("-")(1),line._2.toString,line._1.split("-")(0))
        else
          Row(line._1,line._2.toString)
      })
  }


  /**
    * province statistic
    * @param spark  spark session
    */
  def provinceStatistic(spark:SparkSession) = {
    spark.sql("drop table if exists persona.province_statistic")

    spark
      .sql("SELECT resident_province,count(*) as sum from persona.user_info WHERE resident_province != '' GROUP BY resident_province")
      .write
      .saveAsTable("persona.province_statistic")
  }


  /**
    * city statistic
    * @param spark  spark session
    */
  def cityStatistic(spark:SparkSession) = {
    spark.sql("drop table if exists persona.city_statistic")

    spark
      .sql("SELECT resident_city,count(*) as sum from persona.user_info WHERE resident_city != '' GROUP BY resident_city")
      .write
      .saveAsTable("persona.city_statistic")
  }

}
