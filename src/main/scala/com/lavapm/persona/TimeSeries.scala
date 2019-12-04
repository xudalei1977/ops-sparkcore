package com.lavapm.persona

import java.text.SimpleDateFormat
import com.lavapm.utils.Hive
import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Get interval time from user's journey in hive table persona.activity_cisco_booking.
  * Create by ryan on 07/30/2019.
  */
object TimeSeries {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Time Series")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    spark.sql("drop table if exists persona.cisco_booking_time")
    spark.sql("create table persona.cisco_booking_time like persona.activity_cisco_booking")

    val bookingTimeSchema = "lava_id,email_enc,mobile_enc,data_source,marketing_topic,campaign,country,lead_channel,lead_program," +
      "sales_coverage,subsales_coverage,inbound_outbound,response_channel,activity_date,oppty_forecast_status,oppty_stage," +
      "oppty_status,marketing_intent_tech,purchase_intent_tech,actual_purchase_tech,dt"

    spark
      .createDataFrame(addTime(spark),Hive.generateSchema(bookingTimeSchema,","))
      .write
      .insertInto("persona.cisco_booking_time")

    println("********** after saving persona.cisco_booking_time")


    val intervalTimeSchema = "lava_id,email_enc,mobile_enc,data_source,marketing_topic,campaign,country,lead_channel,lead_program," +
      "sales_coverage,subsales_coverage,inbound_outbound,response_channel,activity_date,oppty_forecast_status,oppty_stage," +
      "oppty_status,marketing_intent_tech,purchase_intent_tech,actual_purchase_tech,interval_time,dt"

    spark
      .createDataFrame(timeSeries(spark),Hive.generateSchema(intervalTimeSchema,","))
      .write
      .insertInto("persona.cisco_interval_time")

    spark.stop()
  }


  /**
    * add random time for field activity_data like "yyyy-MM-dd" => "yyyy-MM-dd HH:mm:ss".
    * @param spark  spark session
    * @return       RDD[Row]
    */
  def addTime (spark:SparkSession) = {
    spark
      .sql("select * from persona.activity_cisco_booking")
      .rdd
      .map(row => {
        val lava_id = row.getAs[String]("lava_id")
        val email_enc = row.getAs[String]("email_enc")
        val mobile_enc = row.getAs[String]("mobile_enc")
        val data_source = row.getAs[String]("data_source")
        val marketing_topic = row.getAs[String]("marketing_topic")
        val campaign = row.getAs[String]("campaign")
        val country = row.getAs[String]("country")
        val lead_channel = row.getAs[String]("lead_channel")
        val lead_program = row.getAs[String]("lead_program")
        val sales_coverage = row.getAs[String]("sales_coverage")
        val subsales_coverage = row.getAs[String]("subsales_coverage")
        val inbound_outbound = row.getAs[String]("inbound_outbound")
        val response_channel = row.getAs[String]("response_channel")
        var h = Random.nextInt(24) + ""
        var m = Random.nextInt(60) + ""
        var s = Random.nextInt(60) + ""
        if (h.toInt < 10) h = "0"+h
        if (m.toInt < 10) m = "0"+m
        if (s.toInt < 10) s = "0"+s
        val activity_date = row.getAs[String]("activity_date") + " "+ h + ":" + m + ":" + s
        val oppty_forecast_status = row.getAs[String]("oppty_forecast_status")
        val oppty_stage = row.getAs[String]("oppty_stage")
        val oppty_status = row.getAs[String]("oppty_status")
        val marketing_intent_tech = row.getAs[String]("marketing_intent_tech")
        val purchase_intent_tech = row.getAs[String]("purchase_intent_tech")
        val actual_purchase_tech = row.getAs[String]("actual_purchase_tech")
        val dt = row.getAs[String]("dt")

        Row(lava_id,email_enc,mobile_enc,data_source,marketing_topic,campaign,country,lead_channel,lead_program,sales_coverage,subsales_coverage,
          inbound_outbound,response_channel,activity_date,oppty_forecast_status,oppty_stage,oppty_status,marketing_intent_tech,purchase_intent_tech,
          actual_purchase_tech,dt)
      })
  }


  /**
    * get the interval time between user's each visit and it's last visit to generate a new field:"interval_time".
    * @param spark  spark session
    * @return       RDD[Row]
    */
  def timeSeries(spark:SparkSession) = {
    val dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    spark
      .sql("select * from persona.cisco_booking_time")
      .rdd
      .groupBy(_.getAs[String]("lava_id"))
      .map(line => {
        line._2
          .toList
          .sortBy(row => dataFormat.parse(row.getAs[String]("activity_date")).getTime)
      })
      .map(list => {
        val rowList = ListBuffer[Row]()
        for (i <- 0 until list.length) {
          val lava_id = list(i).getAs[String]("lava_id")
          val email_enc = list(i).getAs[String]("email_enc")
          val mobile_enc = list(i).getAs[String]("mobile_enc")
          val data_source = list(i).getAs[String]("data_source")
          val marketing_topic = list(i).getAs[String]("marketing_topic")
          val campaign = list(i).getAs[String]("campaign")
          val country = list(i).getAs[String]("country")
          val lead_channel = list(i).getAs[String]("lead_channel")
          val lead_program = list(i).getAs[String]("lead_program")
          val sales_coverage = list(i).getAs[String]("sales_coverage")
          val subsales_coverage = list(i).getAs[String]("subsales_coverage")
          val inbound_outbound = list(i).getAs[String]("inbound_outbound")
          val response_channel = list(i).getAs[String]("response_channel")
          val activity_date = list(i).getAs[String]("activity_date")
          val oppty_forecast_status = list(i).getAs[String]("oppty_forecast_status")
          val oppty_stage = list(i).getAs[String]("oppty_stage")
          val oppty_status = list(i).getAs[String]("oppty_status")
          val marketing_intent_tech = list(i).getAs[String]("marketing_intent_tech")
          val purchase_intent_tech = list(i).getAs[String]("purchase_intent_tech")
          val actual_purchase_tech = list(i).getAs[String]("actual_purchase_tech")
          val dt = list(i).getAs[String]("dt")
          var interval_time = "0"
          //the visit time are sorted,so if i=0,it's the first time of visit, else the interval time between two visits is like below
          if (i != 0)
            interval_time = (dataFormat.parse(list(i).getAs[String]("activity_date")).getTime - dataFormat.parse(list(i - 1).getAs[String]("activity_date")).getTime) / 1000 + ""

          rowList += Row(lava_id, email_enc, mobile_enc, data_source, marketing_topic, campaign, country, lead_channel, lead_program, sales_coverage, subsales_coverage,
            inbound_outbound, response_channel, activity_date, oppty_forecast_status, oppty_stage, oppty_status, marketing_intent_tech, purchase_intent_tech,
            actual_purchase_tech, interval_time, dt)
        }

        rowList
      })
        .flatMap(_.toList)
  }
}
