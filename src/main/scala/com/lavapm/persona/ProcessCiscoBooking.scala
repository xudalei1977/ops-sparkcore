package com.lavapm.persona

import com.lavapm.utils.{Hdfs, UUID}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession


/**
  * Process Data from Cisco Booking
  * Create by ryan on 03/11/19
  */
object ProcessCiscoBooking {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("Process the cisco booking data")
      .enableHiveSupport()
      //set hive table dynamic import
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .getOrCreate()

    //activity path for saving the activity data
    val activityPath_cisco_booking = "/user/dmp_app_user/tmp/cisco_booking"

    println("******************** before transfer cisco booking data to activity data ")
    transferCiscoBooking2ActivityData(spark,activityPath_cisco_booking)

    println("******************** before transfer cisco booking activity data to hive ")
    transferActivityData2Hive(spark,activityPath_cisco_booking)

    println("******************** before transfer cisco booking activity data to HBase ")
    transferActivityData2HBase(spark)

    spark.stop()
  }


  /**
    * transfer the cisco booking data to activity data and save in hdfs
    * @param spark          spark session
    * @param activityPath   hdfs path for activity path
    */
  def transferCiscoBooking2ActivityData(spark: SparkSession,activityPath:String) = {

    //select necessary fields from hive table sandbox_cisco.sql_booking_rawdata left outer join persona.user_mobile_enc and create data frame
    val df = spark
      .sql("SELECT b.lava_id,a.mailaddress_enc,a.busphone_enc,a.data_source,a.marketing_topic,a.campaign,a.country_contact,a.lead_channel," +
        "a.lead_channel_program,a.sales_coverage_code,a.subsales_coverage_code,a.inbound_outbound_flag,a.response_channel,a.activity_date," +
        "a.oppty_forecast_status,a.oppty_stage,oppty_status,a.marketing_intent_technology,a.purchase_intent_technology,a.actual_purchase_technology" +
        " FROM sandbox_cisco.sql_booking_rawdata a left outer join persona.user_mobile_enc b on a.busphone_enc = b.mobile_enc")

    //process df to hiveRDD for saving in hdfs next
    val hiveRDD = df.rdd
      .map(row => {
        var lava_id, email_enc, mobile_enc, data_source, marketing_topic, campaign, country, lead_channel, lead_program, sales_coverage,
        subsales_coverage, inbound_outbound, response_channel, activity_date, oppty_forecast_status, oppty_stage,
        oppty_status, marketing_intent_tech, purchase_intent_tech, actual_purchase_tech, dt = ""

        if (row.getAs[String]("lava_id") != null) {
          lava_id = row.getAs[String]("lava_id").trim
        } else {
          lava_id = UUID.generateUUID()
        }

        if (row.getAs[String]("mailaddress_enc") != null && row.getAs[String]("mailaddress_enc").trim != "")
          email_enc = row.getAs[String]("mailaddress_enc").trim

        if (row.getAs[String]("busphone_enc") != null && row.getAs[String]("busphone_enc").trim != "")
          mobile_enc = row.getAs[String]("busphone_enc").trim

        if (row.getAs[String]("data_source") != null && row.getAs[String]("data_source").trim != "")
          data_source = row.getAs[String]("data_source").trim

        if (row.getAs[String]("marketing_topic") != null && row.getAs[String]("marketing_topic").trim != "")
          marketing_topic = row.getAs[String]("marketing_topic").trim

        if (row.getAs[String]("campaign") != null && row.getAs[String]("campaign").trim != "")
          campaign = row.getAs[String]("campaign").trim

        if (row.getAs[String]("country_contact") != null && row.getAs[String]("country_contact").trim == "CHINA")
          country = "中国"

        if (row.getAs[String]("lead_channel") != null && row.getAs[String]("lead_channel").trim != "")
          lead_channel = row.getAs[String]("lead_channel").trim

        if (row.getAs[String]("lead_channel_program") != null && row.getAs[String]("lead_channel_program").trim != "")
          lead_program = row.getAs[String]("lead_channel_program").trim

        if (row.getAs[String]("sales_coverage_code") != null && row.getAs[String]("sales_coverage_code").trim != "")
          sales_coverage = row.getAs[String]("sales_coverage_code").trim

        if (row.getAs[String]("subsales_coverage_code") != null && row.getAs[String]("subsales_coverage_code").trim != "")
          subsales_coverage = row.getAs[String]("subsales_coverage_code").trim

        if (row.getAs[String]("inbound_outbound_flag") != null && row.getAs[String]("inbound_outbound_flag").trim != "")
          inbound_outbound = row.getAs[String]("inbound_outbound_flag").trim

        if (row.getAs[String]("response_channel") != null && row.getAs[String]("response_channel").trim != "")
          response_channel = row.getAs[String]("response_channel").trim

        if (row.getAs[String]("activity_date") != null && row.getAs[String]("activity_date").trim != "") {
          val time = row.getAs[String]("activity_date").trim
          val timeParts = time.split("-")
          var day = timeParts(0).trim
          if (day.length == 1)
            day = "0" + day

          var mouth = ""
          timeParts(1) match {
            case "Jan" => mouth = "01"
            case "Feb" => mouth = "02"
            case "Mar" => mouth = "03"
            case "Apr" => mouth = "04"
            case "May" => mouth = "05"
            case "Jun" => mouth = "06"
            case "Jul" => mouth = "07"
            case "Aug" => mouth = "08"
            case "Sep" => mouth = "09"
            case "Oct" => mouth = "10"
            case "Nov" => mouth = "11"
            case "Dec" => mouth = "12"
          }

          val year = timeParts(2)

          activity_date = year + "-" + mouth + "-" + day
          dt = activity_date
        }

        if (row.getAs[String]("oppty_forecast_status") != null && row.getAs[String]("oppty_forecast_status").trim != "")
          oppty_forecast_status = row.getAs[String]("oppty_forecast_status").trim

        if (row.getAs[String]("oppty_stage") != null && row.getAs[String]("oppty_stage").trim != "")
          oppty_stage = row.getAs[String]("oppty_stage").trim

        if (row.getAs[String]("oppty_status") != null && row.getAs[String]("oppty_status").trim != "")
          oppty_status = row.getAs[String]("oppty_status").trim

        if (row.getAs[String]("marketing_intent_technology") != null && row.getAs[String]("marketing_intent_technology").trim != "")
          marketing_intent_tech = row.getAs[String]("marketing_intent_technology").trim

        if (row.getAs[String]("purchase_intent_technology") != null && row.getAs[String]("purchase_intent_technology").trim != "")
          purchase_intent_tech = row.getAs[String]("purchase_intent_technology").trim

        if (row.getAs[String]("actual_purchase_technology") != null && row.getAs[String]("actual_purchase_technology").trim != "")
          actual_purchase_tech = row.getAs[String]("actual_purchase_technology").trim

        lava_id + "," + email_enc + "," + mobile_enc + "," + data_source + "," + marketing_topic + "," + campaign + "," +
          country + "," + lead_channel + "," + lead_program + "," + sales_coverage + "," + subsales_coverage + "," +
          inbound_outbound + "," + response_channel + "," + activity_date + "," + oppty_forecast_status + "," + oppty_stage + "," +
          oppty_status + "," + marketing_intent_tech + "," + purchase_intent_tech + "," + actual_purchase_tech + "," + dt

      })

    // delete the target hdfs path if exists
    Hdfs.deleteFile(spark, activityPath)
    hiveRDD.saveAsTextFile(activityPath)
    println("******************************* after saving as text file")

  }


  /**
    * transfer activity data in hdfs to persona.activity_cisco_booking in hive
    * @param spark          spark session
    * @param activityPath   hdfs path for activity data
    */
  def transferActivityData2Hive(spark: SparkSession,activityPath:String) = {
    //create table tmp.cisco_tmp and load activity data
    spark.sql("drop table if exists tmp.cisco_tmp")
    spark.sql("create table tmp.cisco_tmp(" +
      "lava_id STRING COMMENT 'LavaHEAT 用户或者设备的唯一标识'," +
      "email_enc STRING COMMENT '加密的邮箱地址'," +
      "mobile_enc STRING COMMENT '加密的手机号'," +
      "data_source STRING COMMENT '数据源'," +
      "marketing_topic STRING COMMENT '营销主题'," +
      "campaign STRING COMMENT '活动名'," +
      "country STRING COMMENT '联系人所属国家'," +
      "lead_channel STRING COMMENT '留资渠道'," +
      "lead_program STRING COMMENT '留资活动'," +
      "sales_coverage STRING COMMENT '销售覆盖区域'," +
      "subsales_coverage STRING COMMENT '销售覆盖子区域'," +
      "inbound_outbound STRING COMMENT '内外部标识'," +
      "response_channel STRING COMMENT '响应渠道'," +
      "activity_date STRING COMMENT '活跃日期'," +
      "oppty_forecast_status STRING COMMENT '预测到的商机的状态'," +
      "oppty_stage STRING COMMENT '商机阶段'," +
      "oppty_status STRING COMMENT '商机状态'," +
      "marketing_intent_tech STRING COMMENT '营销关注的技术'," +
      "purchase_intent_tech STRING COMMENT '购买关注的技术'," +
      "actual_purchase_tech STRING COMMENT '实际购买的技术'," +
      "dt STRING COMMENT '日期'" +
      ") COMMENT 'Cisco SQL Booking数据临时表' " +
      "ROW FORMAT delimited fields " +
      "terminated by ',' " +
      "lines terminated by '\\n'")

    //load activity data to table tmp.cisco_tmp
    spark.sql("LOAD DATA INPATH '" + activityPath + "' INTO TABLE tmp.cisco_tmp")

    //distinct lava_id for the same mobile_enc
    spark.sql("drop table if exists tmp.ciscobooking_lavaid_distinct")
    spark.sql("create table tmp.ciscobooking_lavaid_distinct stored AS parquet as select max(lava_id) as lava_id,mobile_enc " +
      "from tmp.cisco_tmp group by mobile_enc")

    //refresh the table tmp.cisco_tmp
    spark.sql("INSERT OVERWRITE TABLE tmp.cisco_tmp SELECT a.lava_id,b.email_enc,b.mobile_enc,b.data_source,b.marketing_topic," +
      "b.campaign,b.country,b.lead_channel,b.lead_program,b.sales_coverage,b.subsales_coverage,b.inbound_outbound,b.response_channel," +
      "b.activity_date,b.oppty_forecast_status,b.oppty_stage,b.oppty_status,b.marketing_intent_tech,b.purchase_intent_tech,b.actual_purchase_tech," +
      "b.dt FROM tmp.ciscobooking_lavaid_distinct a,tmp.cisco_tmp b WHERE a.mobile_enc = b.mobile_enc")

    //create partitioned table persona.activity_cisco_booking and insert tmp.cisco_tmp data to it
    spark.sql("drop table if exists persona.activity_cisco_booking")
    spark.sql("create table persona.activity_cisco_booking(" +
      "lava_id STRING COMMENT 'LavaHEAT 用户或者设备的唯一标识'," +
      "email_enc STRING COMMENT '加密的邮箱地址'," +
      "mobile_enc STRING COMMENT '加密的手机号'," +
      "data_source STRING COMMENT '数据源'," +
      "marketing_topic STRING COMMENT '营销主题'," +
      "campaign STRING COMMENT '活动名'," +
      "country STRING COMMENT '联系人所属国家'," +
      "lead_channel STRING COMMENT '留资渠道'," +
      "lead_program STRING COMMENT '留资活动'," +
      "sales_coverage STRING COMMENT '销售覆盖区域'," +
      "subsales_coverage STRING COMMENT '销售覆盖子区域'," +
      "inbound_outbound STRING COMMENT '内外部标识'," +
      "response_channel STRING COMMENT '响应渠道'," +
      "activity_date STRING COMMENT '活跃日期'," +
      "oppty_forecast_status STRING COMMENT '预测到的商机的状态'," +
      "oppty_stage STRING COMMENT '商机阶段'," +
      "oppty_status STRING COMMENT '商机状态'," +
      "marketing_intent_tech STRING COMMENT '营销关注的技术'," +
      "purchase_intent_tech STRING COMMENT '购买关注的技术'," +
      "actual_purchase_tech STRING COMMENT '实际购买的技术'" +
      ") COMMENT 'Cisco SQL Booking数据表' " +
      "PARTITIONED BY (dt string COMMENT '以天分区：yyyy-mm-dd') " +
      "ROW FORMAT delimited fields " +
      "terminated by '\\t' " +
      "lines terminated by '\\n'")

    //insert activity data into final table persona.activity_cisco_booking
    spark.sql("INSERT INTO TABLE persona.activity_cisco_booking PARTITION (dt) SELECT DISTINCT * FROM tmp.cisco_tmp")
  }


  /**
    * transfer persona.activity_cisco_booking to persona:user_info in HBase
    * @param spark    spark session
    */
  def transferActivityData2HBase(spark: SparkSession): Unit = {

    try{
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "persona:user_info")

      lazy val job = Job.getInstance(hbaseConf)
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[Result])
      job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

      //create cisco booking data frame
      val df = spark.sql("select lava_id,email_enc,mobile_enc,response_channel,marketing_intent_tech,purchase_intent_tech," +
        "actual_purchase_tech,country from persona.activity_cisco_booking")

      //process the data
      val hbaseRDD = df.rdd
        .map(row => {
          val lava_id = row.getAs[String]("lava_id")
          val put = new Put(Bytes.toBytes(lava_id))

          // check the email_enc
          if (row.getAs[String]("email_enc").trim != "")
            put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("email_enc"), Bytes.toBytes(row.getAs[String]("email_enc").trim))

          // check the mobile_enc
          if (row.getAs[String]("mobile_enc").trim != "")
            put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("mobile_enc"), Bytes.toBytes(row.getAs[String]("mobile_enc").trim))

          // check the response_channel
          if (row.getAs[String]("response_channel").trim != "")
            put.addColumn(Bytes.toBytes("tob"), Bytes.toBytes("response_channel"), Bytes.toBytes(row.getAs[String]("response_channel").trim))

          // check the marketing_intent_tech
          if (row.getAs[String]("marketing_intent_tech").trim != "")
            put.addColumn(Bytes.toBytes("tob"), Bytes.toBytes("market_intent_tech"), Bytes.toBytes(row.getAs[String]("marketing_intent_tech").trim))

          // check the purchase_intent_tech
          if (row.getAs[String]("purchase_intent_tech").trim != "")
            put.addColumn(Bytes.toBytes("tob"), Bytes.toBytes("purchase_intent_tech"), Bytes.toBytes(row.getAs[String]("purchase_intent_tech").trim))

          // check the actual_purchase_tech
          if (row.getAs[String]("actual_purchase_tech").trim != "")
            put.addColumn(Bytes.toBytes("tob"), Bytes.toBytes("actual_purchase_tech"), Bytes.toBytes(row.getAs[String]("actual_purchase_tech").trim))

          // check the country
          if (row.getAs[String]("country").trim != "")
            put.addColumn(Bytes.toBytes("geo"), Bytes.toBytes("resident_country"), Bytes.toBytes(row.getAs[String]("country").trim))

          // add the interested brand_tag
          put.addColumn(Bytes.toBytes("interest"), Bytes.toBytes("brand_tag"), Bytes.toBytes("Cisco"))

          (new ImmutableBytesWritable, put)
        })

      hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration())
      println("*********** hbase table saved")
    } catch {
      case ex: Exception => {println("*************** ex := " + ex.toString)}
    }
  }

}
