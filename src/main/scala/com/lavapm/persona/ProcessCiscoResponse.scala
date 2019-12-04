package com.lavapm.persona

import com.lavapm.utils._
import java.text.SimpleDateFormat

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

/**
  * Process Data from Cisco Response
  * Create by ryan on 03/11/19
  */
object ProcessCiscoResponse {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Process the cisco response data")
      .enableHiveSupport()
      //set hive table dynamic import
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .getOrCreate()

    //activity path for saving activity data
    val activityPath_cisco_response = "/user/dmp_app_user/tmp/cisco_response"

    println("******************** before transfer cisco response data to activity data ")
    transferCiscoResponse2ActivityData(spark,activityPath_cisco_response)

    println("******************** before transfer cisco response activity data to hive ")
    transferActivityData2Hive(spark,activityPath_cisco_response)

    println("******************** before transfer cisco response activity data to HBase ")
    transferActivityData2HBase(spark)

    spark.stop()
  }


  /**
    * transfer the cisco response data to activity data and save in hdfs
    * @param spark          spark session
    * @param activityPath   hdfs path for activity path
    */
  def transferCiscoResponse2ActivityData(spark: SparkSession,activityPath:String) = {

    //get email to lava_id rdd and mobile to lava_id rdd from HBase table persona:user_info
    val userEmailEncRDD = Hive.transferEmailEncFromUserInfo2UserEmailEncRDD(spark)
    val userEmailRDD = Hive.transferEmailFromUserInfo2UserEmailRDD(spark)
    val userMobileEncRDD = Hive.transferMobileEncFromUserInfo2UserMobileEncRDD(spark)
    val userMobileRDD = Hive.transferMobileFromUserInfo2UserMobileRDD(spark)

    //select necessary fields from hive table sandbox_cisco.responses_rawdata and create data frame
    val df = spark
      .sql("SELECT mailaddress_enc,busphone_enc,source_system_name,flag_registration,call_center_status_code," +
        "communication_method_type_code,mktg_transaction_type_code,response_date,call_center_vendor,country_code," +
        "leads_channel_program,campaign_name FROM sandbox_cisco.responses_rawdata")

    //get email to row rdd from df
    val emailRDD = df.rdd
      .map(row => {
        if (row.getAs[String]("mailaddress_enc") != null && row.getAs[String]("mailaddress_enc").trim != "")
          (row.getAs[String]("mailaddress_enc").trim, row)
        else
          ("",null)
      })

    //get mobile to row rdd from df
    val mobileRDD = df.rdd
      .map(row => {
        if (row.getAs[String]("busphone_enc") != null && row.getAs[String]("busphone_enc").trim != "")
          (row.getAs[String]("busphone_enc").trim, row)
        else
          ("",null)
      })

    //cisco response data join our user info data for mapping the email or mobile which is already in local database
    //get their lava_id and then union the mapped data to get matchRDD
    val matchRDD = (emailRDD.join(userEmailEncRDD))
      .union(emailRDD.join(userEmailRDD))
      .union(mobileRDD.join(userMobileEncRDD))
      .union(mobileRDD.join(userMobileRDD))

    //get the row to lava_id rdd from matchRDD, for there maybe some records exist in both emailRDD and mobileRDD,so distinct it
    val lavaIdRDD = matchRDD
      .map(_._2)
      .distinct()

    //transfer df to row rdd and subtract the matched row to get the unmatched rdd, map the unmatched rdd to mail to row pair rdd or mobile to row pair rdd
    val unMatchRDD = df
      .rdd
      .subtract(lavaIdRDD.map(_._1))
      .map(row => {
        if (row.getAs[String]("mailaddress_enc") != null && row.getAs[String]("mailaddress_enc").trim != "")
          (row.getAs[String]("mailaddress_enc").trim, row)
        else if (row.getAs[String]("busphone_enc") != null && row.getAs[String]("busphone_enc").trim != "")
          (row.getAs[String]("busphone_enc").trim, row)
        else
          ("",null)
      })

    //map the unMatchRDD to get the mail and mobile, filter empty string,distinct and create lava_id for them
    val newLavaIdRDD = unMatchRDD
      .map(_._1)
      .filter(_ != "")
      .distinct()
      .map(x => (x, UUID.generateUUID))

    //unMatchRDD join the newLavaIdRDD and map to get row to lava_id pair rdd
    val noLavaIdRDD = unMatchRDD
      .join(newLavaIdRDD)
      .map(_._2)

    //lavaIdRDD union noLavaIdRDD
    val ciscoRDD = lavaIdRDD.union(noLavaIdRDD)

    //process ciscoRDD to hiveRDD for saving in hdfs next
    val hiveRDD = ciscoRDD
      .map(line => {
        var lava_id, email_enc, mobile_enc, source_system, regist_flag, call_center_status, comm_method, mktg_trans_type,
        response_date, call_center_vendor, country, leads_channel, campaign_name, dt = ""

        lava_id = line._2

        val row = line._1

        if (row.getAs[String]("mailaddress_enc") != null && row.getAs[String]("mailaddress_enc").trim != "")
          email_enc = row.getAs[String]("mailaddress_enc").trim

        if (row.getAs[String]("busphone_enc") != null && row.getAs[String]("busphone_enc").trim != "")
          mobile_enc = row.getAs[String]("busphone_enc").trim

        if (row.getAs[String]("source_system_name") != null && row.getAs[String]("source_system_name").trim != "")
          source_system = row.getAs[String]("source_system_name").trim

        if (row.getAs[String]("flag_registration") != null && row.getAs[String]("flag_registration").trim != "")
          regist_flag = row.getAs[String]("flag_registration").trim

        if (row.getAs[String]("call_center_status_code") != null && row.getAs[String]("call_center_status_code").trim != "")
          call_center_status = row.getAs[String]("call_center_status_code").trim

        if (row.getAs[String]("communication_method_type_code") != null && row.getAs[String]("communication_method_type_code").trim != "")
          comm_method = row.getAs[String]("communication_method_type_code").trim

        if (row.getAs[String]("mktg_transaction_type_code") != null && row.getAs[String]("mktg_transaction_type_code").trim != "")
          mktg_trans_type = row.getAs[String]("mktg_transaction_type_code").trim

        if (row.getAs[String]("response_date") != null && row.getAs[String]("response_date").trim != "") {
          var time = row.getAs[String]("response_date").trim
          if (time.indexOf(" ") > 0)
            time = time.split(" ")(0)

          val date = new SimpleDateFormat("yyyy/M/d").parse(time)
          response_date = new SimpleDateFormat("yyyy-MM-dd").format(date)
          dt = response_date
        }

        if (row.getAs[String]("call_center_vendor") != null && row.getAs[String]("call_center_vendor").trim != "")
          call_center_vendor = row.getAs[String]("call_center_vendor").trim

        if (row.getAs[String]("country_code") != null && row.getAs[String]("country_code").trim == "CN")
          country = "中国"

        if (row.getAs[String]("leads_channel_program") != null && row.getAs[String]("leads_channel_program").trim != "")
          leads_channel = row.getAs[String]("leads_channel_program").trim

        if (row.getAs[String]("campaign_name") != null && row.getAs[String]("campaign_name").trim != "")
          campaign_name = row.getAs[String]("campaign_name").trim

        lava_id + "," + email_enc + "," + mobile_enc + "," + source_system + "," + regist_flag + "," + call_center_status + "," + comm_method +
          "," + mktg_trans_type + "," + response_date + "," + call_center_vendor + "," + country + "," + leads_channel + "," + campaign_name + "," + dt
      })

    // delete the target hdfs path if exists
    Hdfs.deleteFile(spark, activityPath)
    hiveRDD.saveAsTextFile(activityPath)
    println("******************************* after saving as text file")

  }


  /**
    * transfer activity data in hdfs to persona.activity_cisco_response in hive
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
      "source_system STRING COMMENT '客户来源系统'," +
      "regist_flag STRING COMMENT '是否注册,Yes表示注册'," +
      "call_center_status STRING COMMENT '呼叫中心状态'," +
      "comm_method STRING COMMENT '沟通方式'," +
      "mktg_trans_type STRING COMMENT '市场事务'," +
      "response_date STRING COMMENT '反馈时间'," +
      "call_center_vendor STRING COMMENT '呼叫中心供应商'," +
      "country STRING COMMENT '国家代码'," +
      "leads_channel STRING COMMENT '留资渠道'," +
      "campaign_name STRING COMMENT '活动名称'," +
      "dt STRING COMMENT '日期'" +
      ") COMMENT '客户反馈活动临时表' " +
      "ROW FORMAT delimited fields " +
      "terminated by ',' " +
      "lines terminated by '\\n'")

    //load activity data to table tmp.cisco_tmp
    spark.sql("LOAD DATA INPATH '" + activityPath + "' INTO TABLE tmp.cisco_tmp")

    //create partitioned table persona.activity_cisco_response and insert data to it
    spark.sql("drop table if exists persona.activity_cisco_response")
    spark.sql("create table persona.activity_cisco_response(" +
      "lava_id STRING COMMENT 'LavaHEAT 用户或者设备的唯一标识'," +
      "email_enc STRING COMMENT '加密的邮箱地址'," +
      "mobile_enc STRING COMMENT '加密的手机号'," +
      "source_system STRING COMMENT '客户来源系统'," +
      "regist_flag STRING COMMENT '是否注册,Yes表示注册'," +
      "call_center_status STRING COMMENT '呼叫中心状态'," +
      "comm_method STRING COMMENT '沟通方式'," +
      "mktg_trans_type STRING COMMENT '市场事务'," +
      "response_date STRING COMMENT '反馈时间'," +
      "call_center_vendor STRING COMMENT '呼叫中心供应商'," +
      "country STRING COMMENT '国家代码'," +
      "leads_channel STRING COMMENT '留资渠道'," +
      "campaign_name STRING COMMENT '活动名称'" +
      ") COMMENT '客户反馈活动表' " +
      "PARTITIONED BY (dt string COMMENT '以天分区：yyyy-mm-dd') " +
      "ROW FORMAT delimited fields " +
      "terminated by '\\t' " +
      "lines terminated by '\\n'")

    //insert activity data into final table persona.activity_cisco_response
    spark.sql("INSERT INTO TABLE persona.activity_cisco_response PARTITION (dt) SELECT DISTINCT * FROM tmp.cisco_tmp")
  }



  /**
    * transfer persona.activity_cisco_response to persona:user_info in HBase
    * @param spark   spark session
    */
  def transferActivityData2HBase(spark: SparkSession): Unit = {


    try{
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "persona:user_info")

      lazy val job = Job.getInstance(hbaseConf)
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[Result])
      job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

      //create cisco response data frame
      val df = spark.sql("select lava_id,email_enc,mobile_enc,source_system,regist_flag,call_center_status,comm_method," +
        "mktg_trans_type,call_center_vendor,country from persona.activity_cisco_response")

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

          // check the source_system
          if (row.getAs[String]("source_system").trim != "")
            put.addColumn(Bytes.toBytes("tob"), Bytes.toBytes("source_system"), Bytes.toBytes(row.getAs[String]("source_system").trim))

          // check the regist_flag
          if (row.getAs[String]("regist_flag").trim != "")
            put.addColumn(Bytes.toBytes("tob"), Bytes.toBytes("registration"), Bytes.toBytes(row.getAs[String]("regist_flag").trim))

          // check the call_center_status
          if (row.getAs[String]("call_center_status").trim != "")
            put.addColumn(Bytes.toBytes("tob"), Bytes.toBytes("call_center_status"), Bytes.toBytes(row.getAs[String]("call_center_status").trim))

          // check the comm_method
          if (row.getAs[String]("comm_method").trim != "")
            put.addColumn(Bytes.toBytes("tob"), Bytes.toBytes("communication_method"), Bytes.toBytes(row.getAs[String]("comm_method").trim))

          // check the mktg_trans_type
          if (row.getAs[String]("mktg_trans_type").trim != "")
            put.addColumn(Bytes.toBytes("tob"), Bytes.toBytes("mktg_transaction"), Bytes.toBytes(row.getAs[String]("mktg_trans_type").trim))

          // check the call_center_vendor
          if (row.getAs[String]("call_center_vendor").trim != "")
            put.addColumn(Bytes.toBytes("tob"), Bytes.toBytes("call_center_vendor"), Bytes.toBytes(row.getAs[String]("call_center_vendor").trim))

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
