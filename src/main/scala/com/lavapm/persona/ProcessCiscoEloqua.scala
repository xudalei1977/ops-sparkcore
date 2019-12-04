package com.lavapm.persona

import java.text.SimpleDateFormat

import com.lavapm.utils.{Hdfs, UUID}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

/**
  * Process Data from Cisco Eloqua
  * Create by ryan on 03/11/19
  */
object ProcessCiscoEloqua {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Process the cisco eloqua data")
      .enableHiveSupport()
      //set hive table dynamic import
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .getOrCreate()

    //activity path for saving activity data
    val activityPath_eloqua_open = "/user/dmp_app_user/tmp/cisco_eloqua/open"
    val activityPath_eloqua_click = "/user/dmp_app_user/tmp/cisco_eloqua/click"

    val dataType_open = "open"
    val dataType_click = "click"

    println("******************** before transfer cisco eloqua open data to activity data ")
    transferEloquaOpen2ActivityData(spark,activityPath_eloqua_open)

    println("******************** before transfer cisco eloqua open activity data to hive ")
    transferActivityData2Hive(spark,activityPath_eloqua_open,dataType_open)

    println("******************** before transfer cisco eloqua click data to activity data ")
    transferEloquaClick2ActivityData(spark,activityPath_eloqua_click)

    println("******************** before transfer cisco eloqua click activity data to hive ")
    transferActivityData2Hive(spark,activityPath_eloqua_click,dataType_click)

    println("******************** before transfer cisco eloqua activity data to HBase ")
    transferActivityData2HBase(spark)

    spark.stop()
  }


  /**
    * transfer the cisco eloqua open data to activity data and save in hdfs
    * @param spark          spark session
    * @param activityPath   hdfs path for activity path
    */
  def transferEloquaOpen2ActivityData(spark: SparkSession,activityPath:String) = {
    //select necessary fields from sandbox_cisco.eloqua_email_open_data left outer join persona.user_mobile_enc and create data frame
    val df = spark
      .sql("SELECT b.lava_id,a.mailaddress_enc,a.busphone_enc,a.activity_date FROM sandbox_cisco.eloqua_email_open_data a" +
        " left outer join persona.user_mobile_enc b on a.busphone_enc = b.mobile_enc")

    //process df to hiveRDD for saving in hdfs next
    val hiveRDD = df.rdd
      .map(row => {
        var lava_id, email_enc, mobile_enc, open_date, clk_date, is_open, is_click, dt = ""

        if (row.getAs[String]("lava_id") != null) {
          lava_id = row.getAs[String]("lava_id").trim
        } else {
          lava_id = UUID.generateUUID()
        }

        if (row.getAs[String]("mailaddress_enc") != null && row.getAs[String]("mailaddress_enc").trim != "")
          email_enc = row.getAs[String]("mailaddress_enc").trim

        if (row.getAs[String]("busphone_enc") != null && row.getAs[String]("busphone_enc").trim != "")
          mobile_enc = row.getAs[String]("busphone_enc").trim

        if (row.getAs[String]("activity_date") != null && row.getAs[String]("activity_date").trim != ""){
          var time = row.getAs[String]("activity_date").trim
          if (time.indexOf(" ") > 0)
            time = time.split(" ")(0)

          val date = new SimpleDateFormat("yyyy/M/d").parse(time)
          open_date = new SimpleDateFormat("yyyy-MM-dd").format(date)
          dt = open_date
          is_open = "1"

        }

        lava_id + "," + email_enc + "," + mobile_enc + "," + open_date + "," + clk_date + "," + is_open + "," + is_click  + "," + dt
      })

    // delete the target hdfs path if exists
    Hdfs.deleteFile(spark, activityPath)
    hiveRDD.saveAsTextFile(activityPath)
    println("******************************* after saving as text file")

  }


  /**
    * transfer the cisco eloqua click data to activity data and save in hdfs
    * @param spark          spark session
    * @param activityPath   hdfs path for activity path
    */
  def transferEloquaClick2ActivityData(spark: SparkSession,activityPath:String) = {

    //select necessary fields from sandbox_cisco.eloqua_email_click_data left outer join persona.activity_cisco_eloqua and create data frame
    val df = spark
      .sql("SELECT b.lava_id,a.mailaddress_enc,a.busphone_enc,a.activity_date FROM sandbox_cisco.eloqua_email_click_data a" +
        " left outer join persona.activity_cisco_eloqua b on a.busphone_enc = b.mobile_enc")

    //process df to hiveRDD for saving in hdfs next
    val hiveRDD = df.rdd
      .map(row => {
        var lava_id, email_enc, mobile_enc, open_date, clk_date, is_open, is_click, dt = ""

        if (row.getAs[String]("lava_id") != null && row.getAs[String]("lava_id").trim != "")
          lava_id = row.getAs[String]("lava_id").trim
        else
          lava_id = UUID.generateUUID

        if (row.getAs[String]("mailaddress_enc") != null && row.getAs[String]("mailaddress_enc").trim != "")
          email_enc = row.getAs[String]("mailaddress_enc").trim

        if (row.getAs[String]("busphone_enc") != null && row.getAs[String]("busphone_enc").trim != "")
          mobile_enc = row.getAs[String]("busphone_enc").trim

        if (row.getAs[String]("activity_date") != null && row.getAs[String]("activity_date").trim != ""){
          var time = row.getAs[String]("activity_date").trim
          if (time.indexOf(" ") > 0)
            time = time.split(" ")(0)

          val date = new SimpleDateFormat("yyyy/M/d").parse(time)
          clk_date = new SimpleDateFormat("yyyy-MM-dd").format(date)
          dt = clk_date
          is_click = "1"
        }

        lava_id + "," + email_enc + "," + mobile_enc + "," + open_date + "," + clk_date + "," + is_open + "," + is_click  + "," + dt
      })

    // delete the target hdfs path if exists
    Hdfs.deleteFile(spark, activityPath)
    hiveRDD.saveAsTextFile(activityPath)
    println("******************************* after saving as text file")

  }


  /**
    * transfer activity data in hdfs to persona.activity_cisco_eloqua in hive
    * @param spark          spark session
    * @param activityPath   hdfs path for activity data
    */
  def transferActivityData2Hive(spark: SparkSession,activityPath:String,dataType:String) = {

    //create table tmp.cisco_tmp and load activity data
    spark.sql("drop table if exists tmp.cisco_tmp")
    spark.sql("create table tmp.cisco_tmp(" +
      "lava_id STRING COMMENT 'LavaHEAT 用户或者设备的唯一标识'," +
      "email_enc STRING COMMENT '加密的邮箱地址'," +
      "mobile_enc STRING COMMENT '加密的手机号'," +
      "open_date STRING COMMENT '打开邮件的时间'," +
      "clk_date STRING COMMENT '点击邮件中链接的时间'," +
      "is_open STRING COMMENT '是否打开邮件，0表示没有打开，1表示打开'," +
      "is_click STRING COMMENT '是否点击邮件中的链接，0表示没有点击，1表示有点击'," +
      "dt STRING COMMENT '日期'" +
      ") COMMENT 'eloqua邮件展示数据临时表' " +
      "ROW FORMAT delimited fields " +
      "terminated by ',' " +
      "lines terminated by '\\n'")

    //load activity data to table tmp.cisco_tmp
    spark.sql("LOAD DATA INPATH '" + activityPath + "' INTO TABLE tmp.cisco_tmp")

    if (dataType == "open") {

      //distinct lava_id for the same mobile_enc
      spark.sql("drop table if exists tmp.cisco_lavaid_distinct")
      spark.sql("create table tmp.cisco_lavaid_distinct stored AS parquet as select max(lava_id) as lava_id,mobile_enc " +
        "from tmp.cisco_tmp group by mobile_enc")

      //refresh the table tmp.cisco_tmp
      spark.sql("INSERT OVERWRITE TABLE tmp.cisco_tmp SELECT a.lava_id,b.email_enc,b.mobile_enc,b.open_date," +
        "b.clk_date,b.is_open,b.is_click,b.dt FROM tmp.cisco_lavaid_distinct a,tmp.cisco_tmp b WHERE a.mobile_enc = b.mobile_enc")

      //create partitioned table persona.activity_cisco_eloqua and insert tmp.cisco_tmp data to it
      spark.sql("drop table if exists persona.activity_cisco_eloqua")
      spark.sql("create table persona.activity_cisco_eloqua(" +
        "lava_id STRING COMMENT 'LavaHEAT 用户或者设备的唯一标识'," +
        "email_enc STRING COMMENT '加密的邮箱地址'," +
        "mobile_enc STRING COMMENT '加密的手机号'," +
        "open_date STRING COMMENT '打开邮件的时间'," +
        "clk_date STRING COMMENT '点击邮件中链接的时间'," +
        "is_open STRING COMMENT '是否打开邮件，0表示没有打开，1表示打开'," +
        "is_click STRING COMMENT '是否点击邮件中的链接，0表示没有点击，1表示有点击'" +
        ") COMMENT 'eloqua邮件展示数据表' " +
        "PARTITIONED BY (dt string COMMENT '以天分区：yyyy-mm-dd') " +
        "ROW FORMAT delimited fields " +
        "terminated by '\\t' " +
        "lines terminated by '\\n'")
    }

    //insert activity data into final table persona.activity_cisco_eloqua
    spark.sql("INSERT INTO TABLE persona.activity_cisco_eloqua PARTITION (dt) SELECT DISTINCT * FROM tmp.cisco_tmp")
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

      //create cisco eloqua data frame
      val df = spark.sql("select lava_id,email_enc,mobile_enc,is_open,is_click from persona.activity_cisco_eloqua")

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

          // check the is_open
          if (row.getAs[String]("is_open").trim != "")
            put.addColumn(Bytes.toBytes("tob"), Bytes.toBytes("is_open"), Bytes.toBytes(row.getAs[String]("is_open").trim))

          // check the is_click
          if (row.getAs[String]("is_click").trim != "")
            put.addColumn(Bytes.toBytes("tob"), Bytes.toBytes("is_click"), Bytes.toBytes(row.getAs[String]("is_click").trim))

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

